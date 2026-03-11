import json
import random
import re
import time
import uuid as _uuid

from services.redis_store import redis_conn
from celery_worker import celery
from logger import log
from services.gateway import gateway_send_message
from services import state
from services.autoreply import load_autoreply_config
from services.numlist import nl_remaining_count
from services.batches import render_message as _render_msg
from services import msgtpl


@celery.task(name="send_campaign", bind=True, max_retries=0)
def send_campaign(self, device_ids: list, per_device: int, batch_id: str, template_ids: list = None):
    """Tâche Celery pour l'envoi asynchrone d'une campagne SMS."""
    from services.batches import create_batch
    device_ids = [str(x) for x in (device_ids or []) if str(x).strip()]

    # Vérification annulation avant tout (batch planifié annulé entre temps)
    if redis_conn.exists(f"batch:{batch_id}:cancelled"):
        log(f"🚫 Batch {batch_id} annulé avant démarrage")
        return {"batch_id": batch_id, "sent": 0, "failed": 0, "status": "cancelled"}

    # Envoi parallèle : un sous-batch par device
    if len(device_ids) > 1:
        sub_ids = []
        for i, did in enumerate(device_ids):
            sub_id = f"{batch_id}d{i}"
            sub_ids.append(sub_id)
            redis_conn.hset(f"batch:{sub_id}:meta", mapping={
                "batch_id":     sub_id,
                "parent_batch": batch_id,
                "created_ts":   str(int(time.time())),
                "planned":      str(per_device),
                "sent":         "0",
                "failed":       "0",
                "status":       "queued",
                "device_ids":   json.dumps([did]),
                "per_device":   str(per_device),
                "device_count": "1",
            })
            redis_conn.expire(f"batch:{sub_id}:meta", 24 * 3600)
            send_campaign.delay([did], per_device, sub_id, None)
        redis_conn.hset(f"batch:{batch_id}:meta", mapping={
            "status": "dispatched",
            "sub_batches": json.dumps(sub_ids),
        })
        log(f"🚀 Batch {batch_id} → {len(device_ids)} sub-tasks parallèles")
        return {"batch_id": batch_id, "dispatched": len(device_ids)}

    try:
        return create_batch(device_ids, per_device, batch_id=batch_id)
    except Exception as e:
        log(f"❌ send_campaign [{batch_id}] erreur: {e}")
        try:
            redis_conn.hset(f"batch:{batch_id}:meta", mapping={"status": "error", "error": str(e)})
        except Exception:
            pass
        return {"batch_id": batch_id, "sent": 0, "failed": 0, "error": str(e)}


def get_conversation_key(number: str) -> str:
    return f"conv:{number}"


_ARCHIVE_TTL = 30 * 24 * 3600  # 30 jours

def is_archived(number: str) -> bool:
    """Vérifie si un numéro est archivé (sorted set avec TTL 30j)."""
    try:
        score = redis_conn.zscore("archived_numbers", number)
        if score is None:
            return False
        return score > (time.time() - _ARCHIVE_TTL)
    except Exception:
        return False


def archive_number(number: str):
    """Archive un numéro avec timestamp. Purge automatique des entrées > 30j."""
    try:
        ts = time.time()
        redis_conn.zadd("archived_numbers", {number: ts})
        redis_conn.zremrangebyscore("archived_numbers", 0, ts - _ARCHIVE_TTL)
    except Exception:
        pass


def _processed_key(number: str, msg_id) -> str:
    return f"processed:{number}:{msg_id}"


def mark_processed_once(number: str, msg_id) -> bool:
    """
    Retourne True si le message n'avait pas encore été traité (idempotence).
    TTL 30 jours. Fail-safe : retourne False en cas d'erreur Redis (présume déjà traité).
    """
    try:
        k = _processed_key(number, msg_id)
        return bool(redis_conn.set(k, "1", nx=True, ex=30 * 24 * 3600))
    except Exception:
        return False  # Fail-safe : erreur Redis → on ne traite pas (évite les doublons)


def _load_contact_vars(number: str) -> dict:
    """Charge les variables du contact depuis Redis (stockées lors de l'envoi campagne)."""
    try:
        raw_vars = redis_conn.hgetall(f"conv:{number}:vars")
        if not raw_vars:
            return {}
        return {
            (k.decode("utf-8") if isinstance(k, bytes) else k):
            (v.decode("utf-8") if isinstance(v, bytes) else v)
            for k, v in raw_vars.items()
        }
    except Exception:
        return {}


def _apply_vars(text: str, vars_dict: dict) -> str:
    """Remplace %clé% par les valeurs du dict (numéro inclus).
    %rand8% → nombre aléatoire à 8 chiffres (différent à chaque occurrence)."""
    out = text or ""
    if vars_dict:
        for k, v in vars_dict.items():
            out = out.replace("%" + str(k) + "%", str(v))
    out = re.sub(r"%rand8%", lambda _: str(random.randint(10000000, 99999999)), out)
    return out


def _check_cycle_auto_restart(device_id: str):
    """
    Vérifie si le cycle est terminé et le redémarre automatiquement si
    max_cycles n'est pas atteint. Lock Redis anti-doublon (5s).
    Si les derniers paramètres de campagne existent, dispatch un nouveau batch.
    """
    try:
        # Arrêt global demandé par l'utilisateur → ne rien lancer
        if state.cycle_stop_get():
            return

        cycle_recv = state.device_cycle_received_get(device_id)
        cycle_lim  = state.cycle_limit_get()
        if cycle_lim <= 0 or cycle_recv < cycle_lim:
            return  # cycle pas encore terminé

        max_cycles   = state.device_max_cycles_get(device_id)
        current_idx  = state.device_cycle_index_get(device_id)

        # condition restart : illimité (max=0) OU cycle en cours < dernier autorisé
        if max_cycles != 0 and (current_idx + 1) >= max_cycles:
            return  # max atteint, on ne relance plus

        # Lock pour éviter que plusieurs workers relancent en même temps (TTL 30s)
        lock_key = f"cycle:restart_lock:{device_id}"
        if state.set_lock(lock_key, ttl_sec=30):
            state.device_cycle_relancer(device_id)
            log(f"🔄 Auto-restart cycle device={device_id} idx={current_idx+1}")

            # Dispatch nouvelle campagne si paramètres disponibles et numlist non vide
            try:
                params  = state.device_last_campaign_get(device_id)
                per_dev = params.get("per_device", 0)
                tmpl_ids = params.get("template_ids") or None
                if per_dev > 0 and nl_remaining_count() > 0:
                    new_batch_id = str(_uuid.uuid4())[:8]
                    # Initialiser le meta du nouveau batch
                    redis_conn.hset(f"batch:{new_batch_id}:meta", mapping={
                        "batch_id":    new_batch_id,
                        "created_ts":  str(int(time.time())),
                        "planned":     str(per_dev),
                        "sent":        "0",
                        "failed":      "0",
                        "status":      "queued",
                        "device_ids":  json.dumps([device_id]),
                        "template_ids": json.dumps(tmpl_ids or []),
                        "per_device":  str(per_dev),
                        "device_count": "1",
                    })
                    redis_conn.expire(f"batch:{new_batch_id}:meta", 24 * 3600)
                    send_campaign.delay([device_id], per_dev, new_batch_id, tmpl_ids)
                    log(f"🚀 Auto-dispatch campagne device={device_id} batch={new_batch_id}")
            except Exception as exc:
                log(f"⚠️ Auto-dispatch erreur device={device_id}: {exc}")
    except Exception:
        pass



@celery.task(name="process_message")
def process_message(msg_json: str):
    # Heartbeat — l'UI sait que le worker tourne
    try:
        state.worker_heartbeat()
    except Exception:
        pass

    cfg = load_autoreply_config()
    if not cfg.get("enabled"):
        return

    try:
        msg = json.loads(msg_json)
    except Exception:
        return

    number    = (msg.get("number") or "").strip()
    msg_id    = msg.get("ID")
    device_id = str(msg.get("deviceID") or "").strip()

    if not number or not msg_id or not device_id:
        return

    recv_ts = int(msg.get("recv_ts") or 0)

    # Idempotence — évite le double-traitement sur retry Celery
    if not mark_processed_once(number, msg_id):
        return

    # ── Lock par numéro : évite les doublons concurrents (2 SMS simultanés) ──
    # Sans ce lock, 2 SMS reçus en même temps enverraient 2× la réponse 1.
    conv_lock = f"conv_lock:{number}"
    acquired  = False
    for _ in range(10):
        if redis_conn.set(conv_lock, "1", nx=True, ex=10):
            acquired = True
            break
        time.sleep(0.1)

    if not acquired:
        log(f"[proc] conv_lock timeout {number}")
        return

    step_to_process = -1
    text_to_send    = None
    msg_type_to_use = "sms"
    delay_to_apply  = 0
    conv_key        = get_conversation_key(number)

    try:
        if is_archived(number):
            return

        step       = int(redis_conn.hget(conv_key, "step") or 0)
        redis_conn.hset(conv_key, "device", device_id)

        reply_mode  = int(cfg.get("reply_mode", 2))
        from services import arcamps as _arcamps
        _active_arc0 = _arcamps.get_active_step(0)
        _active_arc1 = _arcamps.get_active_step(1)
        step0_text = _arcamps.pick_random(_active_arc0, 0) if _active_arc0 else (msgtpl.pick_random("ar:step0") or "")
        step1_text = _arcamps.pick_random(_active_arc1, 1) if _active_arc1 else (msgtpl.pick_random("ar:step1") or "")
        step0_type  = cfg.get("step0_type", "sms")
        step1_type  = cfg.get("step1_type", "sms")
        _MAX_DELAY  = 300
        step0_delay = min(float(cfg.get("step0_delay") or 0), _MAX_DELAY)
        step1_delay = min(float(cfg.get("step1_delay") or 0), _MAX_DELAY)

        contact_vars = _load_contact_vars(number)
        contact_vars["number"] = number
        try:
            link = state.global_link_get()
            if link:
                contact_vars["link"] = link
        except Exception:
            pass

        if step == 0:
            step_to_process = 0
            msg_type_to_use = step0_type
            delay_to_apply  = step0_delay
            text_to_send    = _apply_vars(step0_text, contact_vars) if step0_text else ""
            if reply_mode == 1:
                archive_number(number)
                redis_conn.delete(conv_key)
            else:
                # Avancer à step 1.
                # step0_exec_ts = moment où step0 est traité.
                # Si le prochain message a recv_ts < step0_exec_ts, c'est un double-message
                # envoyé avant qu'on ait traité step0 → on l'ignorera pour step1.
                redis_conn.hset(conv_key, mapping={
                    "step": 1,
                    "step0_exec_ts": str(int(time.time())),
                })

        elif step == 1 and reply_mode == 2:
            # Anti double-message rapide : si ce message a été reçu AVANT que step0
            # soit traité (recv_ts < step0_exec_ts), c'est un envoi multiple du contact
            # avant même de recevoir notre réponse → on ignore.
            step0_exec_ts = int(redis_conn.hget(conv_key, "step0_exec_ts") or 0)
            if recv_ts > 0 and step0_exec_ts > 0 and recv_ts < step0_exec_ts:
                pass  # double-message rapide — ignorer
            else:
                step_to_process = 1
                msg_type_to_use = step1_type
                delay_to_apply  = step1_delay
                text_to_send    = _apply_vars(step1_text, contact_vars) if step1_text else ""
                archive_number(number)
                redis_conn.delete(conv_key)

        else:
            archive_number(number)
            redis_conn.delete(conv_key)

    except Exception as e:
        log(f"💥 process_message conv error {number}: {e}")
        try:
            state.device_incr_errors(device_id, 1)
        except Exception:
            pass
        return
    finally:
        redis_conn.delete(conv_lock)

    # ── Envoi HORS du verrou ─────────────────────────────────────────────────
    if step_to_process >= 0 and text_to_send:
        try:
            if delay_to_apply > 0:
                time.sleep(delay_to_apply)
            ok, _ = gateway_send_message(number, text_to_send, device_id, msg_type_to_use)
            if ok:
                try:
                    state.device_incr_sent(device_id, 1)
                except Exception:
                    pass
        except Exception as e:
            log(f"💥 process_message send error {number}: {e}")
            try:
                state.device_incr_errors(device_id, 1)
            except Exception:
                pass
