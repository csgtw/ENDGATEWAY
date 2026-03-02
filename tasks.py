import json
import random
import time

from services.redis_store import redis_conn
from celery_worker import celery
from logger import log
from services.gateway import gateway_send_message
from services import state
from services.autoreply import load_autoreply_config


@celery.task(name="send_campaign", bind=True, max_retries=0)
def send_campaign(self, device_ids: list, per_device: int, batch_id: str, template_ids: list = None):
    """Tâche Celery pour l'envoi asynchrone d'une campagne SMS."""
    from services.batches import create_batch
    try:
        return create_batch(device_ids, per_device, batch_id=batch_id, template_ids=template_ids)
    except Exception as e:
        log(f"❌ send_campaign [{batch_id}] erreur: {e}")
        try:
            redis_conn.hset(f"batch:{batch_id}:meta", mapping={"status": "error", "error": str(e)})
        except Exception:
            pass
        return {"batch_id": batch_id, "sent": 0, "failed": 0, "error": str(e)}


def get_conversation_key(number: str) -> str:
    return f"conv:{number}"


def is_archived(number: str) -> bool:
    return redis_conn.sismember("archived_numbers", number)


def archive_number(number: str):
    redis_conn.sadd("archived_numbers", number)


def _processed_key(number: str, msg_id) -> str:
    return f"processed:{number}:{msg_id}"


def mark_processed_once(number: str, msg_id) -> bool:
    """
    Retourne True si le message n'avait pas encore été traité (idempotence).
    TTL 3 jours.
    """
    try:
        k = _processed_key(number, msg_id)
        return bool(redis_conn.set(k, "1", nx=True, ex=3 * 24 * 3600))
    except Exception:
        return True


def _check_cycle_auto_restart(device_id: str):
    """
    Vérifie si le cycle est terminé et le redémarre automatiquement si
    max_cycles n'est pas atteint. Lock Redis anti-doublon (5s).
    """
    try:
        cycle_recv = state.device_cycle_received_get(device_id)
        cycle_lim  = state.cycle_limit_get()
        if cycle_lim <= 0 or cycle_recv < cycle_lim:
            return  # cycle pas encore terminé

        max_cycles   = state.device_max_cycles_get(device_id)
        current_idx  = state.device_cycle_index_get(device_id)

        # condition restart : illimité (max=0) OU cycle en cours < dernier autorisé
        if max_cycles != 0 and (current_idx + 1) >= max_cycles:
            return  # max atteint, on ne relance plus

        # Lock pour éviter que plusieurs workers relancent en même temps
        lock_key = f"cycle:restart_lock:{device_id}"
        if state.set_lock(lock_key, ttl_sec=5):
            state.device_cycle_relancer(device_id)
            log(f"🔄 Auto-restart cycle device={device_id} idx={current_idx+1}")
    except Exception:
        pass


def _pick_reply_text(template_ids: list, fallback_text: str) -> str:
    """
    Choisit aléatoirement un texte parmi les templates fournis.
    Retombe sur fallback_text si aucun template valide.
    """
    if template_ids:
        from services.templates import get_templates_texts
        texts = get_templates_texts(template_ids)
        if texts:
            return random.choice(texts)
    return fallback_text


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

    # Idempotence AVANT les stats — évite le double-comptage sur retry
    if not mark_processed_once(number, msg_id):
        return

    # Stats réception (seulement si le message est nouveau)
    try:
        state.device_mark_seen(device_id)
        state.device_incr_received(device_id, 1)
        # Auto-restart cycle si la limite est atteinte
        _check_cycle_auto_restart(device_id)
    except Exception:
        pass

    try:
        if is_archived(number):
            return

        conv_key  = get_conversation_key(number)
        step      = int(redis_conn.hget(conv_key, "step") or 0)
        redis_conn.hset(conv_key, "device", device_id)

        reply_mode = int(cfg.get("reply_mode", 2))

        step0_text         = (cfg.get("step0_text") or "").strip()
        step1_text         = (cfg.get("step1_text") or "").strip()
        step0_type         = cfg.get("step0_type", "sms")
        step1_type         = cfg.get("step1_type", "sms")
        step0_delay        = float(cfg.get("step0_delay") or 0)
        step1_delay        = float(cfg.get("step1_delay") or 0)
        step0_template_ids = cfg.get("step0_template_ids") or []
        step1_template_ids = cfg.get("step1_template_ids") or []

        if step == 0:
            reply0 = _pick_reply_text(step0_template_ids, step0_text)
            if reply0:
                if step0_delay > 0:
                    time.sleep(step0_delay)
                ok, _ = gateway_send_message(number, reply0, device_id, step0_type)
                if ok:
                    try:
                        state.device_incr_sent(device_id, 1)
                    except Exception:
                        pass

            if reply_mode == 1:
                archive_number(number)
                redis_conn.delete(conv_key)
                return

            redis_conn.hset(conv_key, "step", 1)
            return

        if step == 1:
            if reply_mode == 2:
                reply1 = _pick_reply_text(step1_template_ids, step1_text)
                if reply1:
                    if step1_delay > 0:
                        time.sleep(step1_delay)
                    ok, _ = gateway_send_message(number, reply1, device_id, step1_type)
                    if ok:
                        try:
                            state.device_incr_sent(device_id, 1)
                        except Exception:
                            pass

            archive_number(number)
            redis_conn.delete(conv_key)
            return

        # Step inattendu → archiver proprement
        archive_number(number)
        redis_conn.delete(conv_key)

    except Exception as e:
        log(f"💥 process_message error: {e}")
        try:
            state.device_incr_errors(device_id, 1)
        except Exception:
            pass
