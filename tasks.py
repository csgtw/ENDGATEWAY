import hashlib
import json
import os
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
from services import camps as _camps_svc


@celery.task(name="send_campaign", bind=True, max_retries=0)
def send_campaign(self, device_ids: list, per_device: int, batch_id: str, template_ids: list = None, sequential: bool = False):
    """Tâche Celery pour l'envoi asynchrone d'une campagne SMS."""
    from services.batches import create_batch
    device_ids = [str(x) for x in (device_ids or []) if str(x).strip()]

    # Vérification annulation avant tout (batch planifié annulé entre temps)
    if redis_conn.exists(f"batch:{batch_id}:cancelled"):
        log(f"🚫 Batch {batch_id} annulé avant démarrage")
        return {"batch_id": batch_id, "sent": 0, "failed": 0, "status": "cancelled"}

    # Armer l'auto-restart de cycle AU MOMENT DE L'EXÉCUTION (pas à la programmation).
    # Sinon un envoi programmé arme l'auto-restart tout de suite et peut déclencher
    # des campagnes immédiates/répétées avant l'heure prévue → "envoie beaucoup trop".
    try:
        for did in device_ids:
            state.device_last_campaign_set(did, per_device, template_ids)
    except Exception:
        pass

    if len(device_ids) > 1:
        if sequential:
            # ── Mode séquentiel : un device à la fois ────────────────────
            sub_ids = []
            total_sent = 0
            total_failed = 0
            for i, did in enumerate(device_ids):
                # Vérification annulation parent avant chaque device
                if redis_conn.exists(f"batch:{batch_id}:cancelled"):
                    redis_conn.hset(f"batch:{batch_id}:meta", mapping={
                        "status": "cancelled", "sent": str(total_sent),
                        "failed": str(total_failed), "sub_batches": json.dumps(sub_ids),
                    })
                    log(f"🚫 Batch séquentiel {batch_id} annulé avant device {i}")
                    return {"batch_id": batch_id, "sent": total_sent, "failed": total_failed, "status": "cancelled"}

                sub_id = f"{batch_id}s{i}"
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

                result = create_batch([did], per_device, batch_id=sub_id)
                total_sent   += result.get("sent", 0)
                total_failed += result.get("failed", 0)

                # Stopper si le sub-batch a été mis en pause ou annulé
                if result.get("status") in ("cancelled", "paused"):
                    redis_conn.hset(f"batch:{batch_id}:meta", mapping={
                        "status": result["status"], "sent": str(total_sent),
                        "failed": str(total_failed), "sub_batches": json.dumps(sub_ids),
                    })
                    return {"batch_id": batch_id, "sent": total_sent, "failed": total_failed, "status": result["status"]}

            redis_conn.hset(f"batch:{batch_id}:meta", mapping={
                "status": "done", "sent": str(total_sent), "failed": str(total_failed),
                "sub_batches": json.dumps(sub_ids),
            })
            log(f"🔀 Batch séquentiel {batch_id} terminé | devices={len(device_ids)} sent={total_sent} failed={total_failed}")
            return {"batch_id": batch_id, "sent": total_sent, "failed": total_failed}

        else:
            # ── Mode parallèle : un sous-batch par device ─────────────────
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
    _ar_img_url     = ""
    _ar_audio_url   = ""
    conv_key        = get_conversation_key(number)

    try:
        if is_archived(number):
            return

        step       = int(redis_conn.hget(conv_key, "step") or 0)
        redis_conn.hset(conv_key, "device", device_id)

        reply_mode  = int(cfg.get("reply_mode", 2))
        _active_arc0 = _camps_svc.get_active_ar(0)
        _active_arc1 = _camps_svc.get_active_ar(1)
        step0_text = (_camps_svc.pick_random_msg(_active_arc0) if _active_arc0 else None) or (msgtpl.pick_random("ar:step0") or "")
        step1_text = (_camps_svc.pick_random_msg(_active_arc1) if _active_arc1 else None) or (msgtpl.pick_random("ar:step1") or "")
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
            text_to_send    = _render_msg(step0_text, contact_vars) if step0_text else ""
            _tog0 = redis_conn.get("config:img_toggle:ar:step0")
            _ar_img_url = ""
            if _tog0 and _tog0.decode() == "1":
                _ar_img_url = contact_vars.get("image", "")
            _atog0 = redis_conn.get("config:audio_toggle:ar:step0")
            _ar_audio_url = ""
            if _atog0 and _atog0.decode() == "1":
                _aud0 = redis_conn.get("config:audio:url")
                if _aud0:
                    _ar_audio_url = _aud0.decode()
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
                text_to_send    = _render_msg(step1_text, contact_vars) if step1_text else ""
                _tog1 = redis_conn.get("config:img_toggle:ar:step1")
                _ar_img_url = ""
                if _tog1 and _tog1.decode() == "1":
                    _ar_img_url = contact_vars.get("image", "")
                _atog1 = redis_conn.get("config:audio_toggle:ar:step1")
                _ar_audio_url = ""
                if _atog1 and _atog1.decode() == "1":
                    _aud1 = redis_conn.get("config:audio:url")
                    if _aud1:
                        _ar_audio_url = _aud1.decode()
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
            ok, err_detail = gateway_send_message(number, text_to_send, device_id, msg_type_to_use,
                                                   media_url=_ar_img_url, audio_url=_ar_audio_url)
            if ok:
                try:
                    state.device_incr_sent(device_id, 1)
                except Exception:
                    pass
            else:
                log(f"❌ auto-reply send failed device={device_id} number={number} err={err_detail}")
                try:
                    state.device_incr_errors(device_id, 1)
                except Exception:
                    pass
        except Exception as e:
            log(f"💥 process_message send error {number}: {e}")
            try:
                state.device_incr_errors(device_id, 1)
            except Exception:
                pass


@celery.task(name="check_gateway_delivery", bind=True, max_retries=0)
def check_gateway_delivery(self, batch_id: str):
    """
    Vérifie le statut réel des messages envoyés (10 min après le batch).
    Corrige les compteurs sent/failed si des messages ont échoué côté gateway.
    """
    from services.gateway import gateway_fetch_message_status

    check_key  = f"batch:{batch_id}:gw_check"
    meta_key   = f"batch:{batch_id}:meta"
    failed_key = f"batch:{batch_id}:failed"

    try:
        raw = redis_conn.get(check_key)
        if not raw:
            return
        check_data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except Exception as e:
        log(f"⚠️ check_gateway_delivery [{batch_id}]: lecture gw_check impossible: {e}")
        return
    finally:
        redis_conn.delete(check_key)

    to_check = [x for x in (check_data or []) if x.get("gw_id")]
    if not to_check:
        return

    failed_now      = 0
    errors_by_device = {}

    for entry in to_check:
        gw_id  = entry["gw_id"]
        did    = str(entry.get("device") or "")
        number = str(entry.get("number") or "")

        status = gateway_fetch_message_status(gw_id)
        if status.lower() == "failed":
            failed_now += 1
            base_did = did.split("|")[0]
            errors_by_device[base_did] = errors_by_device.get(base_did, 0) + 1
            redis_conn.lpush(
                failed_key,
                json.dumps({
                    "device": did, "number": number,
                    "error": "gateway_failed", "gw_id": gw_id,
                    "source": "delivery_check",
                }, ensure_ascii=False)
            )

    if failed_now > 0:
        meta = {
            (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in (redis_conn.hgetall(meta_key) or {}).items()
        }
        cur_sent   = int(meta.get("sent",   0) or 0)
        cur_failed = int(meta.get("failed", 0) or 0)
        new_sent   = max(0, cur_sent - failed_now)
        new_failed = cur_failed + failed_now

        redis_conn.hset(meta_key, mapping={
            "sent":             str(new_sent),
            "failed":           str(new_failed),
            "delivery_checked": "1",
        })

        for base_did, n in errors_by_device.items():
            state.device_incr_errors(base_did, n)
            state.device_incr_sent(base_did, -n)  # correction

        log(f"📬 Delivery check batch={batch_id}: {failed_now} échecs détectés, sent {cur_sent}→{new_sent}")
    else:
        redis_conn.hset(meta_key, "delivery_checked", "1")
        log(f"✅ Delivery check batch={batch_id}: tous les {len(to_check)} msgs confirmés envoyés")


def _imgdata_ttl(date_text: str) -> int:
    """Secondes jusqu'à la fin du jour affiché sur l'image (auto-suppression une
    fois le jour passé). Défaut : fin du jour courant. Minimum 1h."""
    import datetime as _dt
    try:
        d = _dt.datetime.strptime((date_text or "").strip(), "%d/%m/%Y").date()
    except Exception:
        d = _dt.date.today()
    # minuit suivant + 3h de marge (décalage fuseau serveur UTC / Paris)
    end = _dt.datetime.combine(d, _dt.time.min) + _dt.timedelta(days=1, hours=3)
    ttl = int((end - _dt.datetime.now()).total_seconds())
    return max(3600, ttl)


@celery.task(name="prepare_nl_images", bind=True, max_retries=0)
def prepare_nl_images(self, list_id: str, base_url: str, img_col: str = "names", date_text: str = ""):
    """Génère les images personnalisées pour tous les contacts d'une liste NL.
    Template lu depuis Redis (nl:template). Images stockées dans Redis (nl:imgdata:...).
    date_text : date affichée sur l'image (défaut = jour courant).
    Annulable via la clé Redis nl:imgcancel:{list_id}."""
    import tempfile
    from services import imggen as _imggen

    list_key   = f"nl:list:{list_id}"
    status_key = f"nl:imgstatus:{list_id}"
    cancel_key = f"nl:imgcancel:{list_id}"
    tpl_path   = None
    _ttl = _imgdata_ttl(date_text)
    # Nettoie un éventuel drapeau d'annulation résiduel avant de démarrer
    try:
        redis_conn.delete(cancel_key)
    except Exception:
        pass

    try:
        # ── Charger le template depuis Redis ──────────────────────────────────
        raw_tpl = redis_conn.get("nl:template")
        if not raw_tpl:
            redis_conn.hset(status_key, mapping={
                "status": "error", "total": "0", "done": "0", "failed": "0",
                "last_error": "Template non trouvé — uploader un template d'abord",
            })
            return

        tmp_tpl = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        tmp_tpl.write(raw_tpl)
        tmp_tpl.close()
        tpl_path = tmp_tpl.name

        # ── Compter les contacts ───────────────────────────────────────────────
        total = int(redis_conn.llen(list_key) or 0)
        if total == 0:
            redis_conn.hset(status_key, mapping={
                "status": "done", "total": "0", "done": "0", "failed": "0"
            })
            return

        redis_conn.hset(status_key, mapping={
            "status": "running", "total": str(total), "done": "0", "failed": "0"
        })

        done = 0
        failed = 0
        last_err = ""
        BATCH = 100

        for offset in range(0, total, BATCH):
            # ── Annulation demandée ? (vérifiée à chaque lot de 100) ──────────
            if redis_conn.exists(cancel_key):
                redis_conn.delete(cancel_key)
                redis_conn.hset(status_key, mapping={
                    "status": "cancelled", "total": str(total),
                    "done": str(done), "failed": str(failed),
                })
                log(f"🚫 prepare_nl_images list={list_id} annulé | done={done}")
                return
            end = min(offset + BATCH - 1, total - 1)
            batch_raw = redis_conn.lrange(list_key, offset, end) or []

            for i, raw in enumerate(batch_raw):
                tmp_out = None
                try:
                    contact = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                    number  = (contact.get("number") or "").strip()
                    names   = (contact.get(img_col) or "").strip()

                    if not number or not names:
                        failed += 1
                        if not last_err:
                            cols = list(contact.keys())
                            last_err = f"Colonne '{img_col}' vide ou absente. Colonnes dispo: {cols}"
                        continue

                    num_hash = hashlib.md5(number.encode()).hexdigest()[:16]

                    # Générer vers /tmp/
                    tmp_out_f = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
                    tmp_out_f.close()
                    tmp_out = tmp_out_f.name

                    _imggen.generate_image(
                        names_text=names,
                        template_path=tpl_path,
                        output_path=tmp_out,
                        seed=offset + i + 1,
                        date_text=date_text,
                    )

                    # Lire l'image générée et stocker dans Redis (TTL = fin du jour affiché)
                    with open(tmp_out, "rb") as fh:
                        img_bytes = fh.read()
                    redis_conn.set(f"nl:imgdata:{list_id}:{num_hash}", img_bytes, ex=_ttl)

                    img_url = f"{base_url}/uploads/{list_id}/{num_hash}.jpg"
                    redis_conn.set(f"nl:img:{list_id}:{number}", img_url, ex=_ttl)
                    done += 1

                except Exception as exc:
                    log(f"⚠️ imggen contact={offset + i} list={list_id}: {exc}")
                    if not last_err:
                        last_err = str(exc)[:300]
                    failed += 1
                finally:
                    if tmp_out and os.path.exists(tmp_out):
                        try:
                            os.unlink(tmp_out)
                        except Exception:
                            pass

            redis_conn.hset(status_key, mapping={"done": str(done), "failed": str(failed)})

        redis_conn.hset(status_key, mapping={
            "status": "done", "total": str(total), "done": str(done),
            "failed": str(failed), "last_error": last_err,
        })
        log(f"✅ prepare_nl_images list={list_id} done={done} failed={failed}")

    except Exception as e:
        log(f"❌ prepare_nl_images list={list_id}: {e}")
        try:
            redis_conn.hset(status_key, mapping={"status": "error", "error": str(e)[:200]})
        except Exception:
            pass
    finally:
        if tpl_path and os.path.exists(tpl_path):
            try:
                os.unlink(tpl_path)
            except Exception:
                pass
