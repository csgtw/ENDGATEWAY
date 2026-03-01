import json
from services.redis_store import redis_conn
from celery_worker import celery
from logger import log
from services.gateway import gateway_send_message
from services import state
from services.autoreply import load_autoreply_config  # Source unique de vérité


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
        # En cas de souci Redis, on préfère traiter plutôt que dropper
        return True


@celery.task(name="process_message")
def process_message(msg_json: str):
    # Heartbeat — l'UI sait que le worker tourne
    try:
        state.worker_heartbeat()
    except Exception:
        pass

    cfg = load_autoreply_config()
    if not cfg.get("enabled", True):
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

    # Stats réception
    try:
        state.device_mark_seen(device_id)
        state.device_incr_received(device_id, 1)
    except Exception:
        pass

    # Idempotence — évite de traiter deux fois le même message
    if not mark_processed_once(number, msg_id):
        return

    try:
        if is_archived(number):
            return

        conv_key   = get_conversation_key(number)
        step       = int(redis_conn.hget(conv_key, "step") or 0)
        redis_conn.hset(conv_key, "device", device_id)

        reply_mode  = int(cfg.get("reply_mode", 2))
        step0_text  = (cfg.get("step0_text") or "").strip()
        step1_text  = (cfg.get("step1_text") or "").strip()
        step0_type  = cfg.get("step0_type", "sms")
        step1_type  = cfg.get("step1_type", "sms")

        if step == 0:
            if step0_text:
                ok, _ = gateway_send_message(number, step0_text, device_id, step0_type)
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
            if reply_mode == 2 and step1_text:
                ok, _ = gateway_send_message(number, step1_text, device_id, step1_type)
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
