import json
import time
import uuid

from services.redis_store import redis_conn
from services.numlist import NL_QUEUE_KEY, load_message_draft, nl_remaining_count
from services.gateway import gateway_send_message
from services import state
from logger import log

BATCH_KEY_TTL = 24 * 3600  # 24h — les clés batch expirent automatiquement


def _now() -> int:
    return int(time.time())


def render_message(template: str, contact: dict) -> str:
    """Remplace les variables {{clé}} par les valeurs du contact."""
    out = template or ""
    for k, v in (contact or {}).items():
        if k == "number":
            continue
        out = out.replace("{{" + str(k) + "}}", str(v))
    return out


def create_batch(device_ids, per_device: int):
    """
    Prend X numéros par appareil, envoie via l'API gateway.
    Les numéros ne sont retirés définitivement que si l'envoi réussit.
    En cas d'échec, le numéro est remis dans la queue.
    """
    device_ids = [str(x) for x in (device_ids or []) if str(x).strip()]
    if not device_ids:
        raise ValueError("Aucun appareil")

    if per_device <= 0:
        raise ValueError("per_device invalide")

    remaining = nl_remaining_count()
    if remaining <= 0:
        raise ValueError("Numlist vide")

    msg_template, msg_type = load_message_draft()
    msg_template = (msg_template or "").strip()
    if not msg_template:
        raise ValueError("Message campagne manquant")

    batch_id   = str(uuid.uuid4())[:8]
    meta_key   = f"batch:{batch_id}:meta"
    sent_key   = f"batch:{batch_id}:sent"
    failed_key = f"batch:{batch_id}:failed"

    total_planned = per_device * len(device_ids)

    p = redis_conn.pipeline()
    p.hset(meta_key, mapping={
        "batch_id":     batch_id,
        "created_ts":   str(_now()),
        "per_device":   str(per_device),
        "device_count": str(len(device_ids)),
        "type":         msg_type,
        "planned":      str(total_planned),
        "sent":         "0",
        "failed":       "0",
    })
    p.expire(meta_key, BATCH_KEY_TTL)
    p.execute()

    sent = 0
    failed = 0

    for did in device_ids:
        for _ in range(per_device):
            raw = redis_conn.rpop(NL_QUEUE_KEY)
            if not raw:
                break

            try:
                contact = json.loads(raw.decode("utf-8"))
            except Exception:
                failed += 1
                redis_conn.lpush(failed_key, json.dumps({"device": did, "error": "bad_json"}))
                continue

            number = (contact.get("number") or "").strip()
            if not number:
                failed += 1
                redis_conn.lpush(failed_key, json.dumps({"device": did, "error": "no_number"}))
                continue

            msg = render_message(msg_template, contact).strip()
            if not msg:
                redis_conn.rpush(NL_QUEUE_KEY, json.dumps(contact, ensure_ascii=False))
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": "empty_message"}, ensure_ascii=False)
                )
                continue

            ok, detail = gateway_send_message(number=number, message=msg, device_id=did, msg_type=msg_type)

            if ok:
                sent += 1
                state.device_incr_sent(did, 1)
                redis_conn.lpush(sent_key, json.dumps({"device": did, "number": number}, ensure_ascii=False))
            else:
                redis_conn.rpush(NL_QUEUE_KEY, json.dumps(contact, ensure_ascii=False))
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": detail or "send_failed"}, ensure_ascii=False)
                )

            redis_conn.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed)})

    # TTL sur les clés sent/failed (créées dynamiquement)
    redis_conn.expire(sent_key, BATCH_KEY_TTL)
    redis_conn.expire(failed_key, BATCH_KEY_TTL)

    log(f"📦 Batch {batch_id} terminé | sent={sent} failed={failed} remaining={nl_remaining_count()}")

    return {"batch_id": batch_id, "sent": sent, "failed": failed}
