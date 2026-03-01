import json
import time
import uuid

from services.redis_store import redis_conn
from services.numlist import NL_QUEUE_KEY, load_message_draft, nl_remaining_count
from services.gateway import gateway_send_message
from services import state
from services.blacklist import is_blacklisted, record_failure
from logger import log

BATCH_KEY_TTL = 24 * 3600  # 24h — clés batch expirent automatiquement


def _now() -> int:
    return int(time.time())


def _base_device_id(device_id: str) -> str:
    """Extrait l'ID de base sans slot SIM (ex: '1|0' → '1', '2' → '2')."""
    return str(device_id).split("|")[0]


def render_message(template: str, contact: dict) -> str:
    """Remplace les variables {{clé}} par les valeurs du contact."""
    out = template or ""
    for k, v in (contact or {}).items():
        if k == "number":
            continue
        out = out.replace("{{" + str(k) + "}}", str(v))
    return out


# ─── Lecture des batches ──────────────────────────────────────────────────────

def get_batch_status(batch_id: str) -> dict | None:
    """Lit le statut d'un batch depuis Redis."""
    key = f"batch:{batch_id}:meta"
    meta = redis_conn.hgetall(key)
    if not meta:
        return None
    return {
        (k.decode("utf-8") if isinstance(k, bytes) else k):
        (v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in meta.items()
    }


def get_recent_batches(limit: int = 15) -> list:
    """Retourne les derniers batches triés par date décroissante."""
    try:
        batches = []
        for key in redis_conn.scan_iter(match="batch:*:meta", count=200):
            meta = redis_conn.hgetall(key)
            if not meta:
                continue
            d = {
                (k.decode("utf-8") if isinstance(k, bytes) else k):
                (v.decode("utf-8") if isinstance(v, bytes) else v)
                for k, v in meta.items()
            }
            if "batch_id" in d:
                batches.append(d)
        batches.sort(key=lambda x: int(x.get("created_ts") or 0), reverse=True)
        return batches[:limit]
    except Exception:
        return []


# ─── Envoi ───────────────────────────────────────────────────────────────────

def create_batch(device_ids, per_device: int, batch_id: str = None):
    """
    Dépile les contacts, applique le template, envoie via le gateway.

    Multi-SIM : device_id peut être '1|0' (device 1, SIM slot 0).
    Stats Redis trackées par device de base (_base_device_id), pas par SIM.

    Blacklist : saute les numéros blacklistés, incrémente le compteur d'échecs
    et blackliste automatiquement après FAIL_THRESHOLD échecs.

    Met à jour batch:{batch_id}:meta en temps réel (sent, failed, status).
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

    batch_id   = batch_id or str(uuid.uuid4())[:8]
    meta_key   = f"batch:{batch_id}:meta"
    sent_key   = f"batch:{batch_id}:sent"
    failed_key = f"batch:{batch_id}:failed"

    total_planned = per_device * len(device_ids)

    # Initialise / met à jour le meta (si déjà créé par Flask en mode async, on écrase)
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
        "status":       "running",
    })
    p.expire(meta_key, BATCH_KEY_TTL)
    p.execute()

    sent   = 0
    failed = 0

    for did in device_ids:
        base_did = _base_device_id(did)   # ex: "1|0" → "1" pour les clés Redis

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

            # Blacklist check — saute sans remettre dans la queue (numéro définitivement exclu)
            if is_blacklisted(number):
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": "blacklisted"}, ensure_ascii=False)
                )
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

            ok, detail = gateway_send_message(
                number=number, message=msg, device_id=did, msg_type=msg_type
            )

            if ok:
                sent += 1
                state.device_incr_sent(base_did, 1)
                redis_conn.lpush(
                    sent_key,
                    json.dumps({"device": did, "number": number}, ensure_ascii=False)
                )
            else:
                # Remettre dans la queue + tracker l'échec
                redis_conn.rpush(NL_QUEUE_KEY, json.dumps(contact, ensure_ascii=False))
                failed += 1
                auto_bl = record_failure(number)
                state.device_incr_errors(base_did, 1)
                redis_conn.lpush(
                    failed_key,
                    json.dumps({
                        "device":           did,
                        "number":           number,
                        "error":            detail or "send_failed",
                        "auto_blacklisted": auto_bl,
                    }, ensure_ascii=False)
                )

            # Mise à jour en temps réel (UI peut poller)
            redis_conn.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed)})

    # Finalisation
    p = redis_conn.pipeline()
    p.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed), "status": "done"})
    p.expire(sent_key,   BATCH_KEY_TTL)
    p.expire(failed_key, BATCH_KEY_TTL)
    p.execute()

    log(f"📦 Batch {batch_id} terminé | sent={sent} failed={failed} remaining={nl_remaining_count()}")
    return {"batch_id": batch_id, "sent": sent, "failed": failed}
