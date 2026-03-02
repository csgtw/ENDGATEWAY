import json
import random
import time
import uuid

from services.redis_store import redis_conn
from services.numlist import NL_QUEUE_KEY, load_message_draft, nl_remaining_count
from services.gateway import gateway_send_message
from services import state
from services.blacklist import is_blacklisted, record_failure
from logger import log

BATCH_KEY_TTL = 24 * 3600  # 24h — clés batch expirent automatiquement

SEND_SPEED_KEY        = "config:send_speed"
CAMPAIGN_TMPL_IDS_KEY = "nl:campaign_template_ids"


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


def _parse_speed_delay(speed_str: str) -> tuple:
    """
    Parse la valeur de vitesse d'envoi.
    '0'   → (0, 0)
    '1'   → (1.0, 1.0)
    '1-2' → (1.0, 2.0)
    Retourne (min_sec, max_sec).
    """
    s = (speed_str or "0").strip()
    if "-" in s:
        parts = s.split("-", 1)
        try:
            return float(parts[0]), float(parts[1])
        except Exception:
            pass
    try:
        v = float(s)
        return v, v
    except Exception:
        return 0.0, 0.0


def get_send_speed() -> str:
    """Retourne la vitesse d'envoi configurée (ex: '0', '1', '1-2')."""
    try:
        v = redis_conn.get(SEND_SPEED_KEY)
        return v.decode("utf-8").strip() if v else "0"
    except Exception:
        return "0"


def save_send_speed(speed_str: str):
    """Sauvegarde la vitesse d'envoi en Redis."""
    redis_conn.set(SEND_SPEED_KEY, (speed_str or "0").strip())


def save_campaign_template_ids(tmpl_ids: list):
    """Sauvegarde les IDs de templates sélectionnés pour la campagne."""
    if tmpl_ids:
        redis_conn.set(CAMPAIGN_TMPL_IDS_KEY, json.dumps([str(x) for x in tmpl_ids]))
    else:
        redis_conn.delete(CAMPAIGN_TMPL_IDS_KEY)


def load_campaign_template_ids() -> list:
    """Retourne les IDs de templates sélectionnés pour la campagne."""
    try:
        raw = redis_conn.get(CAMPAIGN_TMPL_IDS_KEY)
        if not raw:
            return []
        return json.loads(raw)
    except Exception:
        return []


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

def create_batch(device_ids, per_device: int, batch_id: str = None, template_ids: list = None):
    """
    Dépile les contacts, applique le template, envoie via le gateway.

    Multi-SIM : device_id peut être '1|0' (device 1, SIM slot 0).
    Stats Redis trackées par device de base (_base_device_id), pas par SIM.

    Blacklist : saute les numéros blacklistés.

    Pause : si batch:{batch_id}:paused est défini, stoppe proprement (status="paused").
    Annulation : si batch:{batch_id}:cancelled est défini, stoppe (status="cancelled").

    Vitesse : config:send_speed définit le délai entre chaque envoi.
    Templates : si template_ids fournis, chaque contact reçoit un template aléatoire.
    """
    device_ids = [str(x) for x in (device_ids or []) if str(x).strip()]
    if not device_ids:
        raise ValueError("Aucun appareil")
    if per_device <= 0:
        raise ValueError("per_device invalide")

    remaining = nl_remaining_count()
    if remaining <= 0:
        raise ValueError("Numlist vide")

    # Résolution des templates
    from services.templates import get_templates_texts
    tmpl_texts = []
    if template_ids:
        tmpl_texts = get_templates_texts(template_ids)

    # Fallback sur nl:draft si pas de templates
    if not tmpl_texts:
        msg_template, msg_type = load_message_draft()
        msg_template = (msg_template or "").strip()
        if not msg_template:
            raise ValueError("Message campagne manquant")
    else:
        # Le type est pris depuis le premier template (ou on utilise "sms" par défaut)
        msg_template = None
        _, msg_type = load_message_draft()
        msg_type = msg_type or "sms"

    # Vitesse d'envoi
    speed_str = get_send_speed()
    delay_min, delay_max = _parse_speed_delay(speed_str)

    batch_id   = batch_id or str(uuid.uuid4())[:8]
    meta_key   = f"batch:{batch_id}:meta"
    sent_key   = f"batch:{batch_id}:sent"
    failed_key = f"batch:{batch_id}:failed"
    pause_key  = f"batch:{batch_id}:paused"
    cancel_key = f"batch:{batch_id}:cancelled"

    total_planned = per_device * len(device_ids)

    # Initialise / reprend le meta
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
        base_did = _base_device_id(did)

        for _ in range(per_device):
            # ── Vérifications pause / annulation ──────────────────────────
            if redis_conn.exists(cancel_key):
                redis_conn.hset(meta_key, mapping={
                    "sent": str(sent), "failed": str(failed), "status": "cancelled"
                })
                log(f"🚫 Batch {batch_id} annulé | sent={sent} failed={failed}")
                return {"batch_id": batch_id, "sent": sent, "failed": failed, "status": "cancelled"}

            if redis_conn.exists(pause_key):
                redis_conn.hset(meta_key, mapping={
                    "sent": str(sent), "failed": str(failed), "status": "paused"
                })
                log(f"⏸ Batch {batch_id} en pause | sent={sent} failed={failed}")
                return {"batch_id": batch_id, "sent": sent, "failed": failed, "status": "paused"}

            # ── Délai vitesse d'envoi ──────────────────────────────────────
            if delay_max > 0:
                sleep_time = random.uniform(delay_min, delay_max) if delay_max != delay_min else delay_min
                time.sleep(sleep_time)

            # ── Dépilage contact ──────────────────────────────────────────
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

            if is_blacklisted(number):
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": "blacklisted"}, ensure_ascii=False)
                )
                continue

            # ── Choix du message (aléatoire si templates) ─────────────────
            if tmpl_texts:
                chosen_template = random.choice(tmpl_texts)
            else:
                chosen_template = msg_template

            msg = render_message(chosen_template, contact).strip()
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

            redis_conn.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed)})

    # Finalisation
    p = redis_conn.pipeline()
    p.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed), "status": "done"})
    p.expire(sent_key,   BATCH_KEY_TTL)
    p.expire(failed_key, BATCH_KEY_TTL)
    p.execute()

    log(f"📦 Batch {batch_id} terminé | sent={sent} failed={failed} remaining={nl_remaining_count()}")
    return {"batch_id": batch_id, "sent": sent, "failed": failed}
