"""
services/templates.py
CRUD pour les templates de messages (campagne + auto-reply).
Stockés dans Redis : tmpl:{uuid} (JSON) + tmpl:index (list d'IDs).
"""
import json
import time
import uuid

from services.redis_store import redis_conn

TMPL_PREFIX = "tmpl:"
TMPL_INDEX  = "tmpl:index"


def _now() -> int:
    return int(time.time())


def get_all_templates() -> list:
    """Retourne tous les templates, du plus récent au plus ancien."""
    try:
        ids = redis_conn.lrange(TMPL_INDEX, 0, -1)
        templates = []
        for tid in ids:
            if isinstance(tid, (bytes, bytearray)):
                tid = tid.decode("utf-8", errors="ignore")
            raw = redis_conn.get(f"{TMPL_PREFIX}{tid}")
            if raw:
                try:
                    templates.append(json.loads(raw))
                except Exception:
                    continue
        return templates
    except Exception:
        return []


def get_template(tmpl_id: str) -> dict | None:
    """Retourne un template par ID, ou None."""
    try:
        raw = redis_conn.get(f"{TMPL_PREFIX}{tmpl_id}")
        return json.loads(raw) if raw else None
    except Exception:
        return None


def save_template(name: str, text: str, msg_type: str = "sms", tmpl_id: str = None) -> dict:
    """
    Crée ou met à jour un template.
    Si tmpl_id est fourni → update. Sinon → create (nouvel UUID).
    Retourne le dict du template sauvegardé.
    """
    is_new = not bool(tmpl_id)
    if is_new:
        tmpl_id = str(uuid.uuid4())[:8]

    tmpl = {
        "id":         tmpl_id,
        "name":       (name or "").strip(),
        "text":       (text or "").strip(),
        "type":       msg_type if msg_type in ("sms", "mms") else "sms",
        "updated_ts": _now(),
    }

    redis_conn.set(f"{TMPL_PREFIX}{tmpl_id}", json.dumps(tmpl, ensure_ascii=False))

    if is_new:
        # lpush = plus récent en tête de liste
        redis_conn.lpush(TMPL_INDEX, tmpl_id)

    return tmpl


def delete_template(tmpl_id: str):
    """Supprime un template (clé + entrée dans l'index)."""
    redis_conn.delete(f"{TMPL_PREFIX}{tmpl_id}")
    redis_conn.lrem(TMPL_INDEX, 0, tmpl_id)


def get_templates_texts(tmpl_ids: list) -> list:
    """
    Retourne la liste des textes pour les IDs fournis (ignore les IDs invalides).
    Utilisé pour la sélection aléatoire lors de l'envoi.
    """
    texts = []
    for tid in (tmpl_ids or []):
        t = get_template(str(tid))
        if t and t.get("text"):
            texts.append(t["text"])
    return texts
