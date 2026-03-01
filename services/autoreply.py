"""
services/autoreply.py
Source unique de vérité pour la config auto-reply.
Importé par app.py ET tasks.py — jamais dupliqué.
"""
import json
import time
from services.redis_store import redis_conn

CONFIG_KEY = "config:autoreply"


def _now() -> int:
    return int(time.time())


def _defaults() -> dict:
    return {
        "enabled": True,
        "reply_mode": 2,
        "step0_type": "sms",
        "step1_type": "sms",
        "step0_text": "",
        "step1_text": "",
        "updated_ts": 0,
    }


def load_autoreply_config() -> dict:
    defaults = _defaults()
    raw = redis_conn.get(CONFIG_KEY)
    if not raw:
        return defaults

    try:
        cfg = json.loads(raw.decode("utf-8"))
        if not isinstance(cfg, dict):
            return defaults
        defaults.update(cfg)

        defaults["enabled"] = bool(defaults.get("enabled", True))
        defaults["reply_mode"] = 1 if int(defaults.get("reply_mode", 2)) == 1 else 2

        if defaults.get("step0_type") not in ("sms", "mms"):
            defaults["step0_type"] = "sms"
        if defaults.get("step1_type") not in ("sms", "mms"):
            defaults["step1_type"] = "sms"

        defaults["step0_text"] = str(defaults.get("step0_text") or "")
        defaults["step1_text"] = str(defaults.get("step1_text") or "")

        try:
            defaults["updated_ts"] = int(defaults.get("updated_ts") or 0)
        except Exception:
            defaults["updated_ts"] = 0

        return defaults
    except Exception:
        return defaults


def save_autoreply_config(form) -> dict:
    """
    Valide et sauvegarde la config autoreply depuis un form (dict-like).
    Lève ValueError si la config est invalide.
    """
    cfg = _defaults()

    cfg["enabled"] = (form.get("enabled") in ("1", "on", "true", "yes"))
    cfg["reply_mode"] = 1 if (form.get("reply_mode") == "1") else 2

    step0_type = (form.get("step0_type") or "sms").lower().strip()
    step1_type = (form.get("step1_type") or "sms").lower().strip()
    cfg["step0_type"] = step0_type if step0_type in ("sms", "mms") else "sms"
    cfg["step1_type"] = step1_type if step1_type in ("sms", "mms") else "sms"

    cfg["step0_text"] = str(form.get("step0_text") or "").strip()
    cfg["step1_text"] = str(form.get("step1_text") or "").strip()

    if cfg["enabled"]:
        if not cfg["step0_text"]:
            raise ValueError("Message 1 vide")
        if cfg["reply_mode"] == 2 and not cfg["step1_text"]:
            raise ValueError("Message 2 vide")

    # Mode 1 → step1 inutile
    if cfg["reply_mode"] == 1:
        cfg["step1_text"] = ""

    cfg["updated_ts"] = _now()
    redis_conn.set(CONFIG_KEY, json.dumps(cfg, ensure_ascii=False))
    return cfg
