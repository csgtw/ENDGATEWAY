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
        "enabled":           True,
        "reply_mode":        2,
        "step0_type":        "sms",
        "step1_type":        "sms",
        "step0_text":        "",
        "step1_text":        "",
        "step0_delay":       0,
        "step1_delay":       0,
        "step0_template_ids": [],
        "step1_template_ids": [],
        "step0_ai_enabled":  False,
        "step0_ai_prompt":   "",
        "step1_ai_enabled":  False,
        "step1_ai_prompt":   "",
        "updated_ts":        0,
    }


def load_autoreply_config() -> dict:
    cfg = _defaults()
    raw = redis_conn.get(CONFIG_KEY)
    if not raw:
        return cfg

    try:
        stored = json.loads(raw.decode("utf-8"))
        if not isinstance(stored, dict):
            return cfg
        cfg.update(stored)

        cfg["enabled"]    = bool(cfg.get("enabled", True))
        cfg["reply_mode"] = 1 if int(cfg.get("reply_mode", 2)) == 1 else 2

        if cfg.get("step0_type") not in ("sms", "mms"):
            cfg["step0_type"] = "sms"
        if cfg.get("step1_type") not in ("sms", "mms"):
            cfg["step1_type"] = "sms"

        cfg["step0_text"] = str(cfg.get("step0_text") or "")
        cfg["step1_text"] = str(cfg.get("step1_text") or "")

        try:
            cfg["step0_delay"] = float(cfg.get("step0_delay") or 0)
        except Exception:
            cfg["step0_delay"] = 0.0
        try:
            cfg["step1_delay"] = float(cfg.get("step1_delay") or 0)
        except Exception:
            cfg["step1_delay"] = 0.0

        for k in ("step0_template_ids", "step1_template_ids"):
            v = cfg.get(k)
            if not isinstance(v, list):
                cfg[k] = []
            else:
                cfg[k] = [str(x) for x in v if x]

        cfg["step0_ai_enabled"] = bool(cfg.get("step0_ai_enabled", False))
        cfg["step1_ai_enabled"] = bool(cfg.get("step1_ai_enabled", False))
        cfg["step0_ai_prompt"]  = str(cfg.get("step0_ai_prompt") or "")
        cfg["step1_ai_prompt"]  = str(cfg.get("step1_ai_prompt") or "")

        try:
            cfg["updated_ts"] = int(cfg.get("updated_ts") or 0)
        except Exception:
            cfg["updated_ts"] = 0

        return cfg
    except Exception:
        return _defaults()


def save_autoreply_config(form) -> dict:
    """
    Valide et sauvegarde la config autoreply depuis un form (dict-like).
    Lève ValueError si la config est invalide.
    """
    cfg = _defaults()

    cfg["enabled"]    = (form.get("enabled") in ("1", "on", "true", "yes"))
    cfg["reply_mode"] = 1 if (form.get("reply_mode") == "1") else 2

    step0_type = (form.get("step0_type") or "sms").lower().strip()
    step1_type = (form.get("step1_type") or "sms").lower().strip()
    cfg["step0_type"] = step0_type if step0_type in ("sms", "mms") else "sms"
    cfg["step1_type"] = step1_type if step1_type in ("sms", "mms") else "sms"

    cfg["step0_text"] = str(form.get("step0_text") or "").strip()
    cfg["step1_text"] = str(form.get("step1_text") or "").strip()

    cfg["step0_ai_enabled"] = str(form.get("step0_ai_enabled", "0")).strip() in ("1", "on", "true", "yes")
    cfg["step1_ai_enabled"] = str(form.get("step1_ai_enabled", "0")).strip() in ("1", "on", "true", "yes")
    cfg["step0_ai_prompt"]  = str(form.get("step0_ai_prompt") or "").strip()
    cfg["step1_ai_prompt"]  = str(form.get("step1_ai_prompt") or "").strip()

    # Délais (secondes, float, min 0)
    try:
        cfg["step0_delay"] = max(0.0, float(form.get("step0_delay") or 0))
    except Exception:
        cfg["step0_delay"] = 0.0
    try:
        cfg["step1_delay"] = max(0.0, float(form.get("step1_delay") or 0))
    except Exception:
        cfg["step1_delay"] = 0.0

    # Template IDs (multi-value ou JSON)
    def _parse_ids(key):
        # Tente getlist (Flask ImmutableMultiDict) puis JSON string
        if hasattr(form, "getlist"):
            ids = [str(x).strip() for x in form.getlist(key) if str(x).strip()]
            if ids:
                return ids
        raw = form.get(key) or ""
        if raw.startswith("["):
            try:
                return [str(x) for x in json.loads(raw) if x]
            except Exception:
                pass
        return []

    cfg["step0_template_ids"] = _parse_ids("step0_template_ids[]")
    cfg["step1_template_ids"] = _parse_ids("step1_template_ids[]")

    if cfg["enabled"]:
        # Au moins un message OU des templates OU le mode IA pour step0
        if not cfg["step0_ai_enabled"] and not cfg["step0_text"] and not cfg["step0_template_ids"]:
            raise ValueError("Message 1 vide (texte, template ou mode IA requis)")
        if cfg["reply_mode"] == 2 and not cfg["step1_ai_enabled"] and not cfg["step1_text"] and not cfg["step1_template_ids"]:
            raise ValueError("Message 2 vide (texte, template ou mode IA requis)")

    # Mode 1 → step1 inutile
    if cfg["reply_mode"] == 1:
        cfg["step1_text"] = ""
        cfg["step1_template_ids"] = []

    cfg["updated_ts"] = _now()
    redis_conn.set(CONFIG_KEY, json.dumps(cfg, ensure_ascii=False))
    return cfg
