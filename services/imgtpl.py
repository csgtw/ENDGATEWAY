"""
services/imgtpl.py
Collection de templates image (MMS / génération).
Un seul template actif à la fois (choix global). Le template actif est TOUJOURS
mirroré vers `nl:template` (bytes) + `config:img:url`, pour que tout le pipeline
existant (imggen, create_batch, /uploads/img/template.jpg) fonctionne inchangé.

Clés Redis :
- imgtpl:ids           sorted set {id -> created_ts}
- imgtpl:{id}:data     bytes JPEG
- imgtpl:{id}:meta     hash {name, created_ts, size}
- imgtpl:active        string : id du template actif
"""
import time
import uuid
from services.redis_store import redis_conn

IDS_KEY    = "imgtpl:ids"
ACTIVE_KEY = "imgtpl:active"


def _data_key(tid: str) -> str:
    return f"imgtpl:{tid}:data"


def _meta_key(tid: str) -> str:
    return f"imgtpl:{tid}:meta"


def _dec(v, default=""):
    if v is None:
        return default
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)


def get_active() -> str:
    try:
        return _dec(redis_conn.get(ACTIVE_KEY))
    except Exception:
        return ""


def _mirror_active_to_legacy(tid: str, base_url: str = ""):
    """Copie les bytes du template actif vers nl:template + config:img:url.
    Garantit que le pipeline existant utilise toujours le template actif."""
    try:
        data = redis_conn.get(_data_key(tid))
        if data:
            redis_conn.set("nl:template", data)
            if base_url:
                redis_conn.set("config:img:url", f"{base_url.rstrip('/')}/uploads/img/template.jpg")
    except Exception:
        pass


def set_active(tid: str, base_url: str = ""):
    tid = (tid or "").strip()
    if not tid or not redis_conn.exists(_meta_key(tid)):
        return False
    redis_conn.set(ACTIVE_KEY, tid)
    _mirror_active_to_legacy(tid, base_url)
    return True


def add_template(name: str, img_bytes: bytes, base_url: str = "") -> str:
    """Crée un template. Le premier créé devient actif automatiquement."""
    tid = str(uuid.uuid4())[:8]
    ts  = int(time.time())
    p = redis_conn.pipeline()
    p.zadd(IDS_KEY, {tid: ts})
    p.set(_data_key(tid), img_bytes)
    p.hset(_meta_key(tid), mapping={
        "name": (name or "Template").strip()[:60],
        "created_ts": str(ts),
        "size": str(len(img_bytes)),
    })
    p.execute()
    # Premier template → actif d'office
    if not get_active():
        set_active(tid, base_url)
    return tid


def rename_template(tid: str, name: str):
    tid = (tid or "").strip()
    if tid and redis_conn.exists(_meta_key(tid)):
        redis_conn.hset(_meta_key(tid), "name", (name or "").strip()[:60])


def delete_template(tid: str, base_url: str = ""):
    tid = (tid or "").strip()
    if not tid:
        return
    was_active = (get_active() == tid)
    p = redis_conn.pipeline()
    p.zrem(IDS_KEY, tid)
    p.delete(_data_key(tid))
    p.delete(_meta_key(tid))
    p.execute()
    if was_active:
        # Réassigne l'actif au plus récent restant, sinon nettoie le legacy
        remaining = list_templates()
        if remaining:
            set_active(remaining[0]["id"], base_url)
        else:
            redis_conn.delete(ACTIVE_KEY)
            redis_conn.delete("nl:template")
            redis_conn.delete("config:img:url")


def get_bytes(tid: str):
    try:
        return redis_conn.get(_data_key(tid))
    except Exception:
        return None


def list_templates() -> list:
    """[{id, name, size, created_ts}] triés par création décroissante."""
    try:
        ids = redis_conn.zrevrange(IDS_KEY, 0, -1) or []
        if not ids:
            return []
        p = redis_conn.pipeline()
        ids_s = [_dec(i) for i in ids]
        for tid in ids_s:
            p.hgetall(_meta_key(tid))
        metas = p.execute()
        out = []
        for tid, meta in zip(ids_s, metas):
            meta = meta or {}
            name = _dec(meta.get(b"name") or meta.get("name"), tid)
            size = _dec(meta.get(b"size") or meta.get("size"), "0")
            out.append({"id": tid, "name": name, "size": int(size or 0)})
        return out
    except Exception:
        return []


def ensure_migrated(base_url: str = ""):
    """Si aucun template dans la collection mais un ancien nl:template existe,
    l'importe comme 'Template 1' et l'active. Idempotent, sans effet si déjà fait."""
    try:
        if redis_conn.exists(IDS_KEY):
            return
        legacy = redis_conn.get("nl:template")
        if legacy:
            tid = add_template("Template 1", legacy, base_url)
            set_active(tid, base_url)
    except Exception:
        pass
