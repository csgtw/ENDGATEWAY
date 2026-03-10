"""
services/camps.py
Blocs de messages nommés — pools de messages réutilisables pour les campagnes.
Chaque bloc est une liste Redis. Le bloc actif est utilisé par create_batch.
"""
import csv
import io
import time
import uuid

from services.redis_store import redis_conn

CAMP_IDS_KEY  = "camp:ids"   # sorted set: id → created_ts
CAMP_ACTIVE_KEY = "camp:active"  # string: active camp id


def _msgs_key(cid: str) -> str:
    return f"camp:{cid}:msgs"


def _meta_key(cid: str) -> str:
    return f"camp:{cid}:meta"


def list_camps() -> list:
    """Retourne [{id, name, count}] triés par création desc."""
    try:
        ids = redis_conn.zrevrange(CAMP_IDS_KEY, 0, -1)
        if not ids:
            return []
        p = redis_conn.pipeline()
        for cid in ids:
            cid_s = cid.decode() if isinstance(cid, bytes) else cid
            p.hgetall(_meta_key(cid_s))
            p.llen(_msgs_key(cid_s))
        results = p.execute()
        out = []
        for i, cid in enumerate(ids):
            cid_s = cid.decode() if isinstance(cid, bytes) else cid
            meta   = results[i * 2] or {}
            count  = int(results[i * 2 + 1] or 0)
            name   = meta.get(b"name") or meta.get("name") or cid_s
            if isinstance(name, bytes):
                name = name.decode()
            out.append({"id": cid_s, "name": str(name), "count": count})
        return out
    except Exception:
        return []


def create_camp(name: str) -> str:
    """Crée un nouveau bloc, retourne son ID."""
    cid = str(uuid.uuid4())[:8]
    ts  = time.time()
    p   = redis_conn.pipeline()
    p.zadd(CAMP_IDS_KEY, {cid: ts})
    p.hset(_meta_key(cid), mapping={"name": (name or "Bloc").strip(), "created_ts": str(int(ts))})
    p.execute()
    return cid


def get_messages(cid: str) -> list:
    try:
        items = redis_conn.lrange(_msgs_key(cid), 0, -1)
        return [x.decode() if isinstance(x, bytes) else x for x in items]
    except Exception:
        return []


def count_messages(cid: str) -> int:
    try:
        return int(redis_conn.llen(_msgs_key(cid)) or 0)
    except Exception:
        return 0


def add_message(cid: str, text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    try:
        redis_conn.rpush(_msgs_key(cid), text)
        return True
    except Exception:
        return False


def delete_message(cid: str, text: str) -> bool:
    try:
        return bool(redis_conn.lrem(_msgs_key(cid), 1, text))
    except Exception:
        return False


def import_csv_bytes(cid: str, data: bytes) -> int:
    try:
        text   = data.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text))
        rows   = list(reader)
        if not rows:
            return 0
        header    = [c.strip().lower() for c in (rows[0] or [])]
        msg_col   = 0
        start_row = 0
        for i, h in enumerate(header):
            if h in ("message", "msg", "text", "texte"):
                msg_col   = i
                start_row = 1
                break
        else:
            try:
                float(rows[0][0])
            except Exception:
                start_row = 1
        count = 0
        p = redis_conn.pipeline()
        for row in rows[start_row:]:
            if not row:
                continue
            try:
                val = row[msg_col].strip()
            except IndexError:
                continue
            if val:
                p.rpush(_msgs_key(cid), val)
                count += 1
        if count:
            p.execute()
        return count
    except Exception:
        return 0


def clear_messages(cid: str):
    try:
        redis_conn.delete(_msgs_key(cid))
    except Exception:
        pass


def delete_camp(cid: str):
    try:
        p = redis_conn.pipeline()
        p.zrem(CAMP_IDS_KEY, cid)
        p.delete(_msgs_key(cid))
        p.delete(_meta_key(cid))
        p.execute()
        if get_active() == cid:
            redis_conn.delete(CAMP_ACTIVE_KEY)
    except Exception:
        pass


def get_active() -> str:
    try:
        v = redis_conn.get(CAMP_ACTIVE_KEY)
        return (v.decode() if isinstance(v, bytes) else v) or ""
    except Exception:
        return ""


def set_active(cid: str):
    try:
        if cid:
            redis_conn.set(CAMP_ACTIVE_KEY, cid)
        else:
            redis_conn.delete(CAMP_ACTIVE_KEY)
    except Exception:
        pass


def rename_camp(cid: str, new_name: str):
    try:
        redis_conn.hset(_meta_key(cid), "name", (new_name or "").strip())
    except Exception:
        pass


def ensure_default():
    """Si aucun bloc n'existe, crée 'Défaut' depuis tpl:campaign + l'active."""
    try:
        if redis_conn.exists(CAMP_IDS_KEY):
            return
        from services import msgtpl
        existing = msgtpl.get_all("campaign")
        cid = create_camp("Défaut")
        if existing:
            p = redis_conn.pipeline()
            for msg in existing:
                p.rpush(_msgs_key(cid), msg)
            p.execute()
        set_active(cid)
    except Exception:
        pass
