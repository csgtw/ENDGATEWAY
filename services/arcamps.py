"""
services/arcamps.py
Blocs de messages nommés pour l'auto-reply.
Chaque bloc a un pool step0 (Message 1) et step1 (Message 2).
"""
import csv
import io
import random
import time
import uuid

from services.redis_store import redis_conn

ARCAMP_IDS_KEY         = "arcamp:ids"          # sorted set: id → created_ts
ARCAMP_ACTIVE_STEP0_KEY = "arcamp:active:step0"  # string: active arcamp id for step0
ARCAMP_ACTIVE_STEP1_KEY = "arcamp:active:step1"  # string: active arcamp id for step1
_ARCAMP_ACTIVE_LEGACY_KEY = "arcamp:active"      # migration compat


def _active_key(step: int) -> str:
    return ARCAMP_ACTIVE_STEP0_KEY if step == 0 else ARCAMP_ACTIVE_STEP1_KEY


def _step_key(cid: str, step: int) -> str:
    return f"arcamp:{cid}:step{step}"


def _meta_key(cid: str) -> str:
    return f"arcamp:{cid}:meta"


def list_arcamps() -> list:
    """Retourne [{id, name, count0, count1}] triés par création desc."""
    try:
        ids = redis_conn.zrevrange(ARCAMP_IDS_KEY, 0, -1)
        if not ids:
            return []
        p = redis_conn.pipeline()
        for cid in ids:
            cid_s = cid.decode() if isinstance(cid, bytes) else cid
            p.hgetall(_meta_key(cid_s))
            p.llen(_step_key(cid_s, 0))
            p.llen(_step_key(cid_s, 1))
        results = p.execute()
        out = []
        for i, cid in enumerate(ids):
            cid_s  = cid.decode() if isinstance(cid, bytes) else cid
            meta   = results[i * 3] or {}
            count0 = int(results[i * 3 + 1] or 0)
            count1 = int(results[i * 3 + 2] or 0)
            name   = meta.get(b"name") or meta.get("name") or cid_s
            if isinstance(name, bytes):
                name = name.decode()
            out.append({"id": cid_s, "name": str(name), "count0": count0, "count1": count1})
        return out
    except Exception:
        return []


def create_arcamp(name: str) -> str:
    """Crée un nouveau bloc AR, retourne son ID."""
    cid = str(uuid.uuid4())[:8]
    ts  = time.time()
    p   = redis_conn.pipeline()
    p.zadd(ARCAMP_IDS_KEY, {cid: ts})
    p.hset(_meta_key(cid), mapping={"name": (name or "Bloc AR").strip(), "created_ts": str(int(ts))})
    p.execute()
    return cid


def get_messages(cid: str, step: int) -> list:
    try:
        items = redis_conn.lrange(_step_key(cid, step), 0, -1)
        return [x.decode() if isinstance(x, bytes) else x for x in items]
    except Exception:
        return []


def count_messages(cid: str, step: int) -> int:
    try:
        return int(redis_conn.llen(_step_key(cid, step)) or 0)
    except Exception:
        return 0


def add_message(cid: str, step: int, text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    try:
        redis_conn.rpush(_step_key(cid, step), text)
        return True
    except Exception:
        return False


def delete_message(cid: str, step: int, text: str) -> bool:
    try:
        return bool(redis_conn.lrem(_step_key(cid, step), 1, text))
    except Exception:
        return False


def import_csv_bytes(cid: str, step: int, data: bytes) -> int:
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
                p.rpush(_step_key(cid, step), val)
                count += 1
        if count:
            p.execute()
        return count
    except Exception:
        return 0


def clear_messages(cid: str, step: int):
    try:
        redis_conn.delete(_step_key(cid, step))
    except Exception:
        pass


def pick_random(cid: str, step: int) -> object:
    try:
        n = redis_conn.llen(_step_key(cid, step))
        if not n:
            return None
        idx = random.randint(0, n - 1)
        val = redis_conn.lindex(_step_key(cid, step), idx)
        if val is None:
            return None
        return val.decode() if isinstance(val, bytes) else val
    except Exception:
        return None


def delete_arcamp(cid: str):
    try:
        p = redis_conn.pipeline()
        p.zrem(ARCAMP_IDS_KEY, cid)
        p.delete(_step_key(cid, 0))
        p.delete(_step_key(cid, 1))
        p.delete(_meta_key(cid))
        p.execute()
        if get_active_step(0) == cid:
            redis_conn.delete(ARCAMP_ACTIVE_STEP0_KEY)
        if get_active_step(1) == cid:
            redis_conn.delete(ARCAMP_ACTIVE_STEP1_KEY)
    except Exception:
        pass


def get_active_step(step: int) -> str:
    """Retourne l'ID du bloc actif pour le step donné. Migration depuis l'ancienne clé unique."""
    try:
        v = redis_conn.get(_active_key(step))
        if v:
            return (v.decode() if isinstance(v, bytes) else v) or ""
        # Migration : si l'ancienne clé existe, migrer vers les deux nouvelles
        legacy = redis_conn.get(_ARCAMP_ACTIVE_LEGACY_KEY)
        if legacy:
            cid = (legacy.decode() if isinstance(legacy, bytes) else legacy) or ""
            if cid:
                redis_conn.set(ARCAMP_ACTIVE_STEP0_KEY, cid)
                redis_conn.set(ARCAMP_ACTIVE_STEP1_KEY, cid)
                redis_conn.delete(_ARCAMP_ACTIVE_LEGACY_KEY)
                return cid
        return ""
    except Exception:
        return ""


def set_active_step(step: int, cid: str):
    """Définit le bloc actif pour le step donné (0 ou 1)."""
    try:
        if cid:
            redis_conn.set(_active_key(step), cid)
        else:
            redis_conn.delete(_active_key(step))
    except Exception:
        pass


def rename_arcamp(cid: str, new_name: str):
    try:
        redis_conn.hset(_meta_key(cid), "name", (new_name or "").strip())
    except Exception:
        pass


def ensure_default():
    """Si aucun bloc AR n'existe, crée 'Défaut' depuis tpl:ar:step0/step1."""
    try:
        if redis_conn.exists(ARCAMP_IDS_KEY):
            get_active_step(0)  # déclenche migration si besoin
            return
        from services import msgtpl
        step0_msgs = msgtpl.get_all("ar:step0")
        step1_msgs = msgtpl.get_all("ar:step1")
        cid = create_arcamp("Défaut")
        p = redis_conn.pipeline()
        for msg in step0_msgs:
            p.rpush(_step_key(cid, 0), msg)
        for msg in step1_msgs:
            p.rpush(_step_key(cid, 1), msg)
        if step0_msgs or step1_msgs:
            p.execute()
        set_active_step(0, cid)
        set_active_step(1, cid)
    except Exception:
        pass
