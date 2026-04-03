"""
services/msgtpl.py
Pools de messages par slot Redis.
Chaque slot (campaign, ar:step0, ar:step1) = liste Redis (rpush/lrange).
1 message → toujours utilisé ; N messages → 1 tiré au hasard par envoi.
"""
import csv
import io
import random

from services.redis_store import redis_conn

SLOTS = {
    "campaign": "tpl:campaign",
    "ar:step0":  "tpl:ar:step0",
    "ar:step1":  "tpl:ar:step1",
}


def _key(slot: str) -> str:
    k = SLOTS.get(slot)
    if not k:
        raise ValueError(f"Slot inconnu : {slot!r}")
    return k


def get_all(slot: str) -> list[str]:
    """Retourne tous les messages du pool (ordre d'insertion)."""
    try:
        items = redis_conn.lrange(_key(slot), 0, -1)
        return [x.decode("utf-8") if isinstance(x, bytes) else x for x in items]
    except Exception:
        return []


def count(slot: str) -> int:
    """Nombre de messages dans le pool."""
    try:
        return redis_conn.llen(_key(slot))
    except Exception:
        return 0


def add(slot: str, text: str) -> bool:
    """Ajoute un message en fin de liste. Retourne True si ajouté."""
    text = (text or "").strip()
    if not text:
        return False
    try:
        redis_conn.rpush(_key(slot), text)
        return True
    except Exception:
        return False


def delete(slot: str, text: str) -> bool:
    """Supprime la première occurrence exacte du texte. Retourne True si supprimé."""
    text = (text or "").strip()
    if not text:
        return False
    try:
        removed = redis_conn.lrem(_key(slot), 1, text)
        return bool(removed)
    except Exception:
        return False


def import_csv_bytes(slot: str, file_bytes: bytes) -> int:
    """
    Parse un CSV (bytes) et ajoute chaque ligne non-vide dans le pool.
    Détecte la colonne 'message', 'msg' ou 'text' (case-insensitive) ; sinon colonne 0.
    Retourne le nombre de messages ajoutés.
    """
    added = 0
    try:
        text_content = file_bytes.decode("utf-8-sig", errors="replace")
        reader = csv.reader(io.StringIO(text_content))
        rows = list(reader)
        if not rows:
            return 0

        # Détecter l'en-tête
        header = [h.strip().lower() for h in rows[0]]
        col_idx = 0
        is_header = False
        for target in ("message", "msg", "text"):
            if target in header:
                col_idx = header.index(target)
                is_header = True
                break

        start = 1 if is_header else 0
        for row in rows[start:]:
            if not row or col_idx >= len(row):
                continue
            val = row[col_idx].strip()
            if val:
                redis_conn.rpush(_key(slot), val)
                added += 1
    except Exception:
        pass
    return added


def clear(slot: str):
    """Vide le pool."""
    try:
        redis_conn.delete(_key(slot))
    except Exception:
        pass


def pick_random(slot: str) -> object:
    """Retourne un message aléatoire du pool, ou None si vide."""
    try:
        n = redis_conn.llen(_key(slot))
        if n <= 0:
            return None
        idx = random.randint(0, n - 1)
        val = redis_conn.lindex(_key(slot), idx)
        if val is None:
            return None
        return val.decode("utf-8") if isinstance(val, bytes) else val
    except Exception:
        return None
