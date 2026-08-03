"""
services/links.py
Pool de liens multiples avec rotation.
Le lien injecté dans %link% tourne tous les X messages envoyés.
Compteur d'envois par lien, global (tous appareils), continu jusqu'au reset manuel.

Rétrocompat : si le pool est vide, on retombe sur config:global_link (lien unique legacy).
"""
import json
from services.redis_store import redis_conn

POOL_KEY     = "links:pool"          # Redis list : URLs ordonnées
ACTIVE_KEY   = "links:active"        # Redis set  : URLs actives
ROTATE_KEY   = "links:rotate_every"  # int : nb de messages avant de changer de lien
CURSOR_KEY   = "links:cursor"        # int atomique : position de rotation (INCR sur envoi réussi)
SENT_KEY     = "links:sent"          # hash : URL -> nb envoyé (tous appareils)
ROTATION_KEY = "links:rotation_on"   # "1"/"0" : rotation activée (défaut ON)

DEFAULT_ROTATE = 100


def rotation_enabled() -> bool:
    """True si la rotation est activée (défaut). Si OFF, on utilise toujours le 1er lien actif."""
    try:
        v = redis_conn.get(ROTATION_KEY)
        if v is None:
            return True  # défaut = activée
        return _dec(v, "1") == "1"
    except Exception:
        return True


def set_rotation_enabled(on: bool):
    try:
        redis_conn.set(ROTATION_KEY, "1" if on else "0")
    except Exception:
        pass


def _dec(v, default=""):
    if v is None:
        return default
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)


def get_pool() -> list:
    """Liste ordonnée des URLs du pool."""
    try:
        return [_dec(x) for x in (redis_conn.lrange(POOL_KEY, 0, -1) or [])]
    except Exception:
        return []


def _active_set() -> set:
    try:
        return {_dec(x) for x in (redis_conn.smembers(ACTIVE_KEY) or set())}
    except Exception:
        return set()


def get_active_ordered() -> list:
    """URLs actives, dans l'ordre du pool (rotation déterministe)."""
    active = _active_set()
    return [u for u in get_pool() if u in active]


def add_link(url: str) -> bool:
    url = (url or "").strip()
    if not url:
        return False
    try:
        # Évite les doublons
        if url in get_pool():
            return False
        redis_conn.rpush(POOL_KEY, url)
        redis_conn.sadd(ACTIVE_KEY, url)  # actif par défaut
        return True
    except Exception:
        return False


def remove_link(url: str) -> bool:
    url = (url or "").strip()
    if not url:
        return False
    try:
        p = redis_conn.pipeline()
        p.lrem(POOL_KEY, 0, url)
        p.srem(ACTIVE_KEY, url)
        p.hdel(SENT_KEY, url)
        p.execute()
        return True
    except Exception:
        return False


def set_active(url: str, active: bool):
    url = (url or "").strip()
    if not url:
        return
    try:
        if active:
            redis_conn.sadd(ACTIVE_KEY, url)
        else:
            redis_conn.srem(ACTIVE_KEY, url)
    except Exception:
        pass


def get_rotate_every() -> int:
    try:
        v = int(_dec(redis_conn.get(ROTATE_KEY), str(DEFAULT_ROTATE)))
        return v if v >= 1 else DEFAULT_ROTATE
    except Exception:
        return DEFAULT_ROTATE


def set_rotate_every(x):
    try:
        x = int(x)
    except Exception:
        x = DEFAULT_ROTATE
    if x < 1:
        x = 1
    redis_conn.set(ROTATE_KEY, x)


def reset_counters():
    """Remet à zéro la rotation et les compteurs par lien."""
    try:
        p = redis_conn.pipeline()
        p.delete(CURSOR_KEY)
        p.delete(SENT_KEY)
        p.execute()
    except Exception:
        pass


def _sent_map() -> dict:
    try:
        raw = redis_conn.hgetall(SENT_KEY) or {}
        return {_dec(k): int(_dec(v, "0") or 0) for k, v in raw.items()}
    except Exception:
        return {}


def next_rotating_link(active_ordered: list, rotate_every: int) -> str:
    """Avance la rotation (1 seul INCR atomique) et retourne le lien.
    active_ordered et rotate_every sont fournis par l'appelant (chargés UNE fois
    par batch) → aucune autre lecture Redis par message."""
    if not active_ordered:
        return ""
    try:
        new = int(redis_conn.incr(CURSOR_KEY))
        idx = ((new - 1) // max(1, int(rotate_every))) % len(active_ordered)
        return active_ordered[idx]
    except Exception:
        return active_ordered[0]


def record_sent(url: str):
    """Incrémente le compteur d'envois d'un lien (1 seul appel Redis).
    L'appelant garantit que le lien provient du pool."""
    if not url:
        return
    try:
        redis_conn.hincrby(SENT_KEY, url, 1)
    except Exception:
        pass


def peek_link() -> str:
    """Retourne le lien à utiliser pour le prochain envoi SANS avancer le compteur.
    Fallback sur le lien global legacy si aucun lien actif dans le pool."""
    active = get_active_ordered()
    if not active:
        # Fallback legacy : lien unique config:global_link
        try:
            return _dec(redis_conn.get("config:global_link"))
        except Exception:
            return ""
    x = get_rotate_every()
    try:
        cur = int(_dec(redis_conn.get(CURSOR_KEY), "0") or 0)
    except Exception:
        cur = 0
    idx = (cur // max(1, x)) % len(active)
    return active[idx]


def commit_link(url: str):
    """Sur envoi réussi : avance la rotation + incrémente le compteur du lien.
    Ne fait rien si le lien n'appartient pas au pool (mode legacy)."""
    url = (url or "").strip()
    if not url:
        return
    try:
        if url in _active_set():
            p = redis_conn.pipeline()
            p.incr(CURSOR_KEY)
            p.hincrby(SENT_KEY, url, 1)
            p.execute()
    except Exception:
        pass


def ensure_migrated():
    """Si le pool est vide mais qu'un ancien config:global_link existe, l'importe
    comme premier lien actif. Idempotent — ne fait rien si le pool est déjà peuplé."""
    try:
        if redis_conn.exists(POOL_KEY):
            return
        legacy = _dec(redis_conn.get("config:global_link"))
        if legacy:
            add_link(legacy)
    except Exception:
        pass


def get_state() -> dict:
    """État complet pour l'UI."""
    pool   = get_pool()
    active = _active_set()
    sent   = _sent_map()
    x      = get_rotate_every()
    active_ordered = [u for u in pool if u in active]
    try:
        cur = int(_dec(redis_conn.get(CURSOR_KEY), "0") or 0)
    except Exception:
        cur = 0
    current_idx = (cur // max(1, x)) % len(active_ordered) if active_ordered else -1
    current_url = active_ordered[current_idx] if current_idx >= 0 else ""
    rot_on = rotation_enabled()
    # Si rotation OFF → le lien "courant" est toujours le 1er actif
    if not rot_on:
        current_url = active_ordered[0] if active_ordered else ""
    return {
        "links": [{"url": u, "active": u in active, "sent": sent.get(u, 0)} for u in pool],
        "rotate_every": x,
        "rotation_enabled": rot_on,
        "total_sent": sum(sent.values()),
        "current_url": current_url,
        "active_count": len(active_ordered),
    }
