"""
services/blacklist.py
Blacklist automatique des numéros en échec répété.
"""
from services.redis_store import redis_conn

BLACKLIST_KEY   = "blacklist:numbers"
FAIL_KEY_PREFIX = "fail_count:"
FAIL_THRESHOLD  = 3           # échecs consécutifs avant blacklist auto
FAIL_TTL        = 7 * 24 * 3600  # compteur expire après 7 jours


def is_blacklisted(number: str) -> bool:
    try:
        return bool(redis_conn.sismember(BLACKLIST_KEY, number))
    except Exception:
        return False


def record_failure(number: str) -> bool:
    """
    Incrémente le compteur d'échecs du numéro.
    Retourne True si le numéro vient d'être blacklisté automatiquement.
    """
    try:
        key = f"{FAIL_KEY_PREFIX}{number}"
        count = int(redis_conn.incr(key))
        redis_conn.expire(key, FAIL_TTL)
        if count >= FAIL_THRESHOLD:
            redis_conn.sadd(BLACKLIST_KEY, number)
            return True
        return False
    except Exception:
        return False


def get_blacklist() -> list:
    try:
        members = redis_conn.smembers(BLACKLIST_KEY) or set()
        return sorted([
            m.decode("utf-8") if isinstance(m, bytes) else str(m)
            for m in members
        ])
    except Exception:
        return []


def blacklist_count() -> int:
    try:
        return int(redis_conn.scard(BLACKLIST_KEY) or 0)
    except Exception:
        return 0


def remove_from_blacklist(number: str):
    try:
        p = redis_conn.pipeline()
        p.srem(BLACKLIST_KEY, number)
        p.delete(f"{FAIL_KEY_PREFIX}{number}")
        p.execute()
    except Exception:
        pass


def clear_blacklist():
    try:
        numbers = redis_conn.smembers(BLACKLIST_KEY) or set()
        p = redis_conn.pipeline()
        p.delete(BLACKLIST_KEY)
        for n in numbers:
            num = n.decode("utf-8") if isinstance(n, bytes) else str(n)
            p.delete(f"{FAIL_KEY_PREFIX}{num}")
        p.execute()
    except Exception:
        pass
