import time
from services.redis_store import redis_conn


# -----------------------------
# Base helpers
# -----------------------------

def _now() -> int:
    return int(time.time())


def get_int(key: str, default: int = 0) -> int:
    try:
        v = redis_conn.get(key)
        if v is None:
            return default
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", errors="ignore")
        return int(v)
    except Exception:
        return default


def set_int(key: str, value: int):
    redis_conn.set(key, int(value))


def incr(key: str, amount: int = 1) -> int:
    try:
        return int(redis_conn.incrby(key, int(amount)))
    except Exception:
        v = get_int(key, 0) + int(amount)
        set_int(key, v)
        return v


def set_lock(key: str, ttl_sec: int) -> bool:
    """Lock simple anti double-traitement (NX + EX)."""
    try:
        return bool(redis_conn.set(key, b"1", nx=True, ex=int(ttl_sec)))
    except Exception:
        return False


# -----------------------------
# Worker heartbeat
# -----------------------------

def worker_heartbeat():
    set_int("stats:worker:last_seen", _now())


def worker_ok(max_age_sec: int = 90) -> bool:
    last = get_int("stats:worker:last_seen", 0)
    return bool(last and (_now() - last) <= max_age_sec)


# -----------------------------
# Cycle limit (config)
# -----------------------------

def cycle_limit_get() -> int:
    v = get_int("config:cycle_limit", 100)
    return v if v > 0 else 100


def cycle_limit_set(v: int):
    v = int(v)
    if v < 1:
        raise ValueError("cycle_limit invalide")
    set_int("config:cycle_limit", v)


# -----------------------------
# Global stats
# -----------------------------

def global_sent_get() -> int:
    return get_int("stats:global:sent", 0)


def reset_global_sent_and_devices():
    """Reset envoyé global + envoyé par device."""
    p = redis_conn.pipeline()
    p.set("stats:global:sent", 0)
    try:
        for k in redis_conn.scan_iter(match="stats:device:*:sent", count=500):
            p.set(k, 0)
    except Exception:
        pass
    p.execute()


# -----------------------------
# Device stats + cycles
# -----------------------------

def device_mark_seen(device_id: str):
    set_int(f"stats:device:{device_id}:last_seen", _now())


def device_incr_received(device_id: str, amount: int = 1):
    p = redis_conn.pipeline()
    p.incrby(f"stats:device:{device_id}:received", int(amount))
    p.incrby(f"cycle:device:{device_id}:received", int(amount))
    p.execute()


def device_incr_sent(device_id: str, amount: int = 1):
    p = redis_conn.pipeline()
    p.incrby("stats:global:sent", int(amount))
    p.incrby(f"stats:device:{device_id}:sent", int(amount))
    p.incrby(f"cycle:device:{device_id}:sent", int(amount))
    p.execute()


def device_incr_errors(device_id: str, amount: int = 1):
    incr(f"stats:device:{device_id}:errors", int(amount))


def device_cycle_relancer(device_id: str):
    """Nouveau cycle: index++ + compteurs cycle à 0"""
    p = redis_conn.pipeline()
    p.set(f"cycle:device:{device_id}:received", 0)
    p.set(f"cycle:device:{device_id}:sent", 0)
    p.incr(f"cycle:device:{device_id}:index")
    p.execute()


def cycles_reset_all(device_ids: list):
    """Reset cycles (0) pour la liste de devices (ne doit PAS incrémenter)."""
    p = redis_conn.pipeline()
    for did in device_ids:
        p.set(f"cycle:device:{did}:received", 0)
        p.set(f"cycle:device:{did}:sent", 0)
        p.set(f"cycle:device:{did}:index", 0)
    p.execute()


def device_snapshot(device_id: str) -> dict:
    base = f"stats:device:{device_id}:"
    last_seen = get_int(base + "last_seen", 0)
    online = bool(last_seen and (_now() - last_seen) <= 600)

    received = get_int(base + "received", 0)
    sent = get_int(base + "sent", 0)
    errors = get_int(base + "errors", 0)

    cycle_received = get_int(f"cycle:device:{device_id}:received", 0)
    cycle_index = get_int(f"cycle:device:{device_id}:index", 0)
    limit = cycle_limit_get()

    pct = 0
    try:
        pct = int(min(100, (cycle_received * 100) / max(1, limit)))
    except Exception:
        pct = 0

    cycle_state = "neutral"
    if cycle_received >= limit:
        cycle_state = "good"
    elif cycle_received >= int(limit * 0.9):
        cycle_state = "warn"

    return {
        "device_id": device_id,
        "online": online,
        "received": received,
        "sent": sent,
        "errors": errors,
        "cycle_received": cycle_received,
        "cycle_limit": limit,
        "cycle_index": cycle_index,
        "cycle_state": cycle_state,
        "cycle_pct": pct,
    }
