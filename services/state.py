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


def device_reset_field(device_id: str, field: str):
    """Reset un compteur de stats pour un device. Seul owner des clés Redis de stats."""
    if field not in ("sent", "received", "errors"):
        raise ValueError(f"field invalide: {field}")
    p = redis_conn.pipeline()
    p.set(f"stats:device:{device_id}:{field}", 0)
    if field == "received":
        p.set(f"cycle:device:{device_id}:received", 0)
    p.execute()


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
    """Snapshot complet d'un device — 1 seul pipeline Redis au lieu de 7 appels."""
    base = f"stats:device:{device_id}:"
    p = redis_conn.pipeline()
    p.get(base + "last_seen")
    p.get(base + "received")
    p.get(base + "sent")
    p.get(base + "errors")
    p.get(f"cycle:device:{device_id}:received")
    p.get(f"cycle:device:{device_id}:index")
    p.get("config:cycle_limit")
    results = p.execute()

    def _i(v, default=0):
        try:
            if v is None:
                return default
            return int(v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v)
        except Exception:
            return default

    last_seen      = _i(results[0])
    received       = _i(results[1])
    sent           = _i(results[2])
    errors         = _i(results[3])
    cycle_received = _i(results[4])
    cycle_index    = _i(results[5])
    limit          = _i(results[6], 100)
    if limit < 1:
        limit = 100

    online = bool(last_seen and (_now() - last_seen) <= 600)
    pct = int(min(100, (cycle_received * 100) / max(1, limit)))

    cycle_state = "neutral"
    if cycle_received >= limit:
        cycle_state = "good"
    elif cycle_received >= int(limit * 0.9):
        cycle_state = "warn"

    return {
        "device_id":     device_id,
        "online":        online,
        "received":      received,
        "sent":          sent,
        "errors":        errors,
        "cycle_received": cycle_received,
        "cycle_limit":   limit,
        "cycle_index":   cycle_index,
        "cycle_state":   cycle_state,
        "cycle_pct":     pct,
    }
