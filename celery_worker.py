import os
import time
import threading
from celery import Celery
from redis import Redis

REDIS_URL = (os.getenv("REDIS_URL") or "").strip()
_DEFAULT_REDIS = "redis://localhost:6379"


def _celery_url(url: str) -> str:
    if not url:
        return _DEFAULT_REDIS
    if url.startswith("rediss://") and "ssl_cert_reqs=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}ssl_cert_reqs=CERT_NONE"
    return url


BROKER = _celery_url(REDIS_URL)
BACKEND = _celery_url(REDIS_URL)

celery = Celery("sms_app", broker=BROKER, backend=BACKEND)

celery.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    include=["tasks"],  # obligatoire : enregistre send_campaign et process_message dans le worker
)


def _redis_client():
    url = REDIS_URL or _DEFAULT_REDIS
    if url.startswith("rediss://"):
        return Redis.from_url(url, ssl_cert_reqs=None)
    return Redis.from_url(url)


def _heartbeat_loop():
    r = _redis_client()
    while True:
        try:
            r.set("stats:worker:last_seen", int(time.time()))
        except Exception:
            pass
        time.sleep(30)


if os.getenv("RUNNING_AS_WORKER") == "1":
    t = threading.Thread(target=_heartbeat_loop, daemon=True)
    t.start()
