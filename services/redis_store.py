import os
from redis import Redis

REDIS_URL = (os.getenv("REDIS_URL") or "").strip()

if not REDIS_URL:
    redis_conn = Redis(host="localhost", port=6379, db=0)
else:
    # Upstash rediss -> ssl_cert_reqs=None pour redis-py
    if REDIS_URL.startswith("rediss://"):
        redis_conn = Redis.from_url(REDIS_URL, ssl_cert_reqs=None)
    else:
        redis_conn = Redis.from_url(REDIS_URL)
