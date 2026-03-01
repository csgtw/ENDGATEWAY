import os
import time
from services.app_config import LOG_FILE
from services.redis_store import redis_conn

MAX_REDIS_LINES = 800

def log(message: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {message}"

    # 1) fichier (best effort)
    try:
        path = LOG_FILE or "/tmp/app.log"
        folder = os.path.dirname(path)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

    # 2) Redis (source de vérité pour /logs)
    try:
        redis_conn.lpush("logs:lines", line)
        redis_conn.ltrim("logs:lines", 0, MAX_REDIS_LINES - 1)
    except Exception:
        pass
