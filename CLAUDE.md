# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ENDGATEWAY is a Flask-based SMS campaign and auto-reply management panel. It integrates with an external SMS gateway API, manages contact lists, and processes inbound messages via Celery workers. All persistent state is stored in Redis.

## Running the Application

**Install dependencies:**
```bash
pip install -r requirements.txt
```

**Required environment variables:**
```
API_KEY         # Gateway API key
SERVER          # Gateway server URL
ADMIN_PASSWORD  # Admin panel password
APP_SECRET_KEY  # Flask session secret key
REDIS_URL       # Redis connection URL (optional, defaults to localhost:6379)
DEBUG_MODE      # Set to "true" to enable debug logging (optional)
```

**Start the web server:**
```bash
gunicorn app:app
# or for development:
flask run
```

**Start the Celery worker (required for inbound SMS processing):**
```bash
celery -A celery_worker worker --loglevel=info
```

Both processes must be running for full functionality. The `Procfile` defines both for Heroku/Render deployments.

## Architecture

### Data Flow

1. **Outbound campaigns**: Admin uploads CSV/XLSX contact list → contacts stored in Redis queue → admin triggers batch send → `services/batches.py` pops contacts and calls gateway API
2. **Inbound messages**: External gateway POSTs to `/sms_auto_reply` → HMAC-SHA256 signature verified → message queued as Celery task → `tasks.py` processes conversation state → auto-reply sent via gateway

### Key Modules

- **`app.py`** — All Flask routes. Admin panel at `/admin/*`, webhook at `/sms_auto_reply`, logs at `/logs`.
- **`tasks.py`** — Celery task `process_message()`: manages two-step conversation flow, idempotency (via `processed:{number}:{msg_id}` Redis keys), and per-device stats.
- **`celery_worker.py`** — Celery app config + a heartbeat loop that updates `worker:heartbeat` in Redis every 30 seconds.
- **`services/state.py`** — All Redis state operations: global/device sent counters, device cycle tracking, worker heartbeat reads, locking.
- **`services/gateway.py`** — Fetches device list and sends messages via external gateway API with 3-attempt retry/backoff.
- **`services/batches.py`** — Pops contacts from the Redis queue, renders message templates (variable interpolation), calls gateway, rolls back failures.
- **`services/numlist.py`** — Parses uploaded CSV/XLSX files; auto-detects the phone number column; stores contacts in `nl:queue` Redis list with metadata in `nl:meta`.
- **`services/autoreply.py`** — Reads/writes auto-reply configuration (stored as JSON in Redis at `config:autoreply`).
- **`logger.py`** — Appends to `/tmp/log.txt` and maintains the last 800 log lines in Redis (`logs:lines`).

### Redis Key Naming Conventions

| Pattern | Purpose |
|---|---|
| `nl:queue` | Contact list queue (Redis list) |
| `nl:meta` | Import metadata (total, variables, column) |
| `nl:draft` | Message draft |
| `conv:{number}` | Per-number conversation state |
| `stats:global:sent` | Global outbound counter |
| `stats:device:{id}:*` | Per-device stats (sent, received, errors, last_seen) |
| `cycle:device:{id}:*` | Per-device cycle tracking |
| `config:*` | Configuration values |
| `logs:lines` | Last 800 log lines |
| `processed:{number}:{msg_id}` | Idempotency keys for inbound messages |
| `archived_numbers` | Numbers with completed conversations |
| `worker:heartbeat` | Celery worker liveness timestamp |

### Authentication & Security

- Admin routes are protected by session-based login (`ADMIN_PASSWORD`).
- The `/sms_auto_reply` webhook verifies HMAC-SHA256 signatures using `API_KEY`.
- Gateway API calls use `API_KEY` in request headers.

### Redis Connection

`services/redis_store.py` connects to `localhost:6379` by default or uses `REDIS_URL`. Upstash `rediss://` URLs are handled with SSL configuration automatically.
