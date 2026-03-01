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
API_KEY         # Gateway API key (requis — le webhook crashe si absent)
SERVER          # Gateway server URL
ADMIN_PASSWORD  # Admin panel password
APP_SECRET_KEY  # Flask session secret key
REDIS_URL       # Redis connection URL (optionnel, défaut : redis://localhost:6379)
DEBUG_MODE      # "true" pour désactiver la vérification HMAC du webhook (dev uniquement)
```

**Start the web server:**
```bash
gunicorn app:app
# ou en dev :
flask run
```

**Start the Celery worker (requis pour le traitement des SMS entrants) :**
```bash
RUNNING_AS_WORKER=1 celery -A celery_worker worker --loglevel=info
```

`RUNNING_AS_WORKER=1` est obligatoire pour démarrer le thread heartbeat qui maintient le statut worker "online" dans le dashboard. Les deux processus sont définis dans le `Procfile` pour Render.

## Architecture

### Data Flow

1. **Outbound campaigns** : Admin uploade CSV/XLSX → contacts stockés dans `nl:queue` Redis → admin déclenche l'envoi → `services/batches.py` dépile les contacts et appelle l'API gateway
2. **Inbound messages** : Le gateway externe POST sur `/sms_auto_reply` → vérification HMAC-SHA256 → tâche Celery mise en queue → `tasks.py` gère l'état de conversation → auto-reply envoyé via gateway

### Key Modules

- **`app.py`** — Toutes les routes Flask. Panel admin sur `/admin/*` (protégé par login), webhook sur `/sms_auto_reply`, logs sur `/logs` (protégé par login).
- **`tasks.py`** — Tâche Celery `process_message()` : idempotence vérifiée **avant** les stats de réception (évite le double-comptage sur retry), gestion du flux de conversation en 2 étapes.
- **`celery_worker.py`** — Config Celery + thread heartbeat (toutes les 30s) si `RUNNING_AS_WORKER=1`. Fallback `redis://localhost:6379` si `REDIS_URL` est absent.
- **`services/state.py`** — Seul owner des clés Redis de stats. Toujours passer par `state.device_incr_sent()`, `state.device_reset_field()`, etc. — ne jamais écrire directement dans Redis depuis `app.py` ou `batches.py`.
- **`services/gateway.py`** — Récupère la liste des devices et envoie les messages via l'API gateway (3 tentatives avec backoff). Cache mémoire 30s sur `fetch_gateway_devices()`.
- **`services/batches.py`** — Dépile les contacts, applique le template via `render_message()` (fonction partagée, importée dans `app.py`), appelle gateway, rollback en cas d'échec. Clés `batch:*` avec TTL 24h.
- **`services/numlist.py`** — Parse CSV/XLSX, détecte automatiquement la colonne de numéros, stocke dans `nl:queue`.
- **`services/autoreply.py`** — Source unique de vérité pour la config auto-reply (JSON dans `config:autoreply`).
- **`logger.py`** — Écrit dans `/tmp/log.txt` et conserve les 800 dernières lignes dans Redis (`logs:lines`).

### Règles d'architecture importantes

- **`render_message()`** est défini dans `services/batches.py` et importé dans `app.py` — ne pas redupliquer.
- **`services/state.py`** est le seul fichier autorisé à lire/écrire les clés `stats:device:*` et `cycle:device:*`.
- **`device_snapshot()`** utilise un pipeline Redis (1 aller-retour pour 7 valeurs) — ne pas revenir à des appels séquentiels.

### Redis Key Naming Conventions

| Pattern | Purpose |
|---|---|
| `nl:queue` | File de contacts (Redis list, rpush/rpop) |
| `nl:meta` | Métadonnées import (total, variables, colonne) |
| `nl:draft` | Brouillon du message campagne |
| `conv:{number}` | État de conversation par numéro |
| `stats:global:sent` | Compteur global envoyés |
| `stats:device:{id}:*` | Stats par device (sent, received, errors, last_seen) |
| `cycle:device:{id}:*` | Suivi de cycle par device (received, sent, index) |
| `config:*` | Valeurs de configuration |
| `logs:lines` | 800 dernières lignes de log |
| `processed:{number}:{msg_id}` | Clés d'idempotence (TTL 3 jours) |
| `archived_numbers` | Numéros avec conversation terminée |
| `stats:worker:last_seen` | Timestamp heartbeat du worker Celery |
| `batch:{id}:*` | Métadonnées de batch (TTL 24h) |

### Authentication & Security

- Toutes les routes `/admin/*` et `/logs` sont protégées par `_require_login()`.
- Le webhook `/sms_auto_reply` vérifie une signature HMAC-SHA256 avec `API_KEY`. Si `API_KEY` est `None`, le webhook retourne 500 — il faut absolument définir cette variable.
- `DEBUG_MODE=true` désactive la vérification HMAC (dev local uniquement).

### Redis Connection

`services/redis_store.py` se connecte à `localhost:6379` par défaut ou utilise `REDIS_URL`. Les URLs Upstash `rediss://` sont gérées avec SSL automatiquement. `celery_worker.py` applique la même logique de fallback.
