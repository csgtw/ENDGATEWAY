import json
import random
import re
import time
import uuid

from services.redis_store import redis_conn
from services.numlist import NL_QUEUE_KEY, load_message_draft, nl_remaining_count, pop_contact_from_lists
from services.gateway import gateway_send_message
from services import state, msgtpl, links
from services.blacklist import is_blacklisted, record_failure, record_success
from logger import log

BATCH_KEY_TTL = 24 * 3600  # 24h — clés batch expirent automatiquement

SEND_SPEED_KEY = "config:send_speed"


def _now() -> int:
    return int(time.time())


def _base_device_id(device_id: str) -> str:
    """Extrait l'ID de base sans slot SIM (ex: '1|0' → '1', '2' → '2')."""
    return str(device_id).split("|")[0]


def render_message(template: str, contact: dict) -> str:
    """Remplace les variables %clé% par les valeurs du contact (%number% inclus).
    %rand8% → nombre aléatoire à 8 chiffres (différent à chaque occurrence).
    Les variables non présentes dans le contact sont silencieusement retirées
    (compatibilité multi-listes avec schémas hétérogènes)."""
    out = template or ""
    for k, v in (contact or {}).items():
        out = out.replace("%" + str(k) + "%", str(v) if v is not None else "")
    out = re.sub(r"%rand8%", lambda _: str(random.randint(10000000, 99999999)), out)
    # Retire toute variable %xxx% non résolue (contact issu d'une liste sans cette colonne)
    out = re.sub(r"%[A-Za-z_]\w*%", "", out)
    return out


def _parse_speed_delay(speed_str: str) -> tuple:
    """
    Parse la valeur de vitesse d'envoi.
    '0'   → (0, 0)
    '1'   → (1.0, 1.0)
    '1-2' → (1.0, 2.0)
    Retourne (min_sec, max_sec).
    """
    s = (speed_str or "0").strip()
    if "-" in s:
        parts = s.split("-", 1)
        try:
            return float(parts[0]), float(parts[1])
        except Exception:
            pass
    try:
        v = float(s)
        return v, v
    except Exception:
        return 0.0, 0.0


def get_send_speed() -> str:
    """Retourne la vitesse d'envoi configurée (ex: '0', '1', '1-2')."""
    try:
        v = redis_conn.get(SEND_SPEED_KEY)
        return v.decode("utf-8").strip() if v else "0"
    except Exception:
        return "0"


def save_send_speed(speed_str: str):
    """Sauvegarde la vitesse d'envoi en Redis."""
    redis_conn.set(SEND_SPEED_KEY, (speed_str or "0").strip())


# ─── Lecture des batches ──────────────────────────────────────────────────────

def get_batch_status(batch_id: str) -> object:
    """Lit le statut d'un batch depuis Redis."""
    key = f"batch:{batch_id}:meta"
    meta = redis_conn.hgetall(key)
    if not meta:
        return None
    return {
        (k.decode("utf-8") if isinstance(k, bytes) else k):
        (v.decode("utf-8") if isinstance(v, bytes) else v)
        for k, v in meta.items()
    }


def get_device_batch_progress() -> dict:
    """
    Retourne {device_id: {sent, planned, pct, status}} pour les batches actifs (running/queued).
    Utilisé pour afficher la barre de progression par appareil.
    """
    try:
        keys = list(redis_conn.scan_iter(match="batch:*:meta", count=200))
        if not keys:
            return {}
        pipe = redis_conn.pipeline()
        for key in keys:
            pipe.hmget(key, ["status", "device_ids", "sent", "planned", "created_ts"])
        results = pipe.execute()
        out = {}
        for vals in results:
            if not vals or not vals[0]:
                continue
            status_raw = vals[0]
            status = (status_raw.decode() if isinstance(status_raw, bytes) else status_raw) or ""
            if status not in ("running", "queued"):
                continue
            device_ids_raw = vals[1]
            if not device_ids_raw:
                continue
            try:
                ids_str = device_ids_raw.decode() if isinstance(device_ids_raw, bytes) else device_ids_raw
                device_ids = json.loads(ids_str)
                if not device_ids:
                    continue
                did = str(device_ids[0])
            except Exception:
                continue
            sent    = int((vals[2].decode() if isinstance(vals[2], bytes) else vals[2]) or 0)
            planned = int((vals[3].decode() if isinstance(vals[3], bytes) else vals[3]) or 0)
            ts      = int((vals[4].decode() if isinstance(vals[4], bytes) else vals[4]) or 0)
            pct     = min(100, round((sent * 100) / planned)) if planned > 0 else 0
            # Garder le batch le plus récent ou le running en priorité
            existing = out.get(did)
            if not existing or (status == "running" and existing["status"] == "queued") or ts > existing.get("ts", 0):
                out[did] = {"sent": sent, "planned": planned, "pct": pct, "status": status, "ts": ts}
        # Retirer le champ interne "ts"
        return {k: {kk: vv for kk, vv in v.items() if kk != "ts"} for k, v in out.items()}
    except Exception:
        return {}


def get_recent_batches(limit: int = 15) -> list:
    """Retourne les derniers batches triés par date décroissante (pipeline Redis — 1 round-trip)."""
    try:
        keys = list(redis_conn.scan_iter(match="batch:*:meta", count=200))
        if not keys:
            return []
        pipe = redis_conn.pipeline()
        for key in keys:
            pipe.hgetall(key)
        results = pipe.execute()
        batches = []
        for meta in results:
            if not meta:
                continue
            d = {
                (k.decode("utf-8") if isinstance(k, bytes) else k):
                (v.decode("utf-8") if isinstance(v, bytes) else v)
                for k, v in meta.items()
            }
            if "batch_id" in d:
                batches.append(d)
        batches.sort(key=lambda x: int(x.get("created_ts") or 0), reverse=True)
        return batches[:limit]
    except Exception:
        return []


# ─── Envoi ───────────────────────────────────────────────────────────────────

def create_batch(device_ids, per_device: int, batch_id: str = None, template_ids: list = None):
    """
    Dépile les contacts, applique le template, envoie via le gateway.

    Multi-SIM : device_id peut être '1|0' (device 1, SIM slot 0).
    Stats Redis trackées par device de base (_base_device_id), pas par SIM.

    Blacklist : saute les numéros blacklistés.

    Pause : si batch:{batch_id}:paused est défini, stoppe proprement (status="paused").
    Annulation : si batch:{batch_id}:cancelled est défini, stoppe (status="cancelled").

    Vitesse : config:send_speed définit le délai entre chaque envoi.
    Templates : si template_ids fournis, chaque contact reçoit un template aléatoire.
    """
    device_ids = [str(x) for x in (device_ids or []) if str(x).strip()]
    if not device_ids:
        raise ValueError("Aucun appareil")
    if per_device <= 0:
        raise ValueError("per_device invalide")

    remaining = nl_remaining_count()
    if remaining <= 0:
        raise ValueError("Numlist vide")

    # Pool de messages campagne : utilise le bloc actif si défini, sinon tpl:campaign
    from services import camps as _camps
    _active_camp = _camps.get_active()
    if _active_camp:
        templates = _camps.get_messages(_active_camp)
        if not templates:
            templates = msgtpl.get_all("campaign")
    else:
        templates = msgtpl.get_all("campaign")
    if not templates:
        raise ValueError("Aucun message de campagne configuré")

    # Type SMS/MMS depuis le draft (on ne charge que le type)
    _, msg_type = load_message_draft()

    # Lien : rotation gérée par services.links (pool multi-liens, fallback legacy
    # config:global_link si le pool est vide). Le lien est choisi PAR message ci-dessous.

    # Vitesse d'envoi
    speed_str = get_send_speed()
    delay_min, delay_max = _parse_speed_delay(speed_str)

    # Toggles image/audio lus une seule fois — persistants pour toute la campagne
    _img_toggle_on   = (redis_conn.get("config:img_toggle:campaign")   or b"0").decode() == "1"
    _audio_toggle_on = (redis_conn.get("config:audio_toggle:campaign") or b"0").decode() == "1"
    _img_url_global = ""
    if _img_toggle_on:
        _img_g = redis_conn.get("config:img:url")
        if _img_g:
            _img_url_global = _img_g.decode()
    _audio_url_global = ""
    if _audio_toggle_on:
        _aud = redis_conn.get("config:audio:url")
        if _aud:
            _audio_url_global = _aud.decode()

    batch_id   = batch_id or str(uuid.uuid4())[:8]
    meta_key   = f"batch:{batch_id}:meta"
    sent_key   = f"batch:{batch_id}:sent"
    failed_key = f"batch:{batch_id}:failed"
    pause_key  = f"batch:{batch_id}:paused"
    cancel_key = f"batch:{batch_id}:cancelled"

    total_planned = per_device * len(device_ids)

    # Initialise / reprend le meta
    p = redis_conn.pipeline()
    p.hset(meta_key, mapping={
        "batch_id":     batch_id,
        "created_ts":   str(_now()),
        "per_device":   str(per_device),
        "device_count": str(len(device_ids)),
        "type":         msg_type,
        "planned":      str(total_planned),
        "sent":         "0",
        "failed":       "0",
        "status":       "running",
    })
    p.expire(meta_key, BATCH_KEY_TTL)
    p.execute()

    sent   = 0
    failed = 0
    _gw_check_data = []  # [{gw_id, device, number}] — pour vérif post-envoi

    for did in device_ids:
        base_did = _base_device_id(did)

        for _ in range(per_device):
            # ── Vérifications pause / annulation ──────────────────────────
            if redis_conn.exists(cancel_key):
                redis_conn.hset(meta_key, mapping={
                    "sent": str(sent), "failed": str(failed), "status": "cancelled"
                })
                log(f"🚫 Batch {batch_id} annulé | sent={sent} failed={failed}")
                return {"batch_id": batch_id, "sent": sent, "failed": failed, "status": "cancelled"}

            if redis_conn.exists(pause_key):
                redis_conn.hset(meta_key, mapping={
                    "sent": str(sent), "failed": str(failed), "status": "paused"
                })
                log(f"⏸ Batch {batch_id} en pause | sent={sent} failed={failed}")
                return {"batch_id": batch_id, "sent": sent, "failed": failed, "status": "paused"}

            # ── Délai vitesse d'envoi ──────────────────────────────────────
            if delay_max > 0:
                sleep_time = random.uniform(delay_min, delay_max) if delay_max != delay_min else delay_min
                time.sleep(sleep_time)

            # ── Dépilage contact ──────────────────────────────────────────
            raw, src_key = pop_contact_from_lists()
            if not raw:
                break

            try:
                contact = json.loads(raw.decode("utf-8"))
            except Exception:
                failed += 1
                # JSON corrompu : log les données brutes pour audit, ne peut pas être ré-enqueued
                try:
                    redis_conn.lpush(failed_key, json.dumps({
                        "device": did, "error": "bad_json",
                        "raw": raw.decode("utf-8", errors="replace")[:200]
                    }))
                except Exception:
                    pass
                continue

            number = (contact.get("number") or "").strip()
            if not number:
                failed += 1
                redis_conn.lpush(failed_key, json.dumps({"device": did, "error": "no_number"}))
                continue

            if is_blacklisted(number):
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": "blacklisted"}, ensure_ascii=False)
                )
                continue

            # Lien à rotation (peek : ne commit qu'à l'envoi réussi, plus bas)
            _link = links.peek_link()
            if _link:
                contact["link"] = _link

            # Inject %image% URL if generated for this list
            if src_key and src_key.startswith("nl:list:"):
                _lid = src_key[len("nl:list:"):]
                _img_raw = redis_conn.get(f"nl:img:{_lid}:{number}")
                if _img_raw:
                    contact["image"] = _img_raw.decode("utf-8") if isinstance(_img_raw, bytes) else _img_raw

            msg_template = random.choice(templates)
            msg = render_message(msg_template, contact).strip()

            # Image / audio : utiliser les valeurs figées au lancement du batch
            # Priorité : image per-contact (imggen) > image template globale
            _img_url   = (contact.get("image", "") or _img_url_global) if _img_toggle_on else ""
            _audio_url = _audio_url_global

            if not msg and not _img_url and not _audio_url:
                redis_conn.lpush(src_key or NL_QUEUE_KEY, json.dumps(contact, ensure_ascii=False))
                failed += 1
                redis_conn.lpush(
                    failed_key,
                    json.dumps({"device": did, "number": number, "error": "empty_message"}, ensure_ascii=False)
                )
                continue

            ok, detail = gateway_send_message(
                number=number, message=msg, device_id=did, msg_type=msg_type,
                media_url=_img_url, audio_url=_audio_url,
            )

            if ok:
                sent += 1
                state.device_incr_sent(base_did, 1)
                record_success(number)  # reset compteur d'échecs → échecs comptés consécutifs
                links.commit_link(_link)  # avance la rotation + compteur du lien (envoi réussi)
                # Stocker les vars du contact pour interpolation auto-reply (TTL 7j)
                try:
                    vars_data = {str(k): str(v) for k, v in contact.items()
                                 if k != "number" and v is not None and str(v).strip()}
                    if vars_data:
                        redis_conn.hset(f"conv:{number}:vars", mapping=vars_data)
                        redis_conn.expire(f"conv:{number}:vars", 7 * 24 * 3600)
                except Exception:
                    pass
                redis_conn.lpush(
                    sent_key,
                    json.dumps({"device": did, "number": number, "ts": _now(), "gw_id": detail or ""}, ensure_ascii=False)
                )
                if detail:  # detail = gw_id quand ok=True
                    _gw_check_data.append({"gw_id": detail, "device": did, "number": number})
            else:
                redis_conn.lpush(src_key or NL_QUEUE_KEY, json.dumps(contact, ensure_ascii=False))
                failed += 1
                auto_bl = record_failure(number)
                state.device_incr_errors(base_did, 1)
                redis_conn.lpush(
                    failed_key,
                    json.dumps({
                        "device":           did,
                        "number":           number,
                        "error":            detail or "send_failed",
                        "auto_blacklisted": auto_bl,
                    }, ensure_ascii=False)
                )

            redis_conn.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed)})

    # Finalisation
    p = redis_conn.pipeline()
    p.hset(meta_key, mapping={"sent": str(sent), "failed": str(failed), "status": "done"})
    p.expire(sent_key,   BATCH_KEY_TTL)
    p.expire(failed_key, BATCH_KEY_TTL)
    p.execute()

    # Planifier la vérification des livraisons réelles (10 min après la fin du batch)
    if _gw_check_data:
        try:
            check_key = f"batch:{batch_id}:gw_check"
            redis_conn.set(check_key, json.dumps(_gw_check_data), ex=BATCH_KEY_TTL)
            from celery_worker import celery as _celery
            _celery.send_task("check_gateway_delivery", args=[batch_id], countdown=600)
            log(f"📬 check_gateway_delivery planifié dans 10 min pour batch {batch_id} ({len(_gw_check_data)} msgs)")
        except Exception as exc:
            log(f"⚠️ check_gateway_delivery schedule failed: {exc}")

    log(f"📦 Batch {batch_id} terminé | sent={sent} failed={failed} remaining={nl_remaining_count()}")
    return {"batch_id": batch_id, "sent": sent, "failed": failed}
