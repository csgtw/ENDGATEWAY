import os
import json
import time
import hmac
import hashlib
import base64
import uuid
import random
from datetime import datetime, timezone

from flask import (
    Flask, request, Response, redirect, url_for,
    session, render_template, jsonify
)

from logger import log
from tasks import process_message, send_campaign, _check_cycle_auto_restart, prepare_nl_images

from services.app_config import (
    API_KEY, DEBUG_MODE, LOG_FILE,
    ADMIN_PASSWORD, APP_SECRET_KEY
)
from services.redis_store import redis_conn
import services.state as state

from services.gateway import fetch_gateway_devices
from services.numlist import (
    load_nl_meta, nl_remaining_count,
    clear_numlist, import_files,
    load_message_draft, save_message_draft,
    NL_QUEUE_KEY, NL_LISTS_KEY,
    get_named_lists, delete_named_list,
    peek_contacts_from_lists,
    get_list_contacts, delete_contact_from_list,
    get_selected_lists, set_selected_lists,
)
from services.batches import (
    create_batch, render_message, get_batch_status, get_recent_batches,
    save_send_speed, get_send_speed,
)
from services.blacklist import (
    get_blacklist, blacklist_count, clear_blacklist, remove_from_blacklist
)

# Source unique de vérité pour autoreply
from services.autoreply import load_autoreply_config, save_autoreply_config
from services import msgtpl
from services import camps as _camps
from services import arcamps as _arcamps


app = Flask(__name__)
app.jinja_env.auto_reload = True
if not APP_SECRET_KEY:
    import sys
    print("⚠️  APP_SECRET_KEY non définie — les sessions seront invalidées à chaque redémarrage", file=sys.stderr)
app.secret_key = APP_SECRET_KEY or os.urandom(32)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now() -> int:
    return int(time.time())


def _is_logged_in() -> bool:
    return session.get("admin_logged_in") is True


def _require_login():
    if not _is_logged_in():
        return redirect(url_for("admin_login"))
    return None


def _redis_ok() -> bool:
    try:
        redis_conn.ping()
        return True
    except Exception:
        return False


_RATE_LIMIT_MAX    = 10   # tentatives max
_RATE_LIMIT_WINDOW = 300  # fenêtre de 5 minutes


def _check_login_rate_limit() -> bool:
    ip = (request.remote_addr or "unknown").strip()
    try:
        return int(redis_conn.get(f"login:attempts:{ip}") or 0) >= _RATE_LIMIT_MAX
    except Exception:
        return False


def _record_login_failure():
    ip = (request.remote_addr or "unknown").strip()
    try:
        key = f"login:attempts:{ip}"
        p = redis_conn.pipeline()
        p.incr(key)
        p.expire(key, _RATE_LIMIT_WINDOW)
        p.execute()
    except Exception:
        pass


def _login_reset():
    ip = (request.remote_addr or "unknown").strip()
    try:
        redis_conn.delete(f"login:attempts:{ip}")
    except Exception:
        pass


def _wants_json() -> bool:
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    acc = (request.headers.get("Accept") or "").lower()
    return "application/json" in acc


def _peek_contacts(count: int) -> list:
    """Retourne les `count` prochains contacts sans les dépiler (toutes les listes)."""
    return peek_contacts_from_lists(count)


def _build_page_context() -> dict:
    """Contexte partagé entre /admin/settings et /admin/state."""
    _camps.ensure_default()
    _arcamps.ensure_default()
    nl_meta = load_nl_meta()
    remaining = nl_remaining_count()
    total_sent = state.global_sent_get()
    cycle_limit = state.cycle_limit_get()
    nl_message, nl_type = load_message_draft()
    vars_list = list((nl_meta or {}).get("variables") or [])
    ar_cfg = load_autoreply_config()
    redis_ok = _redis_ok()
    worker_ok = state.worker_ok()

    # Migration auto : si pool vide, importer depuis anciens champs Redis
    if redis_ok:
        if msgtpl.count("campaign") == 0:
            existing_msg, _ = load_message_draft()
            if (existing_msg or "").strip():
                msgtpl.add("campaign", existing_msg.strip())
        if msgtpl.count("ar:step0") == 0 and (ar_cfg.get("step0_text") or "").strip():
            msgtpl.add("ar:step0", ar_cfg["step0_text"].strip())
        if msgtpl.count("ar:step1") == 0 and (ar_cfg.get("step1_text") or "").strip():
            msgtpl.add("ar:step1", ar_cfg["step1_text"].strip())

    _active0 = _arcamps.get_active_step(0) if redis_ok else None
    _pool_cnt0 = (_arcamps.count_messages(_active0, 0) if _active0 else 0) or msgtpl.count("ar:step0")
    autoreply_ok = (
        bool(ar_cfg.get("enabled"))
        and redis_ok
        and worker_ok
        and _pool_cnt0 > 0
    )

    gw_devices = fetch_gateway_devices()
    rows = []
    for d in (gw_devices or []):
        did = str(d.get("id"))
        s = state.device_snapshot(did)
        s["name"] = d.get("name") or ""
        s["model"] = d.get("model") or ""
        # lastSeenAt du gateway est plus fiable que notre Redis last_seen
        # (reflète la vraie connexion device→gateway, pas juste les SMS entrants)
        last_seen_at = d.get("lastSeenAt") or ""
        if last_seen_at:
            try:
                ts_str = last_seen_at.replace("+0000", "+00:00").replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                s["online"] = (datetime.now(timezone.utc) - dt).total_seconds() < 600
            except Exception:
                pass
        # sims du gateway = dict {slot_str: "SIM #N [carrier]"} — on transforme en liste pour le template JS
        sims_raw = d.get("sims") or {}
        if isinstance(sims_raw, dict):
            s["sims"] = [{"slot": int(k), "name": v, "enabled": True} for k, v in sims_raw.items()]
        else:
            s["sims"] = []
        # Numéro de téléphone du device (best-effort sur plusieurs noms de champs)
        s["phone_number"] = (
            d.get("phoneNumber") or d.get("phone") or d.get("number") or
            d.get("phone_number") or ""
        ).strip()
        rows.append(s)

    rows.sort(
        key=lambda x: (int(x.get("sent") or 0), int(x.get("received") or 0)),
        reverse=True
    )

    send_speed = get_send_speed()
    reply_cd_min, reply_cd_max = state.reply_countdown_get()
    reply_countdown = f"{reply_cd_min}-{reply_cd_max}" if reply_cd_min != reply_cd_max else str(reply_cd_min)
    global_link = state.global_link_get()

    return {
        "rows": rows,
        "remaining": remaining,
        "total_sent": total_sent,
        "cycle_limit": cycle_limit,
        "nl_meta": nl_meta,
        "nl_message": nl_message,
        "nl_type": nl_type,
        "vars_list": vars_list,
        "ar_cfg": ar_cfg,
        "autoreply_ok": autoreply_ok,
        "redis_ok": redis_ok,
        "worker_ok": worker_ok,
        "worker_last_seen": state.get_int("stats:worker:last_seen", 0),
        "send_speed": send_speed,
        "reply_countdown": reply_countdown,
        "global_link": global_link,
        "tpl_counts": {
            "campaign": msgtpl.count("campaign"),
            "ar:step0":  msgtpl.count("ar:step0"),
            "ar:step1":  msgtpl.count("ar:step1"),
        },
        "named_lists": get_named_lists(),
        "selected_lists": get_selected_lists(),  # None = all
        "camps_list": _camps.list_camps(),
        "active_camp": _camps.get_active(),
        "active_arcamp_step0": _arcamps.get_active_step(0),
        "active_arcamp_step1": _arcamps.get_active_step(1),
        "ts": _now(),
    }


# ─── Auth ─────────────────────────────────────────────────────────────────────

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        if _check_login_rate_limit():
            return Response("Trop de tentatives. Réessaye dans 5 minutes.", status=429, mimetype="text/plain")
        pwd = (request.form.get("password") or "").strip()
        if ADMIN_PASSWORD and pwd == ADMIN_PASSWORD:
            _login_reset()
            session["admin_logged_in"] = True
            return redirect(url_for("admin_settings"))
        _record_login_failure()
        return Response("Mot de passe incorrect", status=401, mimetype="text/plain")
    return render_template("login.html")


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))


@app.route("/admin")
def admin_home():
    guard = _require_login()
    if guard:
        return guard
    return redirect(url_for("admin_settings"))


# ─── Stats ────────────────────────────────────────────────────────────────────

@app.route("/admin/stats/reset_sent", methods=["POST"])
def admin_reset_sent():
    guard = _require_login()
    if guard:
        return guard

    state.reset_global_sent_and_devices()

    if _wants_json():
        return jsonify({"ok": True, "msg": "Compteur envoyé reset"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/device/stats/reset", methods=["POST"])
def admin_device_stats_reset():
    guard = _require_login()
    if guard:
        return guard

    device_id = (request.form.get("device_id") or "").strip()
    field = (request.form.get("field") or "").strip().lower()

    if not device_id:
        return jsonify({"ok": False, "msg": "device_id manquant"}), 400

    try:
        state.device_reset_field(device_id, field)
        return jsonify({"ok": True, "msg": f"{field} reset"}), 200
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─── Cycle ────────────────────────────────────────────────────────────────────

@app.route("/admin/cycle_limit/save", methods=["POST"])
def admin_cycle_limit_save():
    guard = _require_login()
    if guard:
        return guard

    try:
        limit_val = int(request.form.get("cycle_limit") or 0)
    except Exception:
        limit_val = 0

    if limit_val < 1:
        if _wants_json():
            return jsonify({"ok": False, "msg": "Limite invalide"}), 400
        return Response("Limite invalide", status=400)

    state.cycle_limit_set(limit_val)

    if _wants_json():
        return jsonify({"ok": True, "msg": f"Limite cycle = {limit_val}"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/cycles/reset_all", methods=["POST"])
def admin_cycles_reset_all():
    guard = _require_login()
    if guard:
        return guard

    gw_devices = fetch_gateway_devices()
    device_ids = [str(d.get("id")) for d in (gw_devices or []) if d.get("id") is not None]
    state.full_reset_all(device_ids)

    if _wants_json():
        return jsonify({"ok": True, "msg": "Compteurs réinitialisés"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/device/relancer", methods=["POST"])
def admin_device_relancer():
    guard = _require_login()
    if guard:
        return guard

    device_id = (request.form.get("device_id") or "").strip()
    if not device_id:
        if _wants_json():
            return jsonify({"ok": False, "msg": "device_id manquant"}), 400
        return Response("device_id manquant", status=400)

    state.device_cycle_relancer(device_id)

    # Dispatch automatique si derniers paramètres de campagne disponibles
    dispatched = False
    try:
        params     = state.device_last_campaign_get(device_id)
        per_device = params.get("per_device", 0)
        tmpl_ids   = params.get("template_ids") or None
        if per_device > 0 and nl_remaining_count() > 0:
            batch_id = str(uuid.uuid4())[:8]
            redis_conn.hset(f"batch:{batch_id}:meta", mapping={
                "batch_id":     batch_id,
                "created_ts":   str(_now()),
                "planned":      str(per_device),
                "sent":         "0",
                "failed":       "0",
                "status":       "queued",
                "device_ids":   json.dumps([device_id]),
                "template_ids": json.dumps(tmpl_ids or []),
                "per_device":   str(per_device),
                "device_count": "1",
            })
            redis_conn.expire(f"batch:{batch_id}:meta", 24 * 3600)
            send_campaign.delay([device_id], per_device, batch_id, tmpl_ids)
            dispatched = True
            log(f"🚀 Relancer dispatch campagne device={device_id} batch={batch_id}")
    except Exception as exc:
        log(f"⚠️ Relancer dispatch erreur device={device_id}: {exc}")

    suffix = " + campagne re-dispatchée" if dispatched else ""
    if _wants_json():
        return jsonify({"ok": True, "msg": f"Relancé device {device_id}{suffix}", "dispatched": dispatched}), 200
    return redirect(url_for("admin_settings"))


# ─── Images personnalisées ────────────────────────────────────────────────────

@app.route("/admin/nl/template", methods=["POST"])
def admin_nl_template_upload():
    guard = _require_login()
    if guard:
        return guard
    f = request.files.get("template")
    if not f or f.filename == "":
        return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
    try:
        from PIL import Image as _PILImg
        from io import BytesIO
        img = _PILImg.open(f).convert("RGB")
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=95)
        img_bytes = buf.getvalue()
    except ImportError:
        f.seek(0)
        img_bytes = f.read()
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erreur lecture image: {e}"}), 500
    # Stocker dans Redis (partagé web+worker)
    redis_conn.set("nl:template", img_bytes)
    size = len(img_bytes)
    warn = None
    if size > 600 * 1024:
        warn = f"⚠ {round(size/1024)}ko — dépasse la limite MMS 600 KB, risque élevé de non-réception"
    elif size > 300 * 1024:
        warn = f"⚠ {round(size/1024)}ko — compatibilité réduite sur certains opérateurs (recommandé < 300 KB)"
    return jsonify({"ok": True, "msg": "Template enregistré", "size": size, "warn": warn}), 200


@app.route("/admin/nl/template", methods=["GET"])
def admin_nl_template_status():
    guard = _require_login()
    if guard:
        return guard
    raw = redis_conn.get("nl:template")
    exists = raw is not None
    size = len(raw) if raw else 0
    return jsonify({"ok": True, "exists": exists, "size": size}), 200


@app.route("/uploads/<list_id>/<filename>")
def serve_nl_image(list_id, filename):
    """Sert une image générée depuis Redis (accessible sans auth pour le gateway Android)."""
    from flask import Response as _Resp
    h = filename.replace(".jpg", "").replace(".jpeg", "")
    data = redis_conn.get(f"nl:imgdata:{list_id}:{h}")
    if not data:
        return "", 404
    return _Resp(data, mimetype="image/jpeg",
                 headers={"Cache-Control": "public, max-age=604800"})


@app.route("/admin/nl/template", methods=["DELETE"])
def admin_nl_template_delete():
    guard = _require_login()
    if guard:
        return guard
    redis_conn.delete("nl:template")
    return jsonify({"ok": True, "msg": "Template supprimé"}), 200


# ─── Audio vocal upload ───────────────────────────────────────────────────────

ALLOWED_AUDIO_EXT = {".mp3", ".m4a", ".amr", ".wav", ".ogg", ".aac"}

@app.route("/admin/audio/upload", methods=["POST"])
def admin_audio_upload():
    guard = _require_login()
    if guard:
        return guard
    f = request.files.get("audio")
    if not f or f.filename == "":
        return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
    import os as _os
    ext = _os.path.splitext(f.filename or "")[1].lower()
    if ext not in ALLOWED_AUDIO_EXT:
        return jsonify({"ok": False, "msg": f"Format non supporté (accepté : {', '.join(ALLOWED_AUDIO_EXT)})"}), 400
    audio_bytes = f.read()
    filename = f.filename
    base_url = request.url_root.rstrip("/")
    audio_url = f"{base_url}/uploads/audio/{filename}"
    redis_conn.set("config:audio:bytes", audio_bytes)
    redis_conn.set("config:audio:filename", filename)
    redis_conn.set("config:audio:url", audio_url)
    size = len(audio_bytes)
    warn = None
    if size > 600 * 1024:
        warn = f"⚠ {round(size/1024)}ko — dépasse la limite MMS 600 KB, risque élevé de non-réception"
    elif size > 300 * 1024:
        warn = f"⚠ {round(size/1024)}ko — compatibilité réduite sur certains opérateurs (recommandé < 300 KB)"
    return jsonify({"ok": True, "msg": "Vocal enregistré", "size": size, "filename": filename, "warn": warn}), 200


@app.route("/admin/audio/status", methods=["GET"])
def admin_audio_status():
    guard = _require_login()
    if guard:
        return guard
    raw = redis_conn.get("config:audio:bytes")
    fname = redis_conn.get("config:audio:filename")
    exists = raw is not None
    return jsonify({
        "ok": True, "exists": exists,
        "size": len(raw) if raw else 0,
        "filename": fname.decode() if fname else "",
    }), 200


@app.route("/admin/audio", methods=["DELETE"])
def admin_audio_delete():
    guard = _require_login()
    if guard:
        return guard
    redis_conn.delete("config:audio:bytes")
    redis_conn.delete("config:audio:filename")
    redis_conn.delete("config:audio:url")
    return jsonify({"ok": True, "msg": "Vocal supprimé"}), 200


@app.route("/uploads/audio/<filename>")
def serve_audio(filename):
    """Sert le fichier audio depuis Redis (accessible sans auth pour le gateway Android)."""
    from flask import Response as _Resp
    import os as _os
    data = redis_conn.get("config:audio:bytes")
    if not data:
        return "", 404
    ext = _os.path.splitext(filename)[1].lower()
    mime_map = {".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".amr": "audio/amr",
                ".wav": "audio/wav", ".ogg": "audio/ogg", ".aac": "audio/aac"}
    mime = mime_map.get(ext, "audio/mpeg")
    return _Resp(data, mimetype=mime, headers={"Cache-Control": "public, max-age=604800"})


@app.route("/admin/config/audio_toggle", methods=["POST"])
def admin_audio_toggle():
    guard = _require_login()
    if guard:
        return guard
    zone = request.form.get("zone", "")
    enabled = request.form.get("enabled", "0")
    if zone not in ("campaign", "ar:step0", "ar:step1"):
        return jsonify({"ok": False, "msg": "zone invalide"}), 400
    if enabled == "1":
        redis_conn.set(f"config:audio_toggle:{zone}", "1")
    else:
        redis_conn.delete(f"config:audio_toggle:{zone}")
    return jsonify({"ok": True, "enabled": enabled == "1"}), 200


@app.route("/admin/nl/list/<list_id>/prepare-images", methods=["POST"])
def admin_nl_prepare_images(list_id):
    guard = _require_login()
    if guard:
        return guard
    if not redis_conn.exists("nl:template"):
        return jsonify({"ok": False, "msg": "Template manquant — uploader d'abord une image template"}), 400
    if not redis_conn.hexists(NL_LISTS_KEY, list_id):
        return jsonify({"ok": False, "msg": "Liste introuvable"}), 404
    raw_status = redis_conn.hgetall(f"nl:imgstatus:{list_id}")
    if raw_status:
        s = (raw_status.get(b"status") or b"").decode()
        if s == "running":
            return jsonify({"ok": False, "msg": "Génération déjà en cours"}), 429
    img_col = (request.form.get("img_col") or "names").strip() or "names"
    base_url = request.url_root.rstrip("/")
    redis_conn.hset(f"nl:imgstatus:{list_id}", mapping={
        "status": "queued", "total": "0", "done": "0", "failed": "0"
    })
    prepare_nl_images.delay(list_id, base_url, img_col)
    return jsonify({"ok": True, "msg": "Génération lancée"}), 200


@app.route("/admin/nl/list/<list_id>/images-status", methods=["GET"])
def admin_nl_images_status(list_id):
    guard = _require_login()
    if guard:
        return guard
    raw = redis_conn.hgetall(f"nl:imgstatus:{list_id}")
    if not raw:
        return jsonify({"ok": True, "status": "none"}), 200
    status = {
        (k.decode() if isinstance(k, bytes) else k):
        (v.decode() if isinstance(v, bytes) else v)
        for k, v in raw.items()
    }
    return jsonify({"ok": True, **status}), 200


# ─── Numlist ──────────────────────────────────────────────────────────────────

@app.route("/admin/nl/clear", methods=["POST"])
def admin_nl_clear():
    guard = _require_login()
    if guard:
        return guard

    clear_numlist()

    if _wants_json():
        return jsonify({"ok": True, "msg": "Numlist vidée"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/nl/lists", methods=["GET"])
def admin_nl_lists():
    guard = _require_login()
    if guard:
        return guard
    return jsonify({"ok": True, "lists": get_named_lists(), "selected": get_selected_lists()}), 200


@app.route("/admin/nl/list/<list_id>/delete", methods=["POST"])
def admin_nl_list_delete(list_id):
    guard = _require_login()
    if guard:
        return guard
    delete_named_list(list_id)
    remaining = nl_remaining_count()
    lists = get_named_lists()
    return jsonify({"ok": True, "msg": "Liste supprimée", "remaining": remaining, "lists": lists, "selected": get_selected_lists()}), 200


@app.route("/admin/nl/list/<list_id>/contacts", methods=["GET"])
def admin_nl_list_contacts(list_id):
    """Lire les contacts d'une liste spécifique avec pagination."""
    guard = _require_login()
    if guard:
        return guard
    try:
        offset = max(0, int(request.args.get("offset") or 0))
        limit  = max(10, min(int(request.args.get("limit") or 200), 500))
    except Exception:
        offset, limit = 0, 200
    result = get_list_contacts(list_id, offset=offset, limit=limit)
    return jsonify({"ok": True, **result}), 200


@app.route("/admin/nl/list/<list_id>/contact/delete", methods=["POST"])
def admin_nl_list_contact_delete(list_id):
    """Supprime un contact (par numéro) d'une liste spécifique."""
    guard = _require_login()
    if guard:
        return guard
    number = (request.form.get("number") or "").strip()
    if not number:
        return jsonify({"ok": False, "msg": "Numéro manquant"}), 400
    ok = delete_contact_from_list(list_id, number)
    remaining = nl_remaining_count()
    return jsonify({"ok": ok, "msg": "Contact supprimé" if ok else "Contact introuvable", "remaining": remaining}), 200


@app.route("/admin/numlist/select", methods=["POST"])
def admin_numlist_select():
    guard = _require_login()
    if guard: return guard
    list_ids_raw = request.form.getlist("list_ids")
    if not list_ids_raw:
        set_selected_lists(None)  # tout sélectionner
    else:
        set_selected_lists([str(x) for x in list_ids_raw if str(x).strip()])
    return jsonify({"ok": True})


@app.route("/admin/nl/upload", methods=["POST"])
def admin_nl_upload():
    guard = _require_login()
    if guard:
        return guard

    # Lock anti double-import (TTL 300s — suffisant pour 100k+ contacts)
    if not state.set_lock("lock:nl_import", 300):
        return jsonify({"ok": False, "msg": "Import déjà en cours, patiente…"}), 429

    try:
        files = request.files.getlist("files")
        if not files or all(f.filename == "" for f in files):
            if _wants_json():
                return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
            return Response("Fichier manquant", status=400)

        res = import_files(files)
        if _wants_json():
            return jsonify({
                "ok": True,
                "msg": f"Import OK — +{res.get('added', 0)} contacts",
                "added": res.get("added", 0),
                "imported_total": res.get("imported_total", 0),
                "variables": res.get("variables", []),
            }), 200
        return redirect(url_for("admin_settings"))
    except Exception as e:
        if _wants_json():
            return jsonify({"ok": False, "msg": f"Erreur import: {e}"}), 400
        return Response(f"Erreur import: {e}", status=400)
    finally:
        redis_conn.delete("lock:nl_import")


@app.route("/admin/nl/contacts", methods=["GET"])
def admin_nl_contacts():
    guard = _require_login()
    if guard:
        return guard
    try:
        limit = max(10, min(int(request.args.get("limit") or 100), 500))
    except Exception:
        limit = 100

    remaining = nl_remaining_count()
    if remaining <= 0:
        return jsonify({"ok": True, "total": 0, "page": 1, "pages": 1, "contacts": [], "columns": []}), 200

    raw_contacts = peek_contacts_from_lists(limit)
    contacts, columns_seen, columns = [], set(), []
    for c in raw_contacts:
        raw_str = json.dumps(c, ensure_ascii=False)
        c["_raw"] = raw_str
        contacts.append(c)
        for k in c:
            if k != "_raw" and k not in columns_seen:
                columns_seen.add(k)
                columns.append(k)

    return jsonify({"ok": True, "total": remaining, "page": 1,
                    "pages": 1, "limit": limit,
                    "contacts": contacts, "columns": columns}), 200


@app.route("/admin/nl/contact/delete", methods=["POST"])
def admin_nl_contact_delete():
    guard = _require_login()
    if guard:
        return guard
    try:
        raw = (request.form.get("contact_json") or "").strip()
        if not raw:
            return jsonify({"ok": False, "msg": "contact_json manquant"}), 400
        json.loads(raw)  # validate
        # Chercher et supprimer dans toutes les listes nommées + legacy
        removed = 0
        raw_ids = redis_conn.hkeys(NL_LISTS_KEY) or []
        for raw_id in raw_ids:
            lid = raw_id.decode("utf-8") if isinstance(raw_id, bytes) else raw_id
            r = redis_conn.lrem(f"nl:list:{lid}", 1, raw)
            if r:
                removed += r
                break
        if not removed:
            removed = redis_conn.lrem(NL_QUEUE_KEY, 1, raw)
        remaining = nl_remaining_count()
        return jsonify({"ok": bool(removed),
                        "msg": "Contact supprimé" if removed else "Introuvable",
                        "remaining": remaining}), 200
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 400


@app.route("/admin/nl/message", methods=["POST"])
def admin_nl_message():
    guard = _require_login()
    if guard:
        return guard

    msg_type = (request.form.get("nl_type") or "sms").strip().lower()
    if msg_type not in ("sms", "mms"):
        msg_type = "sms"

    save_message_draft("", msg_type)

    if _wants_json():
        return jsonify({"ok": True, "msg": "Type enregistré"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/nl/peek", methods=["GET"])
def admin_nl_peek():
    guard = _require_login()
    if guard:
        return guard

    try:
        count = int(request.args.get("count") or 10)
    except Exception:
        count = 10
    count = max(1, min(count, 50))

    remaining = nl_remaining_count()
    if remaining <= 0:
        return jsonify({"ok": True, "remaining": 0, "preview": []}), 200

    _, msg_type = load_message_draft()
    templates = msgtpl.get_all("campaign")
    contacts = _peek_contacts(count)
    global_link = state.global_link_get() or ""

    preview = []
    for c in contacts:
        number = (c.get("number") or "").strip()
        if not number:
            continue
        if global_link:
            c["link"] = global_link
        msg_template = random.choice(templates) if templates else ""
        msg = render_message(msg_template, c).strip() if msg_template else ""
        preview.append({"number": number, "type": msg_type, "message": msg})

    return jsonify({"ok": True, "remaining": remaining, "preview": preview}), 200


@app.route("/admin/nl/preview", methods=["POST"])
def admin_nl_preview():
    guard = _require_login()
    if guard:
        return guard

    try:
        per_device = int(request.form.get("per_device") or 0)
    except Exception:
        per_device = 0

    device_ids   = [str(x) for x in request.form.getlist("device_ids") if str(x).strip()]
    _, msg_type = load_message_draft()
    _active_camp = _camps.get_active()
    if _active_camp:
        templates = _camps.get_messages(_active_camp) or msgtpl.get_all("campaign")
    else:
        templates = msgtpl.get_all("campaign")
    remaining = nl_remaining_count()

    if per_device <= 0:
        return jsonify({"ok": False, "msg": "Quantité invalide"}), 400
    if not device_ids:
        return jsonify({"ok": False, "msg": "Aucun appareil sélectionné"}), 400
    if not templates:
        return jsonify({"ok": False, "msg": "Aucun message de campagne configuré"}), 400
    if remaining <= 0:
        return jsonify({"ok": False, "msg": "Numlist vide"}), 400

    planned = per_device * len(device_ids)
    take = min(planned, remaining)
    contacts = _peek_contacts(take)
    global_link = state.global_link_get() or ""

    preview = []
    i = 0
    for did in device_ids:
        for _ in range(per_device):
            if i >= len(contacts) or len(preview) >= 10:
                break
            c = contacts[i]
            i += 1
            number = (c.get("number") or "").strip()
            if not number:
                continue
            if global_link:
                c["link"] = global_link
            msg_template = random.choice(templates)
            msg = render_message(msg_template, c).strip()
            preview.append({
                "device_id": did,
                "number": number,
                "type": msg_type,
                "message": msg,
                "contact": c,  # variables du contact pour aperçu auto-reply
            })
        if len(preview) >= 10:
            break

    ar_cfg = load_autoreply_config()
    speed  = get_send_speed()
    max_cycles = {did: state.device_max_cycles_get(did) for did in device_ids}

    return jsonify({
        "ok": True,
        "type": msg_type,
        "planned_total": planned,
        "will_send": take,
        "remaining": remaining,
        "preview": preview,
        # Paramètres campagne
        "speed":        speed,
        "per_device":   per_device,
        "device_count": len(device_ids),
        "tpl_count":    len(templates),
        "max_cycles":   max_cycles,
        # Paramètres auto-reply
        "ar_enabled":    ar_cfg.get("enabled", True),
        "ar_mode":       ar_cfg.get("reply_mode", 2),
        "ar_step0_type": ar_cfg.get("step0_type", "sms"),
        "ar_step1_type": ar_cfg.get("step1_type", "sms"),
        "ar_step0_delay": ar_cfg.get("step0_delay", 0),
        "ar_step1_delay": ar_cfg.get("step1_delay", 0),
    }), 200


@app.route("/admin/nl/send", methods=["POST"])
def admin_nl_send():
    guard = _require_login()
    if guard:
        return guard

    # Lock court anti double-clic (5s — l'envoi réel tourne dans Celery)
    if not state.set_lock("lock:nl_send", 5):
        return jsonify({"ok": False, "msg": "Patiente un instant…"}), 429

    try:
        try:
            per_device = int(request.form.get("per_device") or 0)
        except Exception:
            per_device = 0

        device_ids   = [str(x) for x in request.form.getlist("device_ids") if str(x).strip()]
        remaining    = nl_remaining_count()

        try:
            delay_minutes = max(0, int(request.form.get("delay_minutes") or 0))
        except Exception:
            delay_minutes = 0

        if per_device <= 0:
            return jsonify({"ok": False, "msg": "Quantité invalide"}), 400
        if not device_ids:
            return jsonify({"ok": False, "msg": "Aucun appareil sélectionné"}), 400
        _ac = _camps.get_active()
        _camp_ok = (_camps.count_messages(_ac) > 0 if _ac else False) or msgtpl.count("campaign") > 0
        if not _camp_ok:
            return jsonify({"ok": False, "msg": "Aucun message de campagne configuré"}), 400
        if remaining <= 0:
            return jsonify({"ok": False, "msg": "Numlist vide"}), 400

        # Pré-génère le batch_id et initialise le meta en Redis avant dispatch Celery
        batch_id      = str(uuid.uuid4())[:8]
        total_planned = per_device * len(device_ids)
        scheduled_ts  = _now() + delay_minutes * 60 if delay_minutes > 0 else None

        _ar = load_autoreply_config()
        redis_conn.hset(f"batch:{batch_id}:meta", mapping={
            "batch_id":     batch_id,
            "created_ts":   str(_now()),
            "planned":      str(total_planned),
            "sent":         "0",
            "failed":       "0",
            "status":       "scheduled" if delay_minutes > 0 else "queued",
            "device_ids":   json.dumps(device_ids),
            "template_ids": json.dumps([]),
            "per_device":   str(per_device),
            "device_count": str(len(device_ids)),
            "scheduled_ts": str(scheduled_ts) if scheduled_ts else "",
            # Paramètres campagne (pour récap post-envoi et analyse IA)
            "speed":        get_send_speed(),
            "tpl_count":    str(msgtpl.count("campaign")),
            "ar_enabled":   "1" if _ar.get("enabled") else "0",
            "ar_mode":      str(_ar.get("reply_mode", 2)),
            "ar_step0_type": _ar.get("step0_type", "sms"),
            "ar_step1_type": _ar.get("step1_type", "sms"),
            "ar_step0_delay": str(_ar.get("step0_delay", 0)),
            "ar_step1_delay": str(_ar.get("step1_delay", 0)),
        })
        redis_conn.expire(f"batch:{batch_id}:meta", 24 * 3600)

        # Stocker les paramètres par device pour auto-restart cycle
        for did in device_ids:
            state.device_last_campaign_set(did, per_device, None)

        if delay_minutes > 0:
            send_campaign.apply_async(
                args=[device_ids, per_device, batch_id, None],
                countdown=delay_minutes * 60
            )
            msg_txt = f"Envoi planifié dans {delay_minutes} min ({total_planned} messages)"
        else:
            send_campaign.delay(device_ids, per_device, batch_id, None)
            msg_txt = f"Envoi lancé — {total_planned} messages planifiés"

        return jsonify({
            "ok":           True,
            "async":        True,
            "batch_id":     batch_id,
            "planned":      total_planned,
            "scheduled_ts": scheduled_ts,
            "delay_minutes": delay_minutes,
            "msg":          msg_txt,
        }), 200

    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    finally:
        redis_conn.delete("lock:nl_send")


# ─── Batch status / historique ────────────────────────────────────────────────

@app.route("/admin/batch/<batch_id>")
def admin_batch_status(batch_id):
    guard = _require_login()
    if guard:
        return guard
    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404
    return jsonify({"ok": True, **meta}), 200


@app.route("/admin/batch/<batch_id>/details")
def admin_batch_details(batch_id):
    """Statistiques détaillées d'un batch : erreurs par type + par device."""
    guard = _require_login()
    if guard:
        return guard

    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404

    failed_key = f"batch:{batch_id}:failed"
    sent_key   = f"batch:{batch_id}:sent"

    # Lire tous les enregistrements (max 5000 pour éviter une surcharge)
    failed_raws = redis_conn.lrange(failed_key, 0, 4999) or []
    sent_raws   = redis_conn.lrange(sent_key,   0, 4999) or []

    # Agrégation des erreurs
    errors_by_type   = {}   # {error_type: count}
    errors_by_device = {}   # {device_id: {sent:N, failed:N, errors:{type:N}}}

    for raw in failed_raws:
        try:
            rec = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            err_type = rec.get("error") or "unknown"
            did      = str(rec.get("device") or "?")
            errors_by_type[err_type] = errors_by_type.get(err_type, 0) + 1
            dev = errors_by_device.setdefault(did, {"sent": 0, "failed": 0, "errors": {}})
            dev["failed"] += 1
            dev["errors"][err_type] = dev["errors"].get(err_type, 0) + 1
        except Exception:
            continue

    for raw in sent_raws:
        try:
            rec = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
            did = str(rec.get("device") or "?")
            dev = errors_by_device.setdefault(did, {"sent": 0, "failed": 0, "errors": {}})
            dev["sent"] += 1
        except Exception:
            continue

    planned  = int(meta.get("planned") or 0)
    sent_cnt = int(meta.get("sent") or 0)
    fail_cnt = int(meta.get("failed") or 0)
    rate     = round(sent_cnt * 100 / max(planned, 1), 1)

    # device_ids depuis le meta
    try:
        device_ids_list = json.loads(meta.get("device_ids") or "[]")
    except Exception:
        device_ids_list = []

    # Heure d'envoi (pour analyse patterns temporels)
    created_ts = int(meta.get("created_ts") or 0)
    import datetime as _dt
    hour_of_day = _dt.datetime.utcfromtimestamp(created_ts).hour if created_ts else None

    return jsonify({
        "ok": True,
        "batch_id":        batch_id,
        "status":          meta.get("status"),
        "created_ts":      meta.get("created_ts"),
        "scheduled_ts":    meta.get("scheduled_ts", ""),
        "type":            meta.get("type", "sms"),
        "planned":         planned,
        "sent":            sent_cnt,
        "failed":          fail_cnt,
        "success_rate":    rate,
        "errors_by_type":  errors_by_type,
        "devices":         errors_by_device,
        # Paramètres campagne (récap + analyse IA)
        "device_ids":      device_ids_list,
        "per_device":      int(meta.get("per_device") or 0),
        "device_count":    int(meta.get("device_count") or 0),
        "speed":           meta.get("speed", "0"),
        "tpl_count":       int(meta.get("tpl_count") or 0),
        "ar_enabled":      meta.get("ar_enabled", "0") == "1",
        "ar_mode":         int(meta.get("ar_mode") or 2),
        "ar_step0_type":   meta.get("ar_step0_type", "sms"),
        "ar_step1_type":   meta.get("ar_step1_type", "sms"),
        "ar_step0_delay":  float(meta.get("ar_step0_delay") or 0),
        "ar_step1_delay":  float(meta.get("ar_step1_delay") or 0),
        "hour_of_day":     hour_of_day,
    }), 200


@app.route("/admin/batches")
def admin_batches():
    guard = _require_login()
    if guard:
        return guard
    batches = get_recent_batches(15)
    return jsonify({"ok": True, "batches": batches}), 200


# ─── Blacklist ─────────────────────────────────────────────────────────────────

@app.route("/admin/blacklist")
def admin_blacklist_get():
    guard = _require_login()
    if guard:
        return guard
    numbers = get_blacklist()
    return jsonify({"ok": True, "count": len(numbers), "numbers": numbers}), 200


@app.route("/admin/blacklist/clear", methods=["POST"])
def admin_blacklist_clear():
    guard = _require_login()
    if guard:
        return guard
    clear_blacklist()
    if _wants_json():
        return jsonify({"ok": True, "msg": "Blacklist vidée"}), 200
    return redirect(url_for("admin_settings"))


@app.route("/admin/blacklist/remove", methods=["POST"])
def admin_blacklist_remove():
    guard = _require_login()
    if guard:
        return guard
    number = (request.form.get("number") or "").strip()
    if not number:
        return jsonify({"ok": False, "msg": "Numéro manquant"}), 400
    remove_from_blacklist(number)
    return jsonify({"ok": True, "msg": f"{number} retiré de la blacklist"}), 200


# ─── Auto-reply ───────────────────────────────────────────────────────────────

@app.route("/admin/autoreply/sample", methods=["GET"])
def admin_autoreply_sample():
    """Retourne un aperçu des messages auto-reply avec variables remplacées."""
    guard = _require_login()
    if guard:
        return guard

    from services.batches import render_message as _render
    cfg = load_autoreply_config()

    # Contact de démo fourni en query param (JSON)
    contact_raw = (request.args.get("contact") or "").strip()
    demo_contact = {}
    if contact_raw:
        try:
            demo_contact = json.loads(contact_raw)
        except Exception:
            pass

    # Global link
    try:
        lnk = state.global_link_get()
        if lnk:
            demo_contact.setdefault("link", lnk)
    except Exception:
        pass

    # Identique à tasks.py : lire depuis l'arcamp actif en priorité
    active0 = _arcamps.get_active_step(0)
    active1 = _arcamps.get_active_step(1)
    step0_tpl = (_arcamps.pick_random(active0, 0) if active0 else None) or msgtpl.pick_random("ar:step0") or ""
    step1_tpl = (_arcamps.pick_random(active1, 1) if active1 else None) or msgtpl.pick_random("ar:step1") or ""
    pool_cnt0 = _arcamps.count_messages(active0, 0) if active0 else msgtpl.count("ar:step0")
    pool_cnt1 = _arcamps.count_messages(active1, 1) if active1 else msgtpl.count("ar:step1")

    step0_rendered = _render(step0_tpl, demo_contact) if step0_tpl and demo_contact else step0_tpl
    step1_rendered = _render(step1_tpl, demo_contact) if step1_tpl and demo_contact else step1_tpl

    return jsonify({
        "ok": True,
        "enabled":    cfg.get("enabled", False),
        "reply_mode": cfg.get("reply_mode", 2),
        "step0_text": step0_rendered,
        "step0_type": cfg.get("step0_type", "sms"),
        "step0_delay": cfg.get("step0_delay", 0),
        "step1_text": step1_rendered,
        "step1_type": cfg.get("step1_type", "sms"),
        "step1_delay": cfg.get("step1_delay", 0),
        "pool_count": {
            "step0": pool_cnt0,
            "step1": pool_cnt1,
        },
    }), 200


@app.route("/admin/autoreply/save", methods=["POST"])
def admin_autoreply_save():
    guard = _require_login()
    if guard:
        return guard

    try:
        save_autoreply_config(request.form)
        if _wants_json():
            return jsonify({"ok": True, "msg": "Auto-reply enregistré"}), 200
        return redirect(url_for("admin_settings"))
    except Exception as e:
        if _wants_json():
            return jsonify({"ok": False, "msg": str(e)}), 400
        return Response(str(e), status=400, mimetype="text/plain")


# ─── Pages ────────────────────────────────────────────────────────────────────

@app.route("/admin/settings", methods=["GET"])
def admin_settings():
    guard = _require_login()
    if guard:
        return guard

    ctx = _build_page_context()
    return render_template("settings.html", **ctx)


@app.route("/admin/state", methods=["GET"])
def admin_state():
    guard = _require_login()
    if guard:
        return guard

    ctx = _build_page_context()

    # Comptage global des échecs des 24 derniers batches
    total_failed_recent = 0
    try:
        for b in get_recent_batches(24):
            total_failed_recent += int(b.get("failed") or 0)
    except Exception:
        pass

    return jsonify({
        "remaining": ctx["remaining"],
        "total_sent": ctx["total_sent"],
        "total_failed": total_failed_recent,
        "cycle_limit": ctx["cycle_limit"],
        "devices": ctx["rows"],
        "redis_ok": ctx["redis_ok"],
        "worker_ok": ctx["worker_ok"],
        "autoreply_ok": ctx["autoreply_ok"],
        "worker_last_seen": ctx["worker_last_seen"],
        "vars_list": ctx["vars_list"],
        "ar_updated_ts":   int((ctx["ar_cfg"] or {}).get("updated_ts") or 0),
        "imported_total":  int(((ctx["nl_meta"] or {}).get("imported_total")) or 0),
        "blacklist_count": blacklist_count(),
        "send_speed": ctx["send_speed"],
        "reply_countdown": ctx["reply_countdown"],
        "global_link": ctx["global_link"],
        "tpl_counts": ctx["tpl_counts"],
        "cycle_stopped": state.cycle_stop_get(),
        "active_camp":         _camps.get_active(),
        "active_arcamp_step0": _arcamps.get_active_step(0),
        "active_arcamp_step1": _arcamps.get_active_step(1),
        "img_toggles": {
            "campaign":  (redis_conn.get("config:img_toggle:campaign")  or b"0").decode() == "1",
            "ar:step0":  (redis_conn.get("config:img_toggle:ar:step0")  or b"0").decode() == "1",
            "ar:step1":  (redis_conn.get("config:img_toggle:ar:step1")  or b"0").decode() == "1",
        },
        "audio_toggles": {
            "campaign":  (redis_conn.get("config:audio_toggle:campaign")  or b"0").decode() == "1",
            "ar:step0":  (redis_conn.get("config:audio_toggle:ar:step0")  or b"0").decode() == "1",
            "ar:step1":  (redis_conn.get("config:audio_toggle:ar:step1")  or b"0").decode() == "1",
        },
        "audio_filename": (redis_conn.get("config:audio:filename") or b"").decode(),
        "ts": ctx["ts"],
    })


# ─── Toggle image dans les messages ──────────────────────────────────────────

@app.route("/admin/config/img_toggle", methods=["POST"])
def admin_img_toggle():
    guard = _require_login()
    if guard:
        return guard
    zone = request.form.get("zone", "")
    enabled = request.form.get("enabled", "0")
    if zone not in ("campaign", "ar:step0", "ar:step1"):
        return jsonify({"ok": False, "msg": "zone invalide"}), 400
    if enabled == "1":
        redis_conn.set(f"config:img_toggle:{zone}", "1")
    else:
        redis_conn.delete(f"config:img_toggle:{zone}")
    return jsonify({"ok": True, "enabled": enabled == "1"}), 200


# ─── Stop / reprise auto-restart cycles ───────────────────────────────────────

@app.route("/admin/cycle/stop", methods=["POST"])
def admin_cycle_stop():
    guard = _require_login()
    if guard:
        return guard
    state.cycle_stop_set(True)
    return jsonify({"ok": True, "msg": "Auto-restart cycles stoppé", "cycle_stopped": True})


@app.route("/admin/cycle/resume", methods=["POST"])
def admin_cycle_resume():
    guard = _require_login()
    if guard:
        return guard
    state.cycle_stop_set(False)
    return jsonify({"ok": True, "msg": "Auto-restart cycles repris", "cycle_stopped": False})


@app.route("/admin/batch/cancel_all", methods=["POST"])
def admin_batch_cancel_all():
    """Annule TOUS les batches running/queued/paused + stoppe l'auto-restart cycles."""
    guard = _require_login()
    if guard:
        return guard
    try:
        state.cycle_stop_set(True)
        cancelled = 0
        for key in redis_conn.scan_iter(match="batch:*:meta", count=200):
            try:
                status_raw = redis_conn.hget(key, "status")
                if not status_raw:
                    continue
                status = status_raw.decode("utf-8") if isinstance(status_raw, bytes) else status_raw
                if status in ("running", "queued", "paused"):
                    batch_id_raw = redis_conn.hget(key, "batch_id")
                    if batch_id_raw:
                        bid = batch_id_raw.decode("utf-8") if isinstance(batch_id_raw, bytes) else batch_id_raw
                        redis_conn.set(f"batch:{bid}:cancelled", "1", ex=3600)
                        redis_conn.hset(key, "status", "cancelled")
                        cancelled += 1
            except Exception:
                continue
        return jsonify({"ok": True, "msg": f"{cancelled} batch(es) annulé(s) — cycles stoppés", "cancelled": cancelled})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─── Countdown reply + lien global ────────────────────────────────────────────

@app.route("/admin/reply_countdown/save", methods=["POST"])
def admin_reply_countdown_save():
    guard = _require_login()
    if guard:
        return guard
    value = (request.form.get("reply_countdown") or "60-180").strip()
    state.reply_countdown_save(value)
    label = value if value != "0" else "0 (immédiat)"
    return jsonify({"ok": True, "msg": f"Countdown réponse = {label}"}), 200


@app.route("/admin/global_link/save", methods=["POST"])
def admin_global_link_save():
    guard = _require_login()
    if guard:
        return guard
    link = (request.form.get("global_link") or "").strip()
    state.global_link_set(link)
    return jsonify({"ok": True, "msg": "Lien global enregistré"}), 200


# ─── Vitesse d'envoi ──────────────────────────────────────────────────────────

@app.route("/admin/send_speed/save", methods=["POST"])
def admin_send_speed_save():
    guard = _require_login()
    if guard:
        return guard

    speed = (request.form.get("send_speed") or "0").strip()
    save_send_speed(speed)
    return jsonify({"ok": True, "msg": f"Vitesse = {speed or '0 (max)'}"}), 200


# ─── Batch pause / reprise / annulation ───────────────────────────────────────

@app.route("/admin/batch/<batch_id>/pause", methods=["POST"])
def admin_batch_pause(batch_id):
    guard = _require_login()
    if guard:
        return guard

    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404
    if meta.get("status") not in ("running", "queued"):
        return jsonify({"ok": False, "msg": f"Batch déjà {meta.get('status')}"}), 400

    redis_conn.set(f"batch:{batch_id}:paused", "1", ex=24 * 3600)
    return jsonify({"ok": True, "msg": "Pause demandée"}), 200


@app.route("/admin/batch/<batch_id>/resume", methods=["POST"])
def admin_batch_resume(batch_id):
    guard = _require_login()
    if guard:
        return guard

    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404

    redis_conn.delete(f"batch:{batch_id}:paused")

    # Re-dispatch le Celery task si le batch était en pause
    if meta.get("status") == "paused":
        try:
            per_device = int(meta.get("per_device") or 1)
        except Exception:
            per_device = 1

        # Récupère les device_ids depuis nl:campaign_template_ids et le meta
        # On ne peut pas reconstruire device_ids depuis meta seul → on passe [] et
        # create_batch utilisera les devices encore en session (nl:queue restant).
        # Pour simplifier : on stocke device_ids dans le meta au moment du lancement.
        raw_devices = meta.get("device_ids") or ""
        try:
            device_ids = json.loads(raw_devices)
        except Exception:
            device_ids = []

        if not device_ids:
            return jsonify({"ok": False, "msg": "Impossible de reprendre : device_ids introuvables. Relance manuellement."}), 400

        template_ids_raw = meta.get("template_ids") or ""
        try:
            template_ids = json.loads(template_ids_raw) if template_ids_raw else None
        except Exception:
            template_ids = None

        redis_conn.hset(f"batch:{batch_id}:meta", mapping={"status": "queued"})
        send_campaign.delay(device_ids, per_device, batch_id, template_ids)

    return jsonify({"ok": True, "msg": "Reprise en cours"}), 200


@app.route("/admin/batch/<batch_id>/cancel", methods=["POST"])
def admin_batch_cancel(batch_id):
    guard = _require_login()
    if guard:
        return guard

    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404

    redis_conn.delete(f"batch:{batch_id}:paused")
    redis_conn.set(f"batch:{batch_id}:cancelled", "1", ex=24 * 3600)
    redis_conn.hset(f"batch:{batch_id}:meta", mapping={"status": "cancelled"})
    return jsonify({"ok": True, "msg": "Batch annulé"}), 200


@app.route("/admin/batch/<batch_id>/send_now", methods=["POST"])
def admin_batch_send_now(batch_id):
    """Lance immédiatement un batch programmé (annule le countdown Celery, redispatch)."""
    guard = _require_login()
    if guard:
        return guard

    meta = get_batch_status(batch_id)
    if not meta:
        return jsonify({"ok": False, "msg": "Batch introuvable"}), 404
    if meta.get("status") not in ("scheduled", "queued"):
        return jsonify({"ok": False, "msg": "Ce batch n'est plus en attente"}), 400

    try:
        device_ids_raw = meta.get("device_ids", "[]")
        device_ids     = json.loads(device_ids_raw)
        per_device     = int(meta.get("per_device") or 0)
    except Exception:
        return jsonify({"ok": False, "msg": "Paramètres batch invalides"}), 400

    if not device_ids or per_device <= 0:
        return jsonify({"ok": False, "msg": "Paramètres batch incomplets"}), 400

    # Marquer l'ancien batch comme annulé pour bloquer le countdown Celery
    redis_conn.set(f"batch:{batch_id}:cancelled", "1", ex=24 * 3600)
    redis_conn.hset(f"batch:{batch_id}:meta", mapping={"status": "cancelled"})

    # Créer un nouveau batch immédiat avec les mêmes paramètres
    new_id       = str(uuid.uuid4())[:8]
    total        = per_device * len(device_ids)
    _ar          = load_autoreply_config()
    redis_conn.hset(f"batch:{new_id}:meta", mapping={
        "batch_id":     new_id,
        "created_ts":   str(_now()),
        "planned":      str(total),
        "sent":         "0",
        "failed":       "0",
        "status":       "queued",
        "device_ids":   json.dumps(device_ids),
        "template_ids": json.dumps([]),
        "per_device":   str(per_device),
        "device_count": str(len(device_ids)),
        "scheduled_ts": "",
        "speed":        get_send_speed(),
        "tpl_count":    str(msgtpl.count("campaign")),
        "ar_enabled":   "1" if _ar.get("enabled") else "0",
        "ar_mode":      str(_ar.get("reply_mode", 2)),
        "ar_step0_type": _ar.get("step0_type", "sms"),
        "ar_step1_type": _ar.get("step1_type", "sms"),
        "ar_step0_delay": str(_ar.get("step0_delay", 0)),
        "ar_step1_delay": str(_ar.get("step1_delay", 0)),
    })
    redis_conn.expire(f"batch:{new_id}:meta", 24 * 3600)
    send_campaign.delay(device_ids, per_device, new_id, None)

    return jsonify({
        "ok": True,
        "msg": f"Envoi lancé ({total} messages)",
        "batch_id": new_id,
        "planned": total,
    }), 200


# ─── Max cycles par device ─────────────────────────────────────────────────────

@app.route("/admin/device/max_cycles/save", methods=["POST"])
def admin_device_max_cycles_save():
    guard = _require_login()
    if guard:
        return guard

    device_id  = (request.form.get("device_id") or "").strip()
    try:
        max_cycles = int(request.form.get("max_cycles") or 0)
    except Exception:
        max_cycles = 0

    if not device_id:
        return jsonify({"ok": False, "msg": "device_id manquant"}), 400
    if max_cycles < 0:
        max_cycles = 0

    state.device_max_cycles_set(device_id, max_cycles)
    label = f"{max_cycles}" if max_cycles > 0 else "illimité"
    return jsonify({"ok": True, "msg": f"Max cycles device {device_id} = {label}"}), 200


# ─── Messages reçus par device ────────────────────────────────────────────────

@app.route("/admin/device/<device_id>/recv", methods=["GET"])
def admin_device_recv(device_id):
    guard = _require_login()
    if guard:
        return guard
    device_id = device_id.strip()[:64]
    try:
        raw = redis_conn.lrange(f"recv:msgs:{device_id}", 0, 199) or []
        msgs = []
        for r in raw:
            try:
                msgs.append(json.loads(r.decode("utf-8") if isinstance(r, bytes) else r))
            except Exception:
                continue
        return jsonify({"ok": True, "msgs": msgs, "total": len(msgs)})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─── Messages envoyés récents (tous batches) ──────────────────────────────────

@app.route("/admin/sent/recent", methods=["GET"])
def admin_sent_recent():
    guard = _require_login()
    if guard:
        return guard
    try:
        limit = min(int(request.args.get("limit", 200)), 500)
        batches = get_recent_batches(20)
        msgs = []
        if batches:
            pipe = redis_conn.pipeline()
            batch_ids = [b["batch_id"] for b in batches]
            for bid in batch_ids:
                pipe.lrange(f"batch:{bid}:sent", 0, 49)
            results = pipe.execute()
            for i, rows in enumerate(results):
                bid = batch_ids[i]
                for r in (rows or []):
                    try:
                        entry = json.loads(r.decode("utf-8") if isinstance(r, bytes) else r)
                        entry["batch_id"] = bid
                        msgs.append(entry)
                    except Exception:
                        continue
        msgs.sort(key=lambda x: x.get("ts", 0), reverse=True)
        return jsonify({"ok": True, "msgs": msgs[:limit], "total": len(msgs[:limit])})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─── Batch delete / clear historique ──────────────────────────────────────────

@app.route("/admin/batch/<batch_id>/delete", methods=["POST"])
def admin_batch_delete(batch_id):
    guard = _require_login()
    if guard:
        return guard
    batch_id = batch_id.strip()[:16]
    if not batch_id:
        return jsonify({"ok": False, "msg": "ID manquant"}), 400
    try:
        for suffix in ["meta", "sent", "failed", "paused", "cancelled"]:
            redis_conn.delete(f"batch:{batch_id}:{suffix}")
        return jsonify({"ok": True, "msg": "Batch supprimé"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/admin/batches/clear", methods=["POST"])
def admin_batches_clear():
    guard = _require_login()
    if guard:
        return guard
    try:
        count = 0
        for key in redis_conn.scan_iter(match="batch:*:meta", count=200):
            status_raw = redis_conn.hget(key, "status")
            if not status_raw:
                continue
            status = status_raw.decode("utf-8") if isinstance(status_raw, bytes) else status_raw
            if status in ("done", "cancelled", "error"):
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                batch_id_val = key_str.split(":")[1]
                for suffix in ["meta", "sent", "failed", "paused", "cancelled"]:
                    redis_conn.delete(f"batch:{batch_id_val}:{suffix}")
                count += 1
        return jsonify({"ok": True, "msg": f"{count} batch(es) supprimé(s)"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ─── Pools de messages (msgtpl) ───────────────────────────────────────────────

_VALID_SLOTS = ("campaign", "ar:step0", "ar:step1")


@app.route("/admin/tpl/<slot>", methods=["GET"])
def admin_tpl_list(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            items = _camps.get_messages(active)
            return jsonify({"ok": True, "items": items, "count": len(items)}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step  = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            items = _arcamps.get_messages(active, step)
            return jsonify({"ok": True, "items": items, "count": len(items)}), 200
    items = msgtpl.get_all(slot)
    return jsonify({"ok": True, "items": items, "count": len(items)}), 200


@app.route("/admin/tpl/<slot>/add", methods=["POST"])
def admin_tpl_add(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    text = (request.form.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "msg": "Texte vide"}), 400
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            _camps.add_message(active, text)
            return jsonify({"ok": True, "count": _camps.count_messages(active)}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            _arcamps.add_message(active, step, text)
            return jsonify({"ok": True, "count": _arcamps.count_messages(active, step)}), 200
    ok = msgtpl.add(slot, text)
    return jsonify({"ok": ok, "count": msgtpl.count(slot)}), 200


@app.route("/admin/tpl/<slot>/delete", methods=["POST"])
def admin_tpl_delete(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    text = (request.form.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "msg": "Texte vide"}), 400
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            _camps.delete_message(active, text)
            return jsonify({"ok": True, "count": _camps.count_messages(active)}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            _arcamps.delete_message(active, step, text)
            return jsonify({"ok": True, "count": _arcamps.count_messages(active, step)}), 200
    ok = msgtpl.delete(slot, text)
    return jsonify({"ok": ok, "count": msgtpl.count(slot)}), 200


@app.route("/admin/tpl/<slot>/import", methods=["POST"])
def admin_tpl_import(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    f = request.files.get("file")
    if not f or f.filename == "":
        return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            added = _camps.import_csv_bytes(active, f.read())
            return jsonify({"ok": True, "added": added, "count": _camps.count_messages(active),
                            "msg": f"{added} message(s) importé(s)"}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step  = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            added = _arcamps.import_csv_bytes(active, step, f.read())
            return jsonify({"ok": True, "added": added, "count": _arcamps.count_messages(active, step),
                            "msg": f"{added} message(s) importé(s)"}), 200
    added = msgtpl.import_csv_bytes(slot, f.read())
    return jsonify({"ok": True, "added": added, "count": msgtpl.count(slot),
                    "msg": f"{added} message(s) importé(s)"}), 200


@app.route("/admin/tpl/<slot>/update", methods=["POST"])
def admin_tpl_update(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    old_text = (request.form.get("old_text") or "").strip()
    new_text = (request.form.get("new_text") or "").strip()
    if not old_text or not new_text:
        return jsonify({"ok": False, "msg": "Texte vide"}), 400
    if old_text == new_text:
        return jsonify({"ok": True, "count": msgtpl.count(slot)}), 200
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            _camps.delete_message(active, old_text)
            _camps.add_message(active, new_text)
            return jsonify({"ok": True, "count": _camps.count_messages(active)}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            _arcamps.delete_message(active, step, old_text)
            _arcamps.add_message(active, step, new_text)
            return jsonify({"ok": True, "count": _arcamps.count_messages(active, step)}), 200
    msgtpl.delete(slot, old_text)
    msgtpl.add(slot, new_text)
    return jsonify({"ok": True, "count": msgtpl.count(slot)}), 200


@app.route("/admin/tpl/<slot>/clear", methods=["POST"])
def admin_tpl_clear(slot):
    guard = _require_login()
    if guard:
        return guard
    if slot not in _VALID_SLOTS:
        return jsonify({"ok": False, "msg": "Slot invalide"}), 400
    if slot == "campaign":
        active = _camps.get_active()
        if active:
            _camps.clear_messages(active)
            return jsonify({"ok": True, "count": 0}), 200
    elif slot in ("ar:step0", "ar:step1"):
        step = int(slot[-1])
        active = _arcamps.get_active_step(step)
        if active:
            _arcamps.clear_messages(active, step)
            return jsonify({"ok": True, "count": 0}), 200
    msgtpl.clear(slot)
    return jsonify({"ok": True, "count": 0}), 200


# ─── Blocs de messages (camps) ────────────────────────────────────────────────

@app.route("/admin/camps", methods=["GET"])
def admin_camps_list():
    guard = _require_login()
    if guard: return guard
    return jsonify({"ok": True, "camps": _camps.list_camps(), "active": _camps.get_active()})


@app.route("/admin/camps", methods=["POST"])
def admin_camps_create():
    guard = _require_login()
    if guard: return guard
    name = (request.form.get("name") or "Bloc").strip()[:50]
    cid = _camps.create_camp(name)
    _camps.set_active(cid)
    return jsonify({"ok": True, "id": cid, "name": name})


@app.route("/admin/camps/active", methods=["POST"])
def admin_camps_set_active():
    guard = _require_login()
    if guard: return guard
    cid = (request.form.get("id") or "").strip()
    _camps.set_active(cid)
    return jsonify({"ok": True})


@app.route("/admin/camps/<cid>/rename", methods=["POST"])
def admin_camps_rename(cid):
    guard = _require_login()
    if guard: return guard
    name = (request.form.get("name") or "").strip()[:50]
    if not name:
        return jsonify({"ok": False, "msg": "Nom vide"}), 400
    _camps.rename_camp(cid, name)
    return jsonify({"ok": True})


@app.route("/admin/camps/<cid>/delete", methods=["POST"])
def admin_camps_delete(cid):
    guard = _require_login()
    if guard: return guard
    _camps.delete_camp(cid)
    # Si plus aucun bloc, ensure_default recrée "Défaut"
    _camps.ensure_default()
    return jsonify({"ok": True, "active": _camps.get_active()})


@app.route("/admin/camps/<cid>/add", methods=["POST"])
def admin_camps_add_msg(cid):
    guard = _require_login()
    if guard: return guard
    text = (request.form.get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "msg": "Texte vide"}), 400
    _camps.add_message(cid, text)
    return jsonify({"ok": True, "count": _camps.count_messages(cid)})


@app.route("/admin/camps/<cid>/delete-msg", methods=["POST"])
def admin_camps_delete_msg(cid):
    guard = _require_login()
    if guard: return guard
    text = (request.form.get("text") or "")
    _camps.delete_message(cid, text)
    return jsonify({"ok": True, "count": _camps.count_messages(cid)})


@app.route("/admin/camps/<cid>/import", methods=["POST"])
def admin_camps_import(cid):
    guard = _require_login()
    if guard: return guard
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
    n = _camps.import_csv_bytes(cid, f.read())
    if n == 0:
        return jsonify({"ok": False, "msg": "Aucun message importé"}), 400
    return jsonify({"ok": True, "msg": f"{n} messages importés", "count": _camps.count_messages(cid)})


@app.route("/admin/camps/<cid>/msgs", methods=["GET"])
def admin_camps_msgs(cid):
    guard = _require_login()
    if guard: return guard
    items = _camps.get_messages(cid)
    return jsonify({"ok": True, "items": items, "count": len(items)})


# ─── Blocs auto-reply (arcamps) ───────────────────────────────────────────────

@app.route("/admin/arcamps", methods=["GET"])
def admin_arcamps_list():
    guard = _require_login()
    if guard: return guard
    return jsonify({"ok": True, "arcamps": _arcamps.list_arcamps(),
                    "active_step0": _arcamps.get_active_step(0),
                    "active_step1": _arcamps.get_active_step(1)})


@app.route("/admin/arcamps", methods=["POST"])
def admin_arcamps_create():
    guard = _require_login()
    if guard: return guard
    name = (request.form.get("name") or "Bloc AR").strip()[:50]
    cid = _arcamps.create_arcamp(name)
    # Si le step est précisé, activer seulement ce step ; sinon activer les deux (compat)
    try:
        step_raw = request.form.get("step")
        if step_raw is not None:
            step = int(step_raw)
            if step in (0, 1):
                _arcamps.set_active_step(step, cid)
            else:
                _arcamps.set_active_step(0, cid)
                _arcamps.set_active_step(1, cid)
        else:
            _arcamps.set_active_step(0, cid)
            _arcamps.set_active_step(1, cid)
    except Exception:
        _arcamps.set_active_step(0, cid)
        _arcamps.set_active_step(1, cid)
    return jsonify({"ok": True, "id": cid, "name": name})


@app.route("/admin/arcamps/active", methods=["POST"])
def admin_arcamps_set_active():
    guard = _require_login()
    if guard: return guard
    cid = (request.form.get("id") or "").strip()
    try:
        step = int(request.form.get("step") or 0)
        if step not in (0, 1):
            step = 0
    except Exception:
        step = 0
    _arcamps.set_active_step(step, cid)
    return jsonify({"ok": True})


@app.route("/admin/arcamps/<cid>/rename", methods=["POST"])
def admin_arcamps_rename(cid):
    guard = _require_login()
    if guard: return guard
    name = (request.form.get("name") or "").strip()[:50]
    if not name:
        return jsonify({"ok": False, "msg": "Nom vide"}), 400
    _arcamps.rename_arcamp(cid, name)
    return jsonify({"ok": True})


@app.route("/admin/arcamps/<cid>/delete", methods=["POST"])
def admin_arcamps_delete(cid):
    guard = _require_login()
    if guard: return guard
    _arcamps.delete_arcamp(cid)
    _arcamps.ensure_default()
    return jsonify({"ok": True, "active_step0": _arcamps.get_active_step(0), "active_step1": _arcamps.get_active_step(1)})


# ─── Webhook SMS entrant ──────────────────────────────────────────────────────

@app.route("/sms_auto_reply", methods=["POST"])
def sms_auto_reply():
    request_id = str(uuid.uuid4())[:8]
    messages_raw = request.form.get("messages")

    if not messages_raw:
        log(f"[{request_id}] ❌ Champ 'messages' manquant")
        return "messages manquants", 400

    if not DEBUG_MODE:
        if not API_KEY:
            log(f"[{request_id}] ❌ API_KEY non configurée")
            return "Configuration manquante", 500

        signature = request.headers.get("X-SG-SIGNATURE")
        if not signature:
            log(f"[{request_id}] ❌ Signature manquante")
            return "Signature requise", 403

        expected_hash = base64.b64encode(
            hmac.new(API_KEY.encode(), messages_raw.encode(), hashlib.sha256).digest()
        ).decode()

        if not hmac.compare_digest(signature, expected_hash):
            log(f"[{request_id}] ❌ Signature invalide")
            return "Signature invalide", 403

    try:
        messages = json.loads(messages_raw)
    except json.JSONDecodeError as e:
        log(f"[{request_id}] ❌ JSON invalide : {e}")
        return "Format JSON invalide", 400

    if not isinstance(messages, list):
        return "Liste attendue", 400

    min_cd, max_cd = state.reply_countdown_get()
    dispatched = 0
    for msg in messages:
        try:
            number    = str(msg.get("number") or "").strip()
            msg_id    = msg.get("ID")
            device_id = str(msg.get("deviceID") or "").strip()
            # Comptage immédiat (avant le countdown Celery)
            if number and msg_id and device_id:
                recv_key = f"recv_seen:{number}:{msg_id}"
                if redis_conn.set(recv_key, "1", nx=True, ex=7 * 24 * 3600):
                    state.device_mark_seen(device_id)
                    state.device_incr_received(device_id, 1)
                    # Stocker le message dans la liste récente du device (200 max, TTL 7j)
                    msg_body = str(msg.get("message") or msg.get("body") or "")
                    recv_entry = json.dumps({
                        "from": number, "body": msg_body,
                        "ts": int(time.time()), "id": str(msg_id),
                    }, ensure_ascii=False)
                    recv_list = f"recv:msgs:{device_id}"
                    redis_conn.lpush(recv_list, recv_entry)
                    redis_conn.ltrim(recv_list, 0, 199)
                    redis_conn.expire(recv_list, 7 * 24 * 3600)
                    # Auto-restart cycle immédiat (pas d'attente du worker Celery)
                    try:
                        _check_cycle_auto_restart(device_id)
                    except Exception:
                        pass
            # Horodatage réception pour anti double-reply dans le worker
            msg["recv_ts"] = int(time.time())
            delay = random.randint(min_cd, max_cd) if max_cd > min_cd else min_cd
            if delay > 0:
                process_message.apply_async(args=[json.dumps(msg)], countdown=delay)
            else:
                process_message.delay(json.dumps(msg))
            dispatched += 1
        except Exception as e:
            log(f"[{request_id}] ❌ Erreur Celery : {e}")

    log(f"[{request_id}] ✅ {dispatched}/{len(messages)} messages dispatchés")
    return "OK", 200


# ─── Logs ─────────────────────────────────────────────────────────────────────

@app.route("/logs")
def logs():
    guard = _require_login()
    if guard:
        return guard

    try:
        items = redis_conn.lrange("logs:lines", 0, -1)
        if items:
            txt = "\n".join([
                x.decode("utf-8", errors="ignore") if isinstance(x, (bytes, bytearray)) else str(x)
                for x in items
            ])
            return Response(txt, mimetype="text/plain")
    except Exception:
        pass

    if not os.path.exists(LOG_FILE):
        return Response("Aucun log", mimetype="text/plain")

    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        return Response(f.read(), mimetype="text/plain")
