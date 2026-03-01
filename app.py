import os
import json
import time
import hmac
import hashlib
import base64
import uuid
import random

from flask import (
    Flask, request, Response, redirect, url_for,
    session, render_template, jsonify
)

from logger import log
from tasks import process_message

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
    NL_QUEUE_KEY
)
from services.batches import create_batch

# Source unique de vérité pour autoreply
from services.autoreply import load_autoreply_config, save_autoreply_config


app = Flask(__name__)
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


def _wants_json() -> bool:
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return True
    acc = (request.headers.get("Accept") or "").lower()
    return "application/json" in acc


def _render_message(template: str, contact: dict) -> str:
    out = (template or "")
    for k, v in (contact or {}).items():
        if k == "number":
            continue
        out = out.replace("{{" + str(k) + "}}", str(v))
    return out


def _build_page_context() -> dict:
    """Contexte partagé entre /admin/settings et /admin/state."""
    nl_meta = load_nl_meta()
    remaining = nl_remaining_count()
    total_sent = state.global_sent_get()
    cycle_limit = state.cycle_limit_get()
    nl_message, nl_type = load_message_draft()
    vars_list = list((nl_meta or {}).get("variables") or [])
    ar_cfg = load_autoreply_config()
    redis_ok = _redis_ok()
    worker_ok = state.worker_ok()
    autoreply_ok = bool(ar_cfg.get("enabled")) and redis_ok and worker_ok

    gw_devices = fetch_gateway_devices()
    rows = []
    for d in (gw_devices or []):
        did = str(d.get("id"))
        s = state.device_snapshot(did)
        s["name"] = d.get("name") or ""
        s["model"] = d.get("model") or ""
        rows.append(s)

    rows.sort(
        key=lambda x: (int(x.get("sent") or 0), int(x.get("received") or 0)),
        reverse=True
    )

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
        "ts": _now(),
    }


# ─── Auth ─────────────────────────────────────────────────────────────────────

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        pwd = (request.form.get("password") or "").strip()
        if ADMIN_PASSWORD and pwd == ADMIN_PASSWORD:
            session["admin_logged_in"] = True
            return redirect(url_for("admin_settings"))
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
    if field not in ("received", "sent", "errors"):
        return jsonify({"ok": False, "msg": "field invalide"}), 400

    try:
        # Clé canonique — cohérente avec state.py
        redis_conn.set(f"stats:device:{device_id}:{field}", 0)
        # Reset cycle aussi si c'est received
        if field == "received":
            redis_conn.set(f"cycle:device:{device_id}:received", 0)
        return jsonify({"ok": True, "msg": f"{field} reset"}), 200
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
    state.cycles_reset_all(device_ids)

    if _wants_json():
        return jsonify({"ok": True, "msg": "Cycles reset"}), 200
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

    if _wants_json():
        return jsonify({"ok": True, "msg": f"Relancé device {device_id}"}), 200
    return redirect(url_for("admin_settings"))


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


@app.route("/admin/nl/upload", methods=["POST"])
def admin_nl_upload():
    guard = _require_login()
    if guard:
        return guard

    # Lock anti double-import (TTL 30s)
    if not state.set_lock("lock:nl_import", 30):
        return jsonify({"ok": False, "msg": "Import déjà en cours, patiente…"}), 429

    files = request.files.getlist("files")
    if not files or all(f.filename == "" for f in files):
        redis_conn.delete("lock:nl_import")
        if _wants_json():
            return jsonify({"ok": False, "msg": "Fichier manquant"}), 400
        return Response("Fichier manquant", status=400)

    try:
        res = import_files(files)
        if _wants_json():
            return jsonify({
                "ok": True,
                "msg": f"Import OK — +{res.get('added', 0)} contacts",
                "added": res.get("added", 0),
                "imported_total": res.get("imported_total", 0),
            }), 200
        return redirect(url_for("admin_settings"))
    except Exception as e:
        if _wants_json():
            return jsonify({"ok": False, "msg": f"Erreur import: {e}"}), 400
        return Response(f"Erreur import: {e}", status=400)
    finally:
        redis_conn.delete("lock:nl_import")


@app.route("/admin/nl/message", methods=["POST"])
def admin_nl_message():
    guard = _require_login()
    if guard:
        return guard

    message = (request.form.get("nl_message") or "").strip()
    msg_type = (request.form.get("nl_type") or "sms").strip().lower()
    if msg_type not in ("sms", "mms"):
        msg_type = "sms"

    if not message:
        if _wants_json():
            return jsonify({"ok": False, "msg": "Message campagne vide"}), 400
        return Response("Message campagne vide", status=400)

    save_message_draft(message, msg_type)

    if _wants_json():
        return jsonify({"ok": True, "msg": "Campagne enregistrée", "saved": True}), 200
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
    msg_template, msg_type = load_message_draft()
    msg_template = (msg_template or "").strip()

    if remaining <= 0:
        return jsonify({"ok": True, "remaining": 0, "preview": []}), 200

    take = min(count, remaining)
    start_index = max(0, remaining - take)
    raw_list = redis_conn.lrange(NL_QUEUE_KEY, start_index, remaining - 1) or []

    contacts = []
    for raw in raw_list:
        try:
            contacts.append(json.loads(raw.decode("utf-8")))
        except Exception:
            continue
    contacts = list(reversed(contacts))

    preview = []
    for c in contacts:
        number = (c.get("number") or "").strip()
        if not number:
            continue
        msg = _render_message(msg_template, c).strip() if msg_template else ""
        preview.append({"number": number, "type": msg_type, "message": msg})

    return jsonify({"ok": True, "remaining": remaining, "preview": preview[:count]}), 200


@app.route("/admin/nl/preview", methods=["POST"])
def admin_nl_preview():
    guard = _require_login()
    if guard:
        return guard

    try:
        per_device = int(request.form.get("per_device") or 0)
    except Exception:
        per_device = 0

    device_ids = [str(x) for x in request.form.getlist("device_ids") if str(x).strip()]
    msg_template, msg_type = load_message_draft()
    msg_template = (msg_template or "").strip()
    remaining = nl_remaining_count()

    if per_device <= 0:
        return jsonify({"ok": False, "msg": "Quantité invalide"}), 400
    if not device_ids:
        return jsonify({"ok": False, "msg": "Aucun appareil sélectionné"}), 400
    if not msg_template:
        return jsonify({"ok": False, "msg": "Message campagne manquant"}), 400
    if remaining <= 0:
        return jsonify({"ok": False, "msg": "Numlist vide"}), 400

    planned = per_device * len(device_ids)
    take = min(planned, remaining)
    start_index = max(0, remaining - take)
    raw_list = redis_conn.lrange(NL_QUEUE_KEY, start_index, remaining - 1) or []

    contacts = []
    for raw in raw_list:
        try:
            contacts.append(json.loads(raw.decode("utf-8")))
        except Exception:
            continue
    contacts = list(reversed(contacts))

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
            msg = _render_message(msg_template, c).strip()
            preview.append({
                "device_id": did,
                "number": number,
                "type": msg_type,
                "message": msg,
            })
        if len(preview) >= 10:
            break

    return jsonify({
        "ok": True,
        "type": msg_type,
        "planned_total": planned,
        "will_send": take,
        "remaining": remaining,
        "preview": preview,
    }), 200


@app.route("/admin/nl/send", methods=["POST"])
def admin_nl_send():
    guard = _require_login()
    if guard:
        return guard

    # Lock anti double-envoi (TTL 60s)
    if not state.set_lock("lock:nl_send", 60):
        return jsonify({"ok": False, "msg": "Envoi déjà en cours…"}), 429

    try:
        per_device = int(request.form.get("per_device") or 0)
    except Exception:
        per_device = 0

    device_ids = [str(x) for x in request.form.getlist("device_ids") if str(x).strip()]
    remaining = nl_remaining_count()
    nl_message, _ = load_message_draft()

    if per_device <= 0:
        redis_conn.delete("lock:nl_send")
        return jsonify({"ok": False, "msg": "Quantité invalide"}), 400
    if not device_ids:
        redis_conn.delete("lock:nl_send")
        return jsonify({"ok": False, "msg": "Aucun appareil sélectionné"}), 400
    if not (nl_message or "").strip():
        redis_conn.delete("lock:nl_send")
        return jsonify({"ok": False, "msg": "Message campagne manquant"}), 400
    if remaining <= 0:
        redis_conn.delete("lock:nl_send")
        return jsonify({"ok": False, "msg": "Numlist vide"}), 400

    try:
        meta = create_batch(device_ids, per_device)
        return jsonify({
            "ok": True,
            "msg": f"Envoi terminé — {meta.get('sent', 0)} envoyés, {meta.get('failed', 0)} échoués",
            "sent": meta.get("sent", 0),
            "failed": meta.get("failed", 0),
            "remaining": nl_remaining_count(),
        }), 200
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    finally:
        redis_conn.delete("lock:nl_send")


# ─── Auto-reply ───────────────────────────────────────────────────────────────

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
    return jsonify({
        "remaining": ctx["remaining"],
        "total_sent": ctx["total_sent"],
        "cycle_limit": ctx["cycle_limit"],
        "devices": ctx["rows"],
        "redis_ok": ctx["redis_ok"],
        "worker_ok": ctx["worker_ok"],
        "autoreply_ok": ctx["autoreply_ok"],
        "worker_last_seen": ctx["worker_last_seen"],
        "vars_list": ctx["vars_list"],
        "ar_updated_ts": int((ctx["ar_cfg"] or {}).get("updated_ts") or 0),
        "imported_total": int(((ctx["nl_meta"] or {}).get("imported_total")) or 0),
        "ts": ctx["ts"],
    })


# ─── Webhook SMS entrant ──────────────────────────────────────────────────────

@app.route("/sms_auto_reply", methods=["POST"])
def sms_auto_reply():
    request_id = str(uuid.uuid4())[:8]
    messages_raw = request.form.get("messages")

    if not messages_raw:
        log(f"[{request_id}] ❌ Champ 'messages' manquant")
        return "messages manquants", 400

    if not DEBUG_MODE:
        signature = request.headers.get("X-SG-SIGNATURE")
        if not signature:
            log(f"[{request_id}] ❌ Signature manquante")
            return "Signature requise", 403

        expected_hash = base64.b64encode(
            hmac.new(API_KEY.encode(), messages_raw.encode(), hashlib.sha256).digest()
        ).decode()

        if signature != expected_hash:
            log(f"[{request_id}] ❌ Signature invalide")
            return "Signature invalide", 403

    try:
        messages = json.loads(messages_raw)
    except json.JSONDecodeError as e:
        log(f"[{request_id}] ❌ JSON invalide : {e}")
        return "Format JSON invalide", 400

    if not isinstance(messages, list):
        return "Liste attendue", 400

    dispatched = 0
    for msg in messages:
        try:
            delay = random.randint(60, 180)
            process_message.apply_async(args=[json.dumps(msg)], countdown=delay)
            dispatched += 1
        except Exception as e:
            log(f"[{request_id}] ❌ Erreur Celery : {e}")

    log(f"[{request_id}] ✅ {dispatched}/{len(messages)} messages dispatchés")
    return "OK", 200


# ─── Logs ─────────────────────────────────────────────────────────────────────

@app.route("/logs")
def logs():
    try:
        items = redis_conn.lrange("logs:lines", 0, 500)
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
