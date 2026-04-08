import json
import os
import time
import requests
from logger import log
from services.app_config import SERVER, API_KEY

VERIFY_SSL = (os.getenv("GATEWAY_VERIFY_SSL") or "1").strip() not in ("0", "false", "False")

_devices_cache = {"data": [], "ts": 0}
_CACHE_TTL = 30  # secondes

# Session persistante — réutilise les connexions TCP+SSL (keep-alive)
_session = requests.Session()

def fetch_gateway_devices():
    if not SERVER or not API_KEY:
        log("❌ fetch_gateway_devices: SERVER/API_KEY missing")
        return []

    now = int(time.time())
    if now - _devices_cache["ts"] < _CACHE_TTL:
        return _devices_cache["data"]

    url = f"{SERVER.rstrip('/')}/services/get-devices.php"

    try:
        r = _session.post(url, data={"key": API_KEY}, timeout=(5, 20), verify=VERIFY_SSL)
        if r.status_code != 200:
            log(f"❌ fetch_gateway_devices: HTTP {r.status_code} | body={r.text[:200]}")
            return _devices_cache["data"]

        data = r.json()

        if not isinstance(data, dict):
            log(f"❌ fetch_gateway_devices: JSON not dict: {type(data).__name__}")
            return _devices_cache["data"]

        if not data.get("success"):
            log(f"❌ fetch_gateway_devices: success=false | data={str(data)[:200]}")
            return _devices_cache["data"]

        devices = (data.get("data") or {}).get("devices") or []
        if not isinstance(devices, list):
            log(f"❌ fetch_gateway_devices: devices not list: {type(devices).__name__}")
            return _devices_cache["data"]

        _devices_cache["data"] = devices
        _devices_cache["ts"] = now
        return devices

    except Exception as e:
        log(f"❌ fetch_gateway_devices error: {e}")
        return _devices_cache["data"]


def gateway_send_message(number: str, message: str, device_id: str, msg_type: str, media_url: str = "", audio_url: str = ""):
    """
    Envoi réel via le gateway: /services/send.php
    media_url : URL image MMS. audio_url : URL vocal MMS.
    Si l'un ou l'autre est fourni, force type=mms et passe les deux dans attachments (virgule-séparé).
    Retourne (ok: bool, detail: str)
    """
    if not SERVER or not API_KEY:
        return False, "SERVER/API_KEY missing"

    url = f"{SERVER.rstrip('/')}/services/send.php"
    attachments_parts = [u for u in [media_url, audio_url] if u]
    if attachments_parts:
        msg_type = "mms"
    payload = {
        "number": number,
        "message": message,
        "devices": json.dumps([str(device_id)]),  # send.php attend "devices" (JSON array), pas "device"
        "type": msg_type,
        "prioritize": 1,
        "key": API_KEY,
    }
    if attachments_parts:
        payload["attachments"] = ",".join(attachments_parts)

    last_err = ""
    for attempt in range(1, 4):
        try:
            r = _session.post(url, data=payload, timeout=(5, 25), verify=VERIFY_SSL)
            if 200 <= r.status_code < 300:
                try:
                    j = r.json()
                    if isinstance(j, dict) and not j.get("success"):
                        err_obj = j.get("error") or {}
                        last_err = err_obj.get("message") or "success=false"
                    else:
                        # Extraire l'ID du message pour le tracking
                        try:
                            msgs = ((j or {}).get("data") or {}).get("messages") or []
                            gw_id = str(msgs[0].get("ID", "")) if msgs else ""  # uppercase "ID"
                        except Exception:
                            gw_id = ""
                        return True, gw_id
                except Exception:
                    last_err = "json_parse_error"
            else:
                last_err = f"http_{r.status_code}"
                log(f"❌ send gateway HTTP {r.status_code} body={r.text[:200]}")
        except Exception as e:
            last_err = str(e)

        time.sleep(0.5 * attempt)

    log(f"❌ send fail device={device_id} number={number} err={last_err}")
    return False, last_err


def gateway_fetch_message_status(gw_id: str) -> str:
    """
    Interroge le gateway pour connaître le statut réel d'un message.
    Retourne le statut string ("Sent", "Failed", "Delivered", "Pending", "Queued")
    ou "" en cas d'erreur / non trouvé.
    """
    if not SERVER or not API_KEY or not gw_id:
        return ""
    url = f"{SERVER.rstrip('/')}/services/read-messages.php"
    try:
        r = _session.post(url, data={"key": API_KEY, "id": gw_id}, timeout=(5, 15), verify=VERIFY_SSL)
        if r.status_code != 200:
            return ""
        j = r.json()
        if not isinstance(j, dict) or not j.get("success"):
            return ""
        data = (j.get("data") or {})
        msgs = data.get("messages") or []
        if msgs and isinstance(msgs, list):
            return str(msgs[0].get("status") or "")
        # Réponse directe sans enveloppe "messages"
        if isinstance(data, dict) and "status" in data:
            return str(data.get("status") or "")
        return ""
    except Exception:
        return ""
