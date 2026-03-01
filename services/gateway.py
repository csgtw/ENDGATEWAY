import os
import time
import requests
from logger import log
from services.app_config import SERVER, API_KEY

# Optionnel: si ton https a un certificat chelou
# Met GATEWAY_VERIFY_SSL=0 sur Render si besoin
VERIFY_SSL = (os.getenv("GATEWAY_VERIFY_SSL") or "1").strip() not in ("0", "false", "False")


def fetch_gateway_devices():
    if not SERVER or not API_KEY:
        log("❌ fetch_gateway_devices: SERVER/API_KEY missing")
        return []

    url = f"{SERVER.rstrip('/')}/services/get-devices.php"

    try:
        r = requests.get(url, params={"key": API_KEY}, timeout=(5, 20), verify=VERIFY_SSL)
        # Log utile si ça répond pas 200
        if r.status_code != 200:
            log(f"❌ fetch_gateway_devices: HTTP {r.status_code} | body={r.text[:200]}")
            return []

        data = r.json()

        # Format attendu: {success: true, data: {devices: [...]}}
        if not isinstance(data, dict):
            log(f"❌ fetch_gateway_devices: JSON not dict: {type(data).__name__}")
            return []

        if not data.get("success"):
            log(f"❌ fetch_gateway_devices: success=false | data={str(data)[:200]}")
            return []

        devices = (data.get("data") or {}).get("devices") or []
        if not isinstance(devices, list):
            log(f"❌ fetch_gateway_devices: devices not list: {type(devices).__name__}")
            return []

        return devices

    except Exception as e:
        log(f"❌ fetch_gateway_devices error: {e}")
        return []


def gateway_send_message(number: str, message: str, device_id: str, msg_type: str):
    """
    Envoi réel via ton gateway: /services/send.php
    Retourne (ok: bool, detail: str)
    """
    if not SERVER or not API_KEY:
        return False, "SERVER/API_KEY missing"

    url = f"{SERVER.rstrip('/')}/services/send.php"
    payload = {
        "number": number,
        "message": message,
        "devices": str(device_id),
        "type": msg_type,  # sms|mms
        "prioritize": 1,
        "key": API_KEY,
    }

    last_err = ""
    for attempt in range(1, 4):
        try:
            r = requests.post(url, data=payload, timeout=(5, 25), verify=VERIFY_SSL)
            if 200 <= r.status_code < 300:
                # Certains gateways renvoient JSON {success:true/false}, d’autres non.
                try:
                    j = r.json()
                    if isinstance(j, dict) and j.get("success") is False:
                        last_err = "success=false"
                    else:
                        return True, ""
                except Exception:
                    return True, ""
            else:
                last_err = f"http_{r.status_code}"
        except Exception as e:
            last_err = str(e)

        time.sleep(0.5 * attempt)

    log(f"❌ send fail device={device_id} number={number} err={last_err}")
    return False, last_err
