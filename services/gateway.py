import json
import os
import time
import requests
from logger import log
from services.app_config import SERVER, API_KEY

VERIFY_SSL = (os.getenv("GATEWAY_VERIFY_SSL") or "1").strip() not in ("0", "false", "False")

_devices_cache = {"data": [], "ts": 0}
_CACHE_TTL = 30  # secondes


def fetch_gateway_devices():
    if not SERVER or not API_KEY:
        log("❌ fetch_gateway_devices: SERVER/API_KEY missing")
        return []

    now = int(time.time())
    if now - _devices_cache["ts"] < _CACHE_TTL:
        return _devices_cache["data"]

    url = f"{SERVER.rstrip('/')}/services/get-devices.php"

    try:
        r = requests.post(url, data={"key": API_KEY}, timeout=(5, 20), verify=VERIFY_SSL)
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


def gateway_send_message(number: str, message: str, device_id: str, msg_type: str):
    """
    Envoi réel via le gateway: /services/send.php
    Retourne (ok: bool, detail: str)
    """
    if not SERVER or not API_KEY:
        return False, "SERVER/API_KEY missing"

    url = f"{SERVER.rstrip('/')}/services/send.php"
    payload = {
        "number": number,
        "message": message,
        "devices": json.dumps([str(device_id)]),  # send.php attend "devices" (JSON array), pas "device"
        "type": msg_type,
        "prioritize": 1,
        "key": API_KEY,
    }

    last_err = ""
    for attempt in range(1, 4):
        try:
            r = requests.post(url, data=payload, timeout=(5, 25), verify=VERIFY_SSL)
            if 200 <= r.status_code < 300:
                try:
                    j = r.json()
                    if isinstance(j, dict) and not j.get("success"):
                        err_obj = j.get("error") or {}
                        last_err = err_obj.get("message") or "success=false"
                    else:
                        return True, ""
                except Exception:
                    return True, ""
            else:
                last_err = f"http_{r.status_code}"
                log(f"❌ send gateway HTTP {r.status_code} body={r.text[:200]}")
        except Exception as e:
            last_err = str(e)

        time.sleep(0.5 * attempt)

    log(f"❌ send fail device={device_id} number={number} err={last_err}")
    return False, last_err
