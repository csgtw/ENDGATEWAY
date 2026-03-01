"""
services/numlist.py
Gestion de la numlist : import CSV/XLSX, stockage Redis, draft message.
"""
import csv
import json
import time
import re
from io import BytesIO, StringIO

from openpyxl import load_workbook
from services.redis_store import redis_conn

NL_QUEUE_KEY = "nl:queue"
NL_META_KEY  = "nl:meta"
NL_DRAFT_KEY = "nl:draft"

BATCH_SIZE = 1500  # Contacts par pipeline rpush


def _now() -> int:
    return int(time.time())


def _clean_header(h) -> str:
    h = str(h or "").strip()
    h = re.sub(r"\s+", "_", h)
    return h


def _is_probably_phone_header(h: str) -> bool:
    return h.strip().lower() in (
        "number", "phone", "tel", "telephone", "mobile", "msisdn", "num", "numero"
    )


def _normalize_number(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return ""
    s = re.sub(r"[^\d\+]", "", s)
    if s.count("+") > 1:
        s = "+" + re.sub(r"[^\d]", "", s)
    if s.startswith("00"):
        s = "+" + s[2:]
    return s


def _detect_number_col(headers: list) -> str:
    for h in headers:
        if _is_probably_phone_header(h):
            return h
    return headers[0] if headers else "number"


def _parse_xlsx(file_bytes: bytes):
    wb = load_workbook(filename=BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active
    headers = None
    rows = []

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = [_clean_header(x) for x in row]
            headers = [h or f"col_{idx+1}" for idx, h in enumerate(headers)]
            continue
        if not headers:
            continue
        obj = {headers[idx]: (row[idx] if idx < len(row) else None) for idx in range(len(headers))}
        rows.append(obj)

    return headers or [], rows


def _parse_csv(file_bytes: bytes):
    try:
        text = file_bytes.decode("utf-8")
    except Exception:
        text = file_bytes.decode("latin-1", errors="ignore")

    reader = csv.reader(StringIO(text))
    headers = None
    rows = []

    for i, row in enumerate(reader):
        if i == 0:
            headers = [_clean_header(x) for x in row]
            headers = [h or f"col_{idx+1}" for idx, h in enumerate(headers)]
            continue
        if not headers:
            continue
        obj = {headers[idx]: (row[idx] if idx < len(row) else None) for idx in range(len(headers))}
        rows.append(obj)

    return headers or [], rows


def _meta_imported_total() -> int:
    try:
        return int(redis_conn.hget(NL_META_KEY, "imported_total") or 0)
    except Exception:
        return 0


def import_files(files) -> dict:
    """
    files: list de Werkzeug FileStorage
    Importe les contacts dans Redis via pipeline.
    Retourne {"added": int, "imported_total": int, "variables": list, "number_col": str}
    """
    total_added = 0
    seen_headers = None
    chosen_number_col = None
    variables = set()
    batch = []

    def flush():
        nonlocal batch
        if not batch:
            return
        pipe = redis_conn.pipeline()
        # rpush accepte plusieurs valeurs en un seul appel
        pipe.rpush(NL_QUEUE_KEY, *batch)
        pipe.execute()
        batch = []

    for f in files:
        filename = (f.filename or "").lower()
        content = f.read()

        if filename.endswith(".xlsx"):
            headers, rows = _parse_xlsx(content)
        elif filename.endswith(".csv"):
            headers, rows = _parse_csv(content)
        else:
            raise ValueError(f"Format non supporté pour '{f.filename}' (xlsx/csv uniquement)")

        if not headers:
            continue

        if seen_headers is None:
            seen_headers = headers
            chosen_number_col = _detect_number_col(headers)

        for h in headers:
            if h != chosen_number_col:
                variables.add(h)

        for r in rows:
            raw_num = r.get(chosen_number_col)
            number = _normalize_number(raw_num)
            if not number:
                continue

            contact = {"number": number}
            for k, v in r.items():
                if k == chosen_number_col:
                    continue
                kk = _clean_header(k)
                if not kk:
                    continue
                contact[kk] = "" if v is None else str(v).strip()

            batch.append(json.dumps(contact, ensure_ascii=False))
            total_added += 1

            if len(batch) >= BATCH_SIZE:
                flush()

    flush()

    new_total = int(_meta_imported_total()) + total_added
    meta = {
        "number_col":      chosen_number_col or "number",
        "variables":       json.dumps(sorted(list(variables)), ensure_ascii=False),
        "imported_total":  str(new_total),
        "updated_ts":      str(_now()),
    }
    redis_conn.hset(NL_META_KEY, mapping=meta)

    return {
        "added":          total_added,
        "imported_total": new_total,
        "variables":      sorted(list(variables)),
        "number_col":     chosen_number_col or "number",
    }


def load_nl_meta() -> dict | None:
    m = redis_conn.hgetall(NL_META_KEY) or {}
    if not m:
        return None
    out = {}
    for k, v in m.items():
        kk = k.decode("utf-8", errors="ignore")
        vv = v.decode("utf-8", errors="ignore")
        out[kk] = vv
    try:
        out["variables"] = json.loads(out.get("variables") or "[]")
    except Exception:
        out["variables"] = []
    try:
        out["imported_total"] = int(out.get("imported_total") or 0)
    except Exception:
        out["imported_total"] = 0
    return out


def nl_remaining_count() -> int:
    try:
        return int(redis_conn.llen(NL_QUEUE_KEY) or 0)
    except Exception:
        return 0


def clear_numlist():
    redis_conn.delete(NL_QUEUE_KEY)
    redis_conn.delete(NL_META_KEY)


def save_message_draft(message: str, msg_type: str):
    msg_type = (msg_type or "sms").strip().lower()
    if msg_type not in ("sms", "mms"):
        msg_type = "sms"
    redis_conn.hset(NL_DRAFT_KEY, mapping={
        "message":    str(message or ""),
        "type":       msg_type,
        "updated_ts": str(_now()),
    })


def load_message_draft() -> tuple[str, str]:
    try:
        m = redis_conn.hgetall(NL_DRAFT_KEY) or {}
        msg = (m.get(b"message") or b"").decode("utf-8", errors="ignore")
        typ = (m.get(b"type") or b"sms").decode("utf-8", errors="ignore")
        typ = typ if typ in ("sms", "mms") else "sms"
        return msg, typ
    except Exception:
        return "", "sms"
