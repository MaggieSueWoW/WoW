from __future__ import annotations
import json
import logging
import time
from typing import Dict, List, Any, Tuple
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

LOGGER = logging.getLogger("sheets")

# ---------- SHEETS UTILS ----------

def open_sheet(service_account_json_path: str, sheet_id: str):
    LOGGER.info("Opening Google Sheet %s", sheet_id)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(service_account_json_path, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def ws_by_name(ss, name: str):
    LOGGER.info("Opening worksheet '%s'", name)
    return ss.worksheet(name)

def read_all(ws) -> Tuple[List[str], List[List[str]]]:
    vals = ws.get_all_values()
    if not vals:
        return [], []
    headers = vals[0]
    rows = vals[1:]
    return headers, rows

def rows_to_dicts(headers: List[str], rows: List[List[str]]) -> List[Dict[str, str]]:
    out = []
    for r in rows:
        d = {}
        for i, h in enumerate(headers):
            d[h] = r[i] if i < len(r) else ""
        out.append(d)
    return out

def dicts_to_rows(headers: List[str], dicts: List[Dict[str, Any]]) -> List[List[str]]:
    out = []
    for d in dicts:
        out.append([str(d.get(h, "")) if d.get(h, "") is not None else "" for h in headers])
    return out

def build_index(headers: List[str], rows: List[List[str]], key_fields: List[str]) -> Dict[Tuple[str, ...], int]:
    """Return mapping from key tuple -> 1-based row number (including header)."""
    idx = {}
    h2i = {h: i for i, h in enumerate(headers)}
    for ridx, r in enumerate(rows, start=2):
        key = tuple((r[h2i[k]] if h2i.get(k) is not None and h2i[k] < len(r) else "") for k in key_fields)
        idx[key] = ridx
    return idx

def group_consecutive(updates: List[Tuple[int, List[str]]]) -> List[List[Tuple[int, List[str]]]]:
    """Group (rownum, rowvals) updates into runs of consecutive row numbers."""
    if not updates:
        return []
    updates = sorted(updates, key=lambda x: x[0])
    runs = []
    cur = [updates[0]]
    for (prev_r, _), (r, row) in zip(updates, updates[1:]):
        if r == prev_r + 1:
            cur.append((r, row))
        else:
            runs.append(cur)
            cur = [(r, row)]
    runs.append(cur)
    return runs

def col_letter(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def upsert_rows(ws, headers: List[str], new_dicts: List[Dict[str, Any]], key_fields: List[str]) -> Tuple[int, int]:
    """
    UPSERT rows into `ws`. Returns (insert_count, update_count).

    Fixes:
    - Deduplicate `new_dicts` by key_fields so we don't schedule multiple updates for the same key.
    - Coalesce updates per row number so each row is written at most once per pass.
    - Apply updates in runs of consecutive rows to avoid range-shape issues.
    """
    existing_vals = ws.get_all_values()
    if not existing_vals:
        LOGGER.info("Empty sheet '%s': writing headers and %d rows", ws.title, len(new_dicts))
        ws.update([headers] + dicts_to_rows(headers, new_dicts))
        return (len(new_dicts), 0)

    exist_headers = existing_vals[0]
    if exist_headers != headers:
        raise ValueError(f"Sheet headers mismatch for {ws.title}.\nExpected: {headers}\nFound:    {exist_headers}")

    existing_rows = existing_vals[1:]
    index = build_index(exist_headers, existing_rows, key_fields)

    # --- 1) Dedup new rows by key (last one wins) ---
    dedup_map: Dict[Tuple[str, ...], List[str]] = {}
    for d in new_dicts:
        key = tuple(str(d.get(k, "")) for k in key_fields)
        row = [str(d.get(h, "")) if d.get(h, "") is not None else "" for h in headers]
        dedup_map[key] = row

    inserts: List[List[str]] = []
    updates_by_row: Dict[int, List[str]] = {}

    # --- 2) Compute inserts vs updates; coalesce per row ---
    for key, new_row in dedup_map.items():
        if key in index:
            rownum = index[key]
            existing_row = existing_rows[rownum - 2]
            if existing_row != new_row:
                updates_by_row[rownum] = new_row
        else:
            inserts.append(new_row)

    # --- 3) Apply updates in runs of consecutive rows ---
    if updates_by_row:
        # Make (rownum, rowvals) sorted list
        updates = sorted(updates_by_row.items(), key=lambda x: x[0])
        runs = group_consecutive(updates)  # type: ignore[arg-type]
        for run in runs:
            start = run[0][0]
            end = run[-1][0]
            rng = f"A{start}:{col_letter(len(headers))}{end}"
            LOGGER.info("Updating %s rows %d–%d", ws.title, start, end)
            ws.update(rng, [r for _, r in run])

    if inserts:
        LOGGER.info("Appending %d rows to %s", len(inserts), ws.title)
        ws.append_rows(inserts, value_input_option="USER_ENTERED")

    if updates_by_row or inserts:
        time.sleep(0.200)  # mmmfixme: tmp

    return (len(inserts), len(updates_by_row))

def now_pt_iso(pt_tz: str) -> str:
    tz = ZoneInfo(pt_tz)
    return datetime.now(tz).isoformat(timespec="seconds")

def ms_to_pt_iso(ms: int, pt_tz: str) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(ZoneInfo(pt_tz))
    return dt.isoformat(timespec="seconds")

def to_int(s: Any, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default
