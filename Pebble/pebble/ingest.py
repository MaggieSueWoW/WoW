from __future__ import annotations
from typing import Any, List, Optional
from datetime import datetime
from pymongo import UpdateOne
import re
import logging
from .sheets_client import SheetsClient
from .config_loader import Settings, load_settings
from .mongo_client import get_db, ensure_indexes
from .wcl_client import WCLClient
from .utils.time import (
    night_id_from_ms,
    ms_to_pt_iso,
    ms_to_pt_sheets,
    PT,
    pt_time_to_ms,
    sheets_date_str,
)
from .utils.sheets import update_last_processed

logger = logging.getLogger(__name__)


REPORT_HEADERS = {
    "Report URL": "report_url",
    "Status": "status",
    "Last Checked (PT)": "last_checked_pt",
    "Notes": "notes",
    "Break Override Start (PT)": "break_override_start",
    "Break Override End (PT)": "break_override_end",
    "Report Name": "report_name",
    "Report Start (PT)": "report_start_pt",
    "Report End (PT)": "report_end_pt",
    "Created By": "created_by",
}


ABS_MS_THRESHOLD = 10**12  # heuristic: anything below this is treated as relative ms


def _extract_code_from_url(url: str | None) -> Optional[str]:
    if not url:
        return None
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not host.endswith("warcraftlogs.com"):
            return None
        part = parsed.path.split("/reports/")[1]
        code = part.split("/")[0].split("?")[0].split("#")[0]
        return code or None
    except Exception:
        return None


def _sheet_values(
    s: Settings, tab: str, start: str = "A5", last_processed: str = "B3"
) -> List[List[Any]]:
    client = SheetsClient(s.service_account_json)
    svc = client.svc
    rng = f"{tab}!{start}:Z"
    values = (
        client.execute(
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=s.sheets.spreadsheet_id, range=rng)
        ).get("values", [])
    )
    update_last_processed(
        s.sheets.spreadsheet_id, tab, s.service_account_json, last_processed, client
    )
    return values


def _normalize_fight_times(
    report_start_ms: int, fight_start: int, fight_end: int
) -> tuple[int, int, int, int]:
    """Return (rel_start, rel_end, abs_start, abs_end) in ms.
    WCL GraphQL fights are *usually* relative to report start; use a robust heuristic.
    """
    fs, fe = int(fight_start or 0), int(fight_end or 0)
    if fs < ABS_MS_THRESHOLD and fe < ABS_MS_THRESHOLD:
        rel_start, rel_end = fs, fe
        abs_start, abs_end = report_start_ms + fs, report_start_ms + fe
    else:
        # appears absolute already
        abs_start, abs_end = fs, fe
        rel_start, rel_end = max(0, fs - report_start_ms), max(0, fe - report_start_ms)
    return rel_start, rel_end, abs_start, abs_end


def ingest_roster(s: Settings | None = None) -> int:
    """Ingest the Team Roster sheet into the ``team_roster`` collection."""

    s = s or load_settings()
    db = get_db(s)
    ensure_indexes(db)

    rows = _sheet_values(
        s,
        s.sheets.tabs.team_roster,
        s.sheets.starts.team_roster,
        s.sheets.last_processed.team_roster,
    )
    if not rows:
        db["team_roster"].delete_many({})
        return 0

    header = rows[0]
    try:
        m_idx = header.index("Main")
        j_idx = header.index("Join Date")
        l_idx = header.index("Leave Date")
        a_idx = header.index("Active?")
    except ValueError:
        db["team_roster"].delete_many({})
        return 0

    docs = []
    for r in rows[1:]:
        main = r[m_idx].strip() if m_idx < len(r) else ""
        if not main:
            continue
        join = sheets_date_str(r[j_idx].strip() if j_idx < len(r) else "")
        leave = sheets_date_str(r[l_idx].strip() if l_idx < len(r) else "")
        aval = r[a_idx].strip().lower() if a_idx < len(r) else ""
        active = aval not in ("n", "no", "false", "0", "f")
        docs.append(
            {
                "main": main,
                "join_night": join,
                "leave_night": leave,
                "active": active,
            }
        )

    db["team_roster"].delete_many({})
    inserted = 0
    if docs:
        res = db["team_roster"].insert_many(docs)
        inserted = len(res.inserted_ids)
    return inserted


def canonical_fight_key(
    fight: dict, abs_start_ms: int, abs_end_ms: int
) -> dict:
    """Return canonical key for a fight.

    The key is independent of the report code so that the same pull logged in
    multiple reports maps to a single document in ``fights_all``.  Start and end
    times are rounded to the nearest 100 ms to absorb minor timestamp drift
    while avoiding collisions for distinct fights that occur close together.
    """

    def _round_ms(ms: int) -> int:
        """Round ``ms`` to the nearest 100 ms."""
        return int(round(ms / 100.0) * 100)

    return {
        "encounter_id": int(fight.get("encounterID") or 0),
        "difficulty": int(fight.get("difficulty") or 0),
        "start_rounded_ms": _round_ms(abs_start_ms),
        "end_rounded_ms": _round_ms(abs_end_ms),
    }


def _col_to_index(col: str) -> int:
    """Return zero-based column index for ``col``.

    ``col`` is expected to be in A1 notation (e.g. ``"B"`` or ``"AA"``).
    """
    idx = 0
    for c in col.upper():
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def _index_to_col(idx: int) -> str:
    """Return column letter(s) for zero-based ``idx``.

    This is the inverse of :func:`_col_to_index`.
    """
    idx += 1
    col = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        col = chr(ord("A") + rem) + col
    return col


def ingest_reports(s: Settings | None = None) -> dict:
    s = s or load_settings()
    db = get_db(s)
    ensure_indexes(db)

    client = SheetsClient(s.service_account_json)
    svc = client.svc
    start = s.sheets.starts.reports
    rng = f"{s.sheets.tabs.reports}!{start}:Z"
    rows = (
        client.execute(
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=s.sheets.spreadsheet_id, range=rng)
        ).get("values", [])
    )
    update_last_processed(
        s.sheets.spreadsheet_id,
        s.sheets.tabs.reports,
        s.service_account_json,
        s.sheets.last_processed.reports,
        client,
    )
    if not rows:
        return {"reports": 0, "fights": 0}

    header = rows[0]
    colmap = {name: header.index(name) for name in REPORT_HEADERS if name in header}
    last_checked_idx = colmap.get("Last Checked (PT)")
    report_name_idx = colmap.get("Report Name")
    report_start_idx = colmap.get("Report Start (PT)")
    report_end_idx = colmap.get("Report End (PT)")
    created_by_idx = colmap.get("Created By")
    status_idx = colmap.get("Status")

    # Determine starting row and column for the sheet range
    start_row_match = re.search(r"\d+", start)
    start_row = int(start_row_match.group()) if start_row_match else 1
    start_col_match = re.match(r"[A-Za-z]+", start)
    start_col_idx = _col_to_index(start_col_match.group()) if start_col_match else 0

    def _col_letter(idx: int) -> str:
        return _index_to_col(start_col_idx + idx)

    # Collect targets
    updates: List[dict] = []
    targets: List[dict] = []
    for r_index, row in enumerate(rows[1:], start=start_row + 1):

        def val(col: str) -> str:
            idx = colmap.get(col)
            return row[idx] if idx is not None and idx < len(row) else ""

        status = val("Status").strip().lower()
        if status not in ("", "in-progress", "in‑progress", "in progress"):
            continue
        url = val("Report URL").strip()
        code = _extract_code_from_url(url)
        if not code:
            if url:
                logger.warning("Bad report link at row %s: %s", r_index, url)
                if status_idx is not None:
                    col_letter = _col_letter(status_idx)
                    rng = f"{s.sheets.tabs.reports}!{col_letter}{r_index}"
                    updates.append({"range": rng, "values": [["Bad report link"]]})
            continue
        targets.append(
            {
                "row": r_index,
                "code": code,
                "notes": val("Notes"),
                "break_override_start": val("Break Override Start (PT)"),
                "break_override_end": val("Break Override End (PT)"),
            }
        )

    if not targets:
        if updates:
            client.execute(
                svc.spreadsheets().values().batchUpdate(
                    spreadsheetId=s.sheets.spreadsheet_id,
                    body={"valueInputOption": "RAW", "data": updates},
                )
            )
        return {"reports": 0, "fights": 0}

    wcl = WCLClient(
        s.wcl.client_id,
        s.wcl.client_secret,
        base_url=s.wcl.base_url,
        token_url=s.wcl.token_url,
        redis_url=s.redis.url,
        cache_prefix=s.redis.key_prefix,
    )

    total_fights = 0
    processed_reports = 0
    for rep in targets:
        code = rep["code"]
        try:
            bundle = wcl.fetch_report_bundle(code)
        except Exception:
            logger.warning(
                "Failed to fetch WCL report bundle", extra={"code": code}, exc_info=True
            )
            if status_idx is not None:
                col_letter = _col_letter(status_idx)
                rng = f"{s.sheets.tabs.reports}!{col_letter}{rep['row']}"
                updates.append({"range": rng, "values": [["Bad report link"]]})
            continue
        processed_reports += 1

        # reports upsert
        report_start_ms = int(bundle.get("startTime"))
        report_end_ms = int(bundle.get("endTime"))
        night_id = night_id_from_ms(report_start_ms)
        bos_ms = pt_time_to_ms(rep.get("break_override_start"), report_start_ms)
        boe_ms = pt_time_to_ms(rep.get("break_override_end"), report_start_ms)
        now_dt = datetime.now(PT)
        now_ms = int(now_dt.timestamp() * 1000)
        now_iso = ms_to_pt_iso(now_ms)
        now_sheet = ms_to_pt_sheets(now_ms)
        end_iso = ms_to_pt_iso(report_end_ms) if report_end_ms > 0 else ""
        end_sheet = ms_to_pt_sheets(report_end_ms) if report_end_ms > 0 else ""
        rep_doc = {
            "code": code,
            "title": bundle.get("title"),
            "start_ms": report_start_ms,
            "end_ms": report_end_ms,
            "start_pt": ms_to_pt_iso(report_start_ms),
            "end_pt": end_iso,
            "owner": (bundle.get("owner") or {}).get("name", ""),
            "night_id": night_id,
            "notes": rep.get("notes", ""),
            "break_override_start_ms": bos_ms,
            "break_override_end_ms": boe_ms,
            "break_override_start_pt": (
                ms_to_pt_iso(bos_ms) if bos_ms is not None else ""
            ),
            "break_override_end_pt": ms_to_pt_iso(boe_ms) if boe_ms is not None else "",
            "ingested_at": now_dt,
            "last_checked_pt": now_iso,
        }
        db["reports"].update_one({"code": code}, {"$set": rep_doc}, upsert=True)

        def _update(idx: int | None, value: str):
            if idx is None:
                return
            col_letter = _col_letter(idx)
            rng = f"{s.sheets.tabs.reports}!{col_letter}{rep['row']}"
            updates.append({"range": rng, "values": [[value]]})

        _update(last_checked_idx, now_sheet)
        _update(report_name_idx, bundle.get("title", ""))
        _update(report_start_idx, ms_to_pt_sheets(report_start_ms))
        _update(report_end_idx, end_sheet)
        _update(created_by_idx, (bundle.get("owner") or {}).get("name", ""))

        # actors (players) per report — small, useful for audits; dedup by (report_code, actor_id)
        actors = (bundle.get("masterData") or {}).get("actors") or []
        actor_map = {
            int(a.get("id")): {
                "actor_id": int(a.get("id")),
                "name": (
                    f"{a.get('name')}-{a.get('server')}"
                    if a.get("server")
                    else a.get("name")
                ),
                "type": a.get("type"),
                "subType": a.get("subType"),
                "server": a.get("server"),
            }
            for a in actors
        }
        if actor_map:
            ops = []
            for aid, a in actor_map.items():
                key = {"report_code": code, "actor_id": aid}
                ops.append(UpdateOne(key, {"$set": {**key, **a}}, upsert=True))
            if ops:
                db["actors"].bulk_write(ops, ordered=False)

        # fights (single unified collection persisted to ``fights_all``)
        fights = bundle.get("fights", []) or []
        fops = []
        for f in fights:
            rel_s, rel_e, abs_s, abs_e = _normalize_fight_times(
                report_start_ms, f.get("startTime"), f.get("endTime")
            )
            participants = []
            for pid in f.get("friendlyPlayers") or []:
                a = actor_map.get(int(pid))
                if not a:
                    continue
                if str(a.get("type", "")).lower() != "player":
                    continue
                participants.append(
                    {
                        "actor_id": a["actor_id"],
                        "name": a.get("name"),
                        "class": a.get("subType"),  # WoW class
                        "server": a.get("server"),
                    }
                )

            key = canonical_fight_key(f, abs_s, abs_e)
            base = {
                **key,
                "report_code": code,
                "id": int(f.get("id")),
                "night_id": night_id,
                "name": f.get("name"),
                "is_mythic": int(f.get("difficulty") or 0) == 5,
                "kill": bool(f.get("kill")),
                # times
                "report_start_ms": report_start_ms,
                "report_start_pt": ms_to_pt_iso(report_start_ms),
                "fight_rel_start_ms": rel_s,
                "fight_rel_end_ms": rel_e,
                "fight_abs_start_ms": abs_s,
                "fight_abs_start_pt": ms_to_pt_iso(abs_s),
                "fight_abs_end_ms": abs_e,
                "fight_abs_end_pt": ms_to_pt_iso(abs_e),
            }
            # Use $setOnInsert so the first observed report for a given fight
            # establishes the document; subsequent overlapping reports only add
            # participants but do not clobber the original report metadata.
            fops.append(
                UpdateOne(
                    key,
                    {
                        "$setOnInsert": base,
                        "$addToSet": {"participants": {"$each": participants}},
                    },
                    upsert=True,
                )
            )
        if fops:
            db["fights_all"].bulk_write(fops, ordered=False)
        total_fights += len(fights)

    if updates:
        client.execute(
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=s.sheets.spreadsheet_id,
                body={"valueInputOption": "RAW", "data": updates},
            )
        )

    return {"reports": processed_reports, "fights": total_fights}
