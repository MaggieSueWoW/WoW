from __future__ import annotations
import sys
import yaml
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from sheets_io import (
    open_sheet, ws_by_name, read_all, rows_to_dicts, upsert_rows,
    now_pt_iso
)

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("sprint3")

# ---------- Helpers ----------
def parse_local_time(s: str) -> time:
    hh, mm = [int(x) for x in s.strip().split(":")]
    return time(hh, mm)

def weekday_index(name: str) -> int:
    # Monday=0 ... Sunday=6
    name = (name or "").strip().lower()
    table = {"monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4,"saturday":5,"sunday":6}
    if name not in table:
        raise ValueError(f"Invalid day: {name}")
    return table[name]

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x))
    except Exception:
        return default

def dt_from_night_id(nid: str, tz: str) -> datetime:
    # Night ID is "YYYY-MM-DD" in PT by construction (Sprint 2)
    d = date.fromisoformat(nid)
    # use 20:00 local to avoid any edge with week boundary at 07:00
    return datetime(d.year, d.month, d.day, 20, 0, tzinfo=ZoneInfo(tz))

def week_anchor_for_dt(local_dt: datetime, tz: str, anchor_day: str, anchor_time: str) -> date:
    """
    Returns the date (YYYY-MM-DD) of the most recent 'anchor_day' at 'anchor_time' local,
    whose datetime is <= local_dt.
    """
    target_wd = weekday_index(anchor_day)
    t = parse_local_time(anchor_time)
    # Start from local_dt's date; if it's the anchor weekday but before anchor time, step back a week
    base = local_dt
    # Move back to the weekday
    days_back = (base.weekday() - target_wd) % 7
    candidate = datetime(base.year, base.month, base.day, t.hour, t.minute, tzinfo=base.tzinfo) - timedelta(days=days_back)
    if local_dt < candidate:
        candidate -= timedelta(days=7)
    return candidate.date()

# ---------- Data classes ----------
@dataclass
class NightTotal:
    night_id: str
    main: str
    minutes_total: float

# ---------- Sprint 3 main ----------
def main():
    if len(sys.argv) < 2:
        print("Usage: python sprint3.py config/config.yaml")
        sys.exit(2)

    load_dotenv()
    cfg_path = sys.argv[1]
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    tz = cfg["app"]["timezone"]
    sheet_id = cfg["app"]["sheet_id"]
    sheets_cfg = cfg["app"]["sheets"]

    week_anchor_day = cfg["app"]["week_reset"]["day_of_week"]         # e.g., "Tuesday"
    week_anchor_time = cfg["app"]["week_reset"]["time_local"]          # e.g., "07:00"

    ss = open_sheet(cfg["google"]["service_account_json_path"], sheet_id)

    ws_control   = ws_by_name(ss, sheets_cfg["control"])
    ws_roster    = ws_by_name(ss, sheets_cfg["roster_map"])
    ws_ntotals   = ws_by_name(ss, sheets_cfg["night_totals"])
    ws_wtotals   = ws_by_name(ss, sheets_cfg["week_totals"])
    ws_service   = ws_by_name(ss, sheets_cfg["service_log"])

    # --- Load Roster Map (optional Role lookup) ---
    rm_headers, rm_rows = read_all(ws_roster)
    roster = rows_to_dicts(rm_headers, rm_rows)
    main_role: Dict[str, str] = {}
    for rr in roster:
        main = (rr.get("Main (Name-Realm)") or "").strip()
        role = (rr.get("Role") or "").strip()
        if main and role:
            main_role[main] = role

    # --- Load Night Totals ---
    nt_headers, nt_rows = read_all(ws_ntotals)
    if not nt_headers:
        raise RuntimeError("Night Totals sheet missing headers.")
    nt_list = rows_to_dicts(nt_headers, nt_rows)

    nights: List[NightTotal] = []
    for r in nt_list:
        nid = (r.get("Night ID") or "").strip()
        main = (r.get("Main") or "").strip()
        if not nid or not main:
            continue
        minutes = safe_float(r.get("Minutes (Total)"), 0.0)
        nights.append(NightTotal(nid, main, minutes))

    LOG.info("Loaded %d Night Totals rows", len(nights))

    # --- Group by (week, main) and sum minutes ---
    week_sum: Dict[Tuple[date, str], float] = {}
    tzinfo = ZoneInfo(tz)
    for nt in nights:
        local_dt = dt_from_night_id(nt.night_id, tz)
        wk = week_anchor_for_dt(local_dt, tz, week_anchor_day, week_anchor_time)
        key = (wk, nt.main)
        week_sum[key] = week_sum.get(key, 0.0) + nt.minutes_total

    # --- Compute season-to-date cumulative per main (ascending by week) ---
    # 1) collect unique weeks sorted
    all_weeks = sorted({wk for (wk, _m) in week_sum.keys()})
    # 2) accumulate
    s2d_by_week_main: Dict[Tuple[date, str], float] = {}
    totals_per_main: Dict[str, float] = {}
    for wk in all_weeks:
        mains_this_week = [m for (w, m) in week_sum.keys() if w == wk]
        for m in sorted(mains_this_week):
            week_minutes = week_sum[(wk, m)]
            running = totals_per_main.get(m, 0.0) + week_minutes
            totals_per_main[m] = running
            s2d_by_week_main[(wk, m)] = running

    # --- Build final Week Totals rows (rank by season-to-date; least first) ---
    week_rows: List[Dict[str, Any]] = []
    for wk in all_weeks:
        # Prepare ranking data for this week
        entries: List[Tuple[str, float, float]] = []  # (main, week_min, s2d)
        mains_this_week = [m for (w, m) in week_sum.keys() if w == wk]
        for m in mains_this_week:
            week_min = week_sum[(wk, m)]
            s2d = s2d_by_week_main[(wk, m)]
            entries.append((m, week_min, s2d))

        # ✅ Rank by Season-to-date minutes (ascending)
        # Tiebreak 1: current week's minutes (ascending)
        # Tiebreak 2: name (A→Z) to keep deterministic
        entries.sort(key=lambda x: (x[2], x[1], x[0].lower()))

        rank = 1
        for m, week_min, s2d in entries:
            week_rows.append({
                "Game Week (Tuesday PT)": wk.isoformat(),
                "Main": m,
                "Minutes (Week)": f"{week_min:.2f}",
                "Minutes (Season-to-date)": f"{s2d:.2f}",
                "Rank (Least time first)": str(rank),
                "Role": main_role.get(m, ""),
                "Nights Count": "1",  # keep simple for 1-night team; adjust later if you add nights
            })
            rank += 1

    # --- UPSERT into Week Totals ---
    wt_headers, _ = read_all(ws_wtotals)
    if not wt_headers:
        raise RuntimeError("Week Totals sheet missing headers.")

    ins_w, upd_w = upsert_rows(
        ws_wtotals,
        wt_headers,
        week_rows,
        ["Game Week (Tuesday PT)", "Main"]
    )
    LOG.info("Week Totals upserts: +%d / updated %d", ins_w, upd_w)

    # --- Service log ---
    log(ws_service, tz, "", "", "ROLLUP", f"WeekTotals +{ins_w}/{upd_w}")

def log(ws_service, tz: str, report_code: str, night_id: str, stage: str, message: str, details: Dict[str, Any] | None = None):
    headers, _ = read_all(ws_service)
    if not headers:
        return
    from sheets_io import now_pt_iso  # avoid re-import at top just for clarity
    row = {
        "Timestamp (PT)": now_pt_iso(tz),
        "Level": "info",
        "Report Code": report_code,
        "Night ID": night_id or "",
        "Stage": stage,
        "Message": message,
        "Details JSON": "{}",
    }
    ws_service.append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
