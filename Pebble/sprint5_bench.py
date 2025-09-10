# sprint5_bench.py
from __future__ import annotations
import sys
import yaml
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from sheets_io import (
    open_sheet,
    ws_by_name,
    read_all,
    rows_to_dicts,
    upsert_rows,
    now_pt_iso,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("sprint5_bench")


# ------------ helpers ------------
def parse_local_time(s: str) -> time:
    hh, mm = [int(x) for x in s.strip().split(":")]
    return time(hh, mm)


def weekday_index(name: str) -> int:
    table = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    n = (name or "").strip().lower()
    if n not in table:
        raise ValueError(f"Invalid day: {name}")
    return table[n]


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x))
    except Exception:
        return default


def dt_from_night_id(nid: str, tz: str) -> datetime:
    d = date.fromisoformat(nid)
    return datetime(d.year, d.month, d.day, 20, 0, tzinfo=ZoneInfo(tz))  # 8pm anchor


def week_anchor_for_dt(
    local_dt: datetime, tz: str, anchor_day: str, anchor_time: str
) -> date:
    target_wd = weekday_index(anchor_day)
    t = parse_local_time(anchor_time)
    base = local_dt
    days_back = (base.weekday() - target_wd) % 7
    candidate = datetime(
        base.year, base.month, base.day, t.hour, t.minute, tzinfo=base.tzinfo
    ) - timedelta(days=days_back)
    if local_dt < candidate:
        candidate -= timedelta(days=7)
    return candidate.date()


# ------------ main ------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python sprint5_bench.py config/config.yaml")
        sys.exit(2)

    load_dotenv()
    cfg_path = sys.argv[1]
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    tz = cfg["app"]["timezone"]
    tzinfo = ZoneInfo(tz)
    sheets = cfg["app"]["sheets"]
    sheet_id = cfg["app"]["sheet_id"]

    week_anchor_day = cfg["app"]["week_reset"]["day_of_week"]
    week_anchor_time = cfg["app"]["week_reset"]["time_local"]

    # Open sheets
    ss = open_sheet(cfg["google"]["service_account_json_path"], sheet_id)
    ws_roster = ws_by_name(ss, sheets.get("roster_map", "Roster Map"))
    ws_team = ws_by_name(ss, sheets.get("team_roster", "Team Roster"))
    ws_over = ws_by_name(
        ss, sheets.get("availability_overrides", "Availability Overrides")
    )
    ws_ntotals = ws_by_name(ss, sheets.get("night_totals", "Night Totals"))
    ws_qa = ws_by_name(ss, sheets.get("night_qa", "Night QA"))
    ws_bnight = ws_by_name(ss, sheets.get("bench_night_totals", "Bench Night Totals"))
    ws_bweek = ws_by_name(ss, sheets.get("bench_week_totals", "Bench Week Totals"))
    ws_service = ws_by_name(ss, sheets.get("service_log", "Service Log"))
    ws_fights = ws_by_name(ss, sheets.get("fights", "Fights"))

    # --- Load Night Totals ---
    nt_headers, nt_rows = read_all(ws_ntotals)
    nt = rows_to_dicts(nt_headers, nt_rows)
    # map: (nid, main) -> played_pre/post/total (minutes)
    played: Dict[Tuple[str, str], Dict[str, float]] = {}
    mains_seen_first_night: Dict[str, str] = {}
    for r in nt:
        nid = (r.get("Night ID") or "").strip()
        main = (r.get("Main") or "").strip()
        if not nid or not main:
            continue
        tot = safe_float(r.get("Minutes (Total)"), 0.0)
        pre = safe_float(r.get("Minutes Pre-Break"), 0.0)
        post = safe_float(r.get("Minutes Post-Break"), 0.0)
        played[(nid, main)] = {"tot": tot, "pre": pre, "post": post}
        mains_seen_first_night.setdefault(main, nid)

    LOG.info(
        "Loaded %d Night Totals rows (%d unique players)",
        len(nt),
        len({m for _, m in played.keys()}),
    )

    # --- Load Night QA (night durations and break) ---
    qa_headers, qa_rows = read_all(ws_qa)
    qa = rows_to_dicts(qa_headers, qa_rows)
    night_dur: Dict[str, Dict[str, float]] = (
        {}
    )  # nid -> {"pre":min,"post":min,"total":min}
    for r in qa:
        nid = (r.get("Night ID") or "").strip()
        if not nid:
            continue
        m_pre = safe_float(r.get("Mythic Pre Duration (min)"), float("nan"))
        m_post = safe_float(r.get("Mythic Post Duration (min)"), float("nan"))

        # ONLY use mythic durations for bench math.
        if (m_pre == m_pre) and (m_post == m_post):  # not NaN
            night_dur[nid] = {"pre": m_pre, "post": m_post, "total": m_pre + m_post}
        else:
            LOG.warning(
                "Night QA missing Mythic durations for Night ID %s; treating as 0.", nid
            )
            night_dur[nid] = {"pre": 0.0, "post": 0.0, "total": 0.0}

    # --- Load Team Roster (membership windows) ---
    team_headers, team_rows = read_all(ws_team)
    team = rows_to_dicts(team_headers, team_rows)
    team_join: Dict[str, Optional[str]] = {}
    team_leave: Dict[str, Optional[str]] = {}
    team_active: Dict[str, bool] = {}
    for r in team:
        m = (r.get("Main") or "").strip()
        if not m:
            continue
        j = (r.get("Join Night (YYYY-MM-DD)") or "").strip()
        l = (r.get("Leave Night (YYYY-MM-DD)") or "").strip()
        a = (r.get("Active?") or "").strip().lower() in ("true", "t", "yes", "y", "1")
        team_join[m] = j if j else None
        team_leave[m] = l if l else None
        team_active[m] = a if r.get("Active?") != "" else True  # default True if blank

    # Inferred first-night join if missing explicit join
    for m, first_nid in mains_seen_first_night.items():
        if team_join.get(m) in (
            None,
            "",
        ):
            team_join[m] = first_nid

    # --- Load Availability Overrides ---
    ov_headers, ov_rows = read_all(ws_over)
    ov = rows_to_dicts(ov_headers, ov_rows)
    overrides: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in ov:
        nid = (r.get("Night ID") or "").strip()
        m = (r.get("Main") or "").strip()
        if not nid or not m:
            continue
        overrides[(nid, m)] = {
            "status": (r.get("Status") or "").strip(),  # Available | Out | Break
            "pre": (r.get("Avail Pre?") or "").strip().lower(),
            "post": (r.get("Avail Post?") or "").strip().lower(),
            "reason": r.get("Reason") or "",
        }

    # --- Load Role (for Bench Week Totals) from Roster Map ---
    rm_headers, rm_rows = read_all(ws_roster)
    roster = rows_to_dicts(rm_headers, rm_rows)
    main_role: Dict[str, str] = {}
    for rr in roster:
        main = (rr.get("Main (Name-Realm)") or "").strip()
        role = (rr.get("Role") or "").strip()
        if main and role:
            main_role[main] = role
    char_to_main: Dict[str, str] = {}
    for rr in roster:
        ch = (rr.get("Character (Name-Realm)") or "").strip()
        mn = (rr.get("Main (Name-Realm)") or "").strip()
        if ch:
            char_to_main[ch] = mn if mn else ch

    # Build map: Night ID -> (first mythic start PT, reports involved)
    qa_by_nid: Dict[str, Dict[str, Any]] = {}
    for r in qa:
        nid = (r.get("Night ID") or "").strip()
        if not nid:
            continue
        first_mythic_iso = (r.get("Mythic Start (PT)") or "").strip()
        qa_by_nid[nid] = {
            "night_start_pt": (
                first_mythic_iso or r.get("Night Start (PT)") or ""
            ).strip(),
            "reports": [
                x.strip()
                for x in (r.get("Reports Involved") or "").split(",")
                if x.strip()
            ],
        }

    # Load fights table once
    f_headers, f_rows = read_all(ws_fights)
    fights_tbl = rows_to_dicts(f_headers, f_rows)

    def iso_to_dt_local(s: str, tz: str) -> Optional[datetime]:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=ZoneInfo(tz))
        except Exception:
            return None

    # For each night, find the last Heroic boss (difficulty==4, encounter>0) that ends <= first Mythic start
    last_heroic_roster_mains: Dict[str, set[str]] = {}
    for nid, meta in qa_by_nid.items():
        start_iso = meta["night_start_pt"]
        night_start_dt = iso_to_dt_local(start_iso, tz)
        if not night_start_dt:
            continue
        night_start_ms = int(night_start_dt.timestamp() * 1000)
        reports = set(meta["reports"])

        # filter candidate heroic fights
        candidates = []
        for fr in fights_tbl:
            try:
                if (fr.get("Report Code") or "") not in reports:
                    continue
                if int(fr.get("Difficulty") or 0) != 4:  # Heroic only
                    continue
                if int(fr.get("Encounter ID") or 0) <= 0:
                    continue
                end_ms = int(fr.get("End (UTC ms)") or 0)
                if end_ms <= night_start_ms:
                    candidates.append(fr)
            except Exception:
                continue

        if not candidates:
            continue

        # pick the latest one
        last = max(candidates, key=lambda x: int(x.get("End (UTC ms)") or 0))

        # read roster characters and map to mains
        chars_csv = last.get("Roster (Characters CSV)") or ""
        if not chars_csv.strip():
            continue  # column not present or empty → skip
        mains = set()
        for ch in [c.strip() for c in chars_csv.split(",") if c.strip()]:
            mains.add(char_to_main.get(ch, ch))
        if mains:
            last_heroic_roster_mains[nid] = mains

    # Build a map of mains who played by night (from Night Totals)
    played_mains_by_night: Dict[str, set[str]] = {}
    for (nid_p, main_p), _vals in played.items():
        played_mains_by_night.setdefault(nid_p, set()).add(main_p)

    # For each night, we will consider all mains who either played or were present for the last-heroic-before-mythic roster
    all_mains_for_night: Dict[str, set[str]] = {}
    for nid in night_dur.keys():
        all_mains_for_night[nid] = set()
        all_mains_for_night[nid].update(played_mains_by_night.get(nid, set()))
        all_mains_for_night[nid].update(last_heroic_roster_mains.get(nid, set()))

    for nid in sorted(all_mains_for_night.keys()):
        LOG.info(
            "Bench calc Night %s: mythic_dur(pre=%.2f, post=%.2f), candidates=%d (played=%d, last_heroic=%d)",
            nid,
            night_dur.get(nid, {}).get("pre", 0.0),
            night_dur.get(nid, {}).get("post", 0.0),
            len(all_mains_for_night[nid]),
            len(played_mains_by_night.get(nid, set())),
            len(last_heroic_roster_mains.get(nid, set())),
        )

    # --- Compute Bench Night Totals ---
    bnight_rows: List[Dict[str, Any]] = []
    nights_avail_count: Dict[Tuple[str, str], bool] = {}

    for nid in sorted(all_mains_for_night.keys()):
        nd = night_dur.get(nid)
        if not nd:
            LOG.warning("Night QA has no durations for Night ID %s; skipping all bench rows for this night", nid)
            continue

        night_pre = nd["pre"]
        night_post = nd["post"]

        for main in sorted(all_mains_for_night[nid]):
            # Respect roster membership windows / Active?
            # (We keep existing membership logic)
            def within_membership(m: str, night_id: str) -> bool:
                # reuse team_join/team_leave/team_active computed earlier
                active = team_active.get(m, True)
                if not active:
                    # If inactive and leave date exists, disallow >= leave
                    lw = team_leave.get(m)
                    if lw and night_id >= lw:
                        return False
                j = team_join.get(m)
                l = team_leave.get(m)
                if j and night_id < j:
                    return False
                if l and night_id > l:
                    return False
                return True

            if not within_membership(main, nid):
                continue

            p = played.get((nid, main), {"tot": 0.0, "pre": 0.0, "post": 0.0})
            played_tot = p["tot"]
            played_pre = p["pre"]
            played_post = p["post"]

            # Default availability
            # If they played any Mythic that night -> available by halves they actually played.
            # If they did NOT play any Mythic but WERE in the last-heroic roster -> available for BOTH halves (they sat Mythic).
            in_last_heroic = main in last_heroic_roster_mains.get(nid, set())
            status_src = "inferred"
            if played_tot > 0.0:
                avail_pre = played_pre > 0.0
                avail_post = played_post > 0.0
            else:
                if in_last_heroic:
                    avail_pre = True
                    avail_post = True
                else:
                    avail_pre = False
                    avail_post = False

            # Apply explicit overrides
            ov_row = overrides.get((nid, main))
            if ov_row:
                st = ov_row["status"].lower()
                pre_flag = ov_row["pre"]
                post_flag = ov_row["post"]

                if st in ("out", "break"):
                    avail_pre = False
                    avail_post = False
                    status_src = f"override:{'Break' if st == 'break' else 'Out'}"
                elif st == "available":
                    status_src = "override:Available"

                if pre_flag in ("true", "t", "yes", "y", "1"):
                    avail_pre = True
                elif pre_flag in ("false", "f", "no", "n", "0"):
                    avail_pre = False

                if post_flag in ("true", "t", "yes", "y", "1"):
                    avail_post = True
                elif post_flag in ("false", "f", "no", "n", "0"):
                    avail_post = False

            # Bench math (Mythic only)
            bench_pre = max(0.0, night_pre - played_pre) if avail_pre else 0.0
            bench_post = max(0.0, night_post - played_post) if avail_post else 0.0
            bench_tot = bench_pre + bench_post

            nights_avail_count[(nid, main)] = (avail_pre or avail_post)

            bnight_rows.append(
                {
                    "Night ID": nid,
                    "Main": main,
                    "Bench Minutes (Total)": f"{bench_tot:.2f}",
                    "Bench Minutes Pre": f"{bench_pre:.2f}",
                    "Bench Minutes Post": f"{bench_post:.2f}",
                    "Played Minutes (Total)": f"{played_tot:.2f}",
                    "Played Pre": f"{played_pre:.2f}",
                    "Played Post": f"{played_post:.2f}",
                    "Avail Pre?": "TRUE" if avail_pre else "FALSE",
                    "Avail Post?": "TRUE" if avail_post else "FALSE",
                    "Status Source": status_src,
                    "Notes": "",
                }
            )

    # Upsert Bench Night Totals (key: Night ID + Main)
    bnh_headers, _ = read_all(ws_bnight)
    if not bnh_headers:
        raise RuntimeError("Bench Night Totals sheet missing headers.")
    ins_bn, upd_bn = upsert_rows(
        ws_bnight, bnh_headers, bnight_rows, ["Night ID", "Main"]
    )
    LOG.info("Bench Night Totals upserts: +%d / updated %d", ins_bn, upd_bn)

    # --- Bench Week Totals: include EVERY active roster member each week in membership window ---

    # Build week_sum of bench by (week, main) from bnight_rows
    week_sum: Dict[Tuple[date, str], float] = {}
    avail_nights_per_week: Dict[Tuple[date, str], int] = {}

    # We also need Night ID -> week mapping for availability counts
    def night_to_week(nid: str) -> date:
        local_dt = dt_from_night_id(nid, tz)
        return week_anchor_for_dt(local_dt, tz, week_anchor_day, week_anchor_time)

    for r in bnight_rows:
        nid = r["Night ID"]
        m = r["Main"]
        wk = night_to_week(nid)
        bench = safe_float(r.get("Bench Minutes (Total)"), 0.0)
        key = (wk, m)
        week_sum[key] = week_sum.get(key, 0.0) + bench

    # Availability nights per week (informational)
    for (nid, m), avail in nights_avail_count.items():
        if avail:
            wk = night_to_week(nid)
            k = (wk, m)
            avail_nights_per_week[k] = avail_nights_per_week.get(k, 0) + 1

    # Determine membership weeks per main
    # join_week inferred if join night known (explicit or first seen). If still unknown, main won't be shown until they play or join is added.
    join_week: Dict[str, Optional[date]] = {}
    leave_week: Dict[str, Optional[date]] = {}

    for m, j in team_join.items():
        if j:
            join_week[m] = week_anchor_for_dt(
                dt_from_night_id(j, tz), tz, week_anchor_day, week_anchor_time
            )
        else:
            join_week[m] = None
    for m, l in team_leave.items():
        if l:
            leave_week[m] = week_anchor_for_dt(
                dt_from_night_id(l, tz), tz, week_anchor_day, week_anchor_time
            )
        else:
            leave_week[m] = None

    # --- Universe of weeks = ONLY weeks where we have logs ---
    # Prefer Night QA (covers every Night ID with a mythic boss timeline).
    observed_weeks = set()

    def night_to_week(nid: str) -> date:
        local_dt = dt_from_night_id(nid, tz)
        return week_anchor_for_dt(local_dt, tz, week_anchor_day, week_anchor_time)

    for nid in night_dur.keys():
        observed_weeks.add(night_to_week(nid))

    # Fallback: if QA is empty for some reason, use the weeks present in bench rows
    if not observed_weeks:
        observed_weeks = {w for (w, _m) in week_sum.keys()}

    all_weeks: List[date] = sorted(observed_weeks)

    # Prepare list of roster mains that we consider for inclusion
    roster_mains = {
        (r.get("Main") or "").strip() for r in team if (r.get("Main") or "").strip()
    }
    # Ensure mains that appeared in Night Totals (but not in Team Roster) are still included
    roster_mains.update({m for (_nid, m) in played.keys()})

    # Season-to-date cumulative across all weeks, even when week bench is 0
    s2d_by_week_main: Dict[Tuple[date, str], float] = {}
    totals_per_main: Dict[str, float] = {}

    # Build final weekly rows
    bweek_rows: List[Dict[str, Any]] = []

    for wk in all_weeks:
        # mains present this week = everyone on roster whose membership includes this week
        mains_this_week: List[str] = []
        for m in sorted(roster_mains):
            # respect Active?/leave bounds
            active = team_active.get(m, True)
            if not active:
                # If inactive and a leave week exists, exclude weeks >= leave
                lw = leave_week.get(m)
                if lw and wk >= lw:
                    continue
            jw = join_week.get(m)
            lw = leave_week.get(m)
            if jw is None:
                # no known join yet; only include if they already played sometime (we'll have a jw from mains_seen first night)
                # if still None, skip until officers set a join night
                continue
            if wk < jw:
                continue
            if lw and wk > lw:
                continue
            mains_this_week.append(m)

        # For every main included this week, compute week bench + s2d and write a row
        entries: List[Tuple[str, float, float, int]] = (
            []
        )  # (main, week_bench, s2d_bench, avail_count)
        for m in mains_this_week:
            week_bench = week_sum.get((wk, m), 0.0)
            running = totals_per_main.get(m, 0.0) + week_bench
            totals_per_main[m] = running
            s2d_by_week_main[(wk, m)] = running
            avail_n = avail_nights_per_week.get((wk, m), 0)
            entries.append((m, week_bench, running, avail_n))

        # Rank: least season-to-date bench first; then this week; then name
        entries.sort(key=lambda x: (x[2], x[1], x[0].lower()))
        rank = 1
        for m, wmin, s2d, avail_n in entries:
            bweek_rows.append(
                {
                    "Game Week (Tuesday PT)": wk.isoformat(),
                    "Main": m,
                    "Bench Minutes (Week)": f"{wmin:.2f}",
                    "Bench Minutes (Season-to-date)": f"{s2d:.2f}",
                    "Rank (Least bench first)": str(rank),
                    "Role": main_role.get(m, ""),
                    "Nights Count (Avail)": str(avail_n),
                }
            )
            rank += 1

    bw_headers, _ = read_all(ws_bweek)
    if not bw_headers:
        raise RuntimeError("Bench Week Totals sheet missing headers.")
    ins_bw, upd_bw = upsert_rows(
        ws_bweek, bw_headers, bweek_rows, ["Game Week (Tuesday PT)", "Main"]
    )
    LOG.info("Bench Week Totals upserts: +%d / updated %d", ins_bw, upd_bw)

    # Service log entry
    log(
        ws_service,
        tz,
        "",
        "",
        "BENCH",
        f"BenchNight +{ins_bn}/{upd_bn}; BenchWeek +{ins_bw}/{upd_bw}",
    )


def log(
    ws_service,
    tz: str,
    report_code: str,
    night_id: str,
    stage: str,
    message: str,
    details: Dict[str, Any] | None = None,
):
    headers, _ = read_all(ws_service)
    if not headers:
        return
    row = {
        "Timestamp (PT)": now_pt_iso(tz),
        "Level": "info",
        "Report Code": report_code,
        "Night ID": night_id or "",
        "Stage": stage,
        "Message": message,
        "Details JSON": "{}",
    }
    ws_service.append_row(
        [row.get(h, "") for h in headers], value_input_option="USER_ENTERED"
    )


if __name__ == "__main__":
    main()
