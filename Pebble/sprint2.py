from __future__ import annotations
import os
import sys
import json
import yaml
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from sheets_io import (
    open_sheet, ws_by_name, read_all, rows_to_dicts, upsert_rows,
    now_pt_iso, ms_to_pt_iso, to_int, col_letter
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("sprint2")

@dataclass(frozen=True)
class Fight:
    report_code: str
    fight_id: int
    encounter_id: int
    name: str
    difficulty: int
    start_ms: int
    end_ms: int
    start_pt: datetime
    end_pt: datetime
    is_mythic: bool
    is_trash: bool
    temp_key: str  # "{report_code}:{fight_id}"

@dataclass(frozen=True)
class PartRow:
    fight_key: str
    report_code: str
    actor_id: int
    character: str
    main: str
    start_pt: datetime
    end_pt: datetime
    duration_sec: int
    in_mythic: bool

def parse_time_local(t: str) -> time:
    # "19:00" -> time(19,0)
    hh, mm = [int(x) for x in t.strip().split(":")]
    return time(hh, mm)

def within_window(dt_local: datetime, win_start: time, win_end: time) -> bool:
    t = dt_local.timetz()
    s = win_start
    e = win_end
    # assumes start<end on same day (your schedule is 19:00–22:30)
    return (t >= s) and (t <= e)

def iso_to_dt_local(s: str, tz: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(tz))
        return dt.astimezone(ZoneInfo(tz))
    except Exception:
        return None

def main():
    if len(sys.argv) < 2:
        print("Usage: python sprint2.py config/config.yaml")
        sys.exit(2)

    load_dotenv()
    cfg_path = sys.argv[1]
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    tz = cfg["app"]["timezone"]
    sheets_cfg = cfg["app"]["sheets"]
    raid_start_t = parse_time_local(cfg["app"]["raid_window"]["start_local"])
    raid_end_t   = parse_time_local(cfg["app"]["raid_window"]["end_local"])

    break_start_t = parse_time_local(cfg["app"]["break_window"]["start_local"])
    break_end_t   = parse_time_local(cfg["app"]["break_window"]["end_local"])
    min_break_min = int(cfg["app"]["break_window"]["min_gap_minutes"])
    max_break_min = int(cfg["app"]["break_window"]["max_gap_minutes"])
    dedupe_tol_s  = int(cfg["app"]["dedupe"]["overlap_merge_tolerance_sec"])

    ss = open_sheet(cfg["google"]["service_account_json_path"], cfg["app"]["sheet_id"])
    ws_reports = ws_by_name(ss, sheets_cfg["reports"])
    ws_control = ws_by_name(ss, sheets_cfg["control"])
    ws_roster  = ws_by_name(ss, sheets_cfg["roster_map"])
    ws_fights  = ws_by_name(ss, sheets_cfg["fights"])
    ws_part    = ws_by_name(ss, sheets_cfg["participation"])
    ws_blocks  = ws_by_name(ss, sheets_cfg["blocks"])
    ws_ntotals = ws_by_name(ss, sheets_cfg["night_totals"])
    ws_service = ws_by_name(ss, sheets_cfg["service_log"])

    # --- Load Reports (for manual Night ID overrides, etc.)
    rep_headers, rep_rows = read_all(ws_reports)
    reports = rows_to_dicts(rep_headers, rep_rows)
    report_night_override: Dict[str, str] = {}
    manual_breaks: Dict[str, Tuple[Optional[datetime], Optional[datetime]]] = {}
    for r in reports:
        code = (r.get("Report Code") or "").strip()
        if code:
            night_override = (r.get("Raid Night ID (override)") or "").strip()
            if night_override:
                report_night_override[code] = night_override
            # manual break
            mbs = iso_to_dt_local(r.get("Manual Break Start (PT)", ""), tz)
            mbe = iso_to_dt_local(r.get("Manual Break End (PT)", ""), tz)
            manual_breaks[code] = (mbs, mbe)

    # --- Load Roster Map (character -> main)
    rm_headers, rm_rows = read_all(ws_roster)
    roster = rows_to_dicts(rm_headers, rm_rows)
    char_to_main: Dict[str, str] = {}
    for rr in roster:
        char = (rr.get("Character (Name-Realm)") or "").strip()
        main = (rr.get("Main (Name-Realm)") or "").strip()
        if char:
            char_to_main[char] = main if main else char

    # --- Load Fights
    f_headers, f_rows = read_all(ws_fights)
    fights_raw = rows_to_dicts(f_headers, f_rows)

    fights: List[Fight] = []
    tzinfo = ZoneInfo(tz)

    for fr in fights_raw:
        try:
            code = fr["Report Code"]
            fid = int(fr["Fight ID (in report)"])
            enc = int(fr["Encounter ID"])
            diff = int(fr.get("Difficulty") or 0)
            is_mythic = (diff == 5)
            is_trash = (enc == 0)
            s_ms = int(fr["Start (UTC ms)"])
            e_ms = int(fr["End (UTC ms)"])
            s_pt = datetime.fromtimestamp(s_ms/1000, tz=timezone.utc).astimezone(tzinfo)
            e_pt = datetime.fromtimestamp(e_ms/1000, tz=timezone.utc).astimezone(tzinfo)
            fights.append(Fight(
                report_code=code,
                fight_id=fid,
                encounter_id=enc,
                name=fr.get("Encounter Name",""),
                difficulty=diff,
                start_ms=s_ms,
                end_ms=e_ms,
                start_pt=s_pt,
                end_pt=e_pt,
                is_mythic=is_mythic,
                is_trash=is_trash,
                temp_key=f"{code}:{fid}",
            ))
        except Exception as ex:
            LOG.warning("Skipping malformed fight row: %s", ex)

    # --- Load Participation (already Mythic boss-only from Sprint 1)
    p_headers, p_rows = read_all(ws_part)
    parts_raw = rows_to_dicts(p_headers, p_rows)
    parts: List[PartRow] = []
    for pr in parts_raw:
        try:
            s = iso_to_dt_local(pr.get("Start (PT)",""), tz)
            e = iso_to_dt_local(pr.get("End (PT)",""), tz)
            if not s or not e:
                continue
            parts.append(PartRow(
                fight_key=pr["Fight Key"],
                report_code=pr["Report Code"],
                actor_id=int(pr["Actor ID"]),
                character=pr.get("Character (Name-Realm)",""),
                main=pr.get("Main","") or pr.get("Character (Name-Realm)",""),
                start_pt=s,
                end_pt=e,
                duration_sec=int(pr.get("Duration (sec)") or 0),
                in_mythic=(pr.get("In Mythic","").upper() == "TRUE")
            ))
        except Exception as ex:
            LOG.warning("Skipping malformed participation row: %s", ex)

    # --- Canonicalize fights (in-memory dedupe across overlapping logs)
    # Key = (encounter_id, round(start_ms/tol), round(end_ms/tol))
    def canon_key(f: Fight) -> Tuple[int, int, int]:
        return (
            f.encounter_id,
            round(f.start_ms / (dedupe_tol_s * 1000)),
            round(f.end_ms / (dedupe_tol_s * 1000)),
        )

    canon_map: Dict[Tuple[int,int,int], Fight] = {}
    fk_map: Dict[str, str] = {}  # temp_key -> canonical temp_key
    for f in sorted(fights, key=lambda x: (x.encounter_id, x.start_ms, x.end_ms, x.report_code)):
        ck = canon_key(f)
        if ck not in canon_map:
            canon_map[ck] = f
        # map every source temp_key to the canonical temp_key
        fk_map[f.temp_key] = canon_map[ck].temp_key

    # --- Assign Night IDs (by PT raid window; allow per-report override)
    # Night ID format: YYYY-MM-DD (PT) by fight start date that falls in raid window; else by date of start.
    fight_by_canon: Dict[str, Fight] = {}
    night_of_fight: Dict[str, str] = {}

    for f in canon_map.values():
        fight_by_canon[f.temp_key] = f
    for f in fight_by_canon.values():
        # Prefer per-report override if present
        night_id = report_night_override.get(f.report_code)
        if not night_id:
            local_date = f.start_pt.date()
            # If fight starts within configured raid window, use that date
            if within_window(f.start_pt, raid_start_t, raid_end_t):
                night_id = local_date.isoformat()
            else:
                # Fallback: if fight end is within window, use that date; else start date
                if within_window(f.end_pt, raid_start_t, raid_end_t):
                    night_id = f.end_pt.date().isoformat()
                else:
                    night_id = local_date.isoformat()
        night_of_fight[f.temp_key] = night_id

    # --- Break detection per Night ID (auto; allow per-report manual override)
    # We'll detect break using *mythic boss fights* only.
    # If multiple reports share a night, we combine all canonical fights for that night.
    nights: Dict[str, List[Fight]] = {}
    for fk, f in fight_by_canon.items():
        if not f.is_mythic or f.is_trash:
            continue  # boss-only for break detection
        nid = night_of_fight[fk]
        nights.setdefault(nid, []).append(f)

    break_range_by_night: Dict[str, Optional[Tuple[datetime, datetime]]] = {}
    for nid, fs in nights.items():
        if not fs:
            break_range_by_night[nid] = None
            continue
        fs = sorted(fs, key=lambda x: x.start_ms)
        # manual override if any report for this night provided explicit break
        manual = None
        for f in fs:
            mbs, mbe = manual_breaks.get(f.report_code, (None, None))
            if mbs and mbe and mbe > mbs:
                manual = (mbs, mbe)
                break
        if manual:
            break_range_by_night[nid] = manual
            continue

        # auto: find max gap inside [break_start_t, break_end_t] window with min/max duration
        best = None
        best_len = timedelta(0)
        for prev, nxt in zip(fs, fs[1:]):
            gap_start = prev.end_pt
            gap_end = nxt.start_pt
            gap = gap_end - gap_start
            if gap < timedelta(minutes=min_break_min) or gap > timedelta(minutes=max_break_min):
                continue
            # midpoint inside window?
            mid = gap_start + gap/2
            if not within_window(mid, break_start_t, break_end_t):
                continue
            if gap > best_len:
                best = (gap_start, gap_end)
                best_len = gap
        break_range_by_night[nid] = best

    # --- Build contiguous Blocks per Night/Main
    # We use boss fights timeline for "contiguous"; time credited is from first boss start to last boss end, minus any break overlap.
    # Participation already tells us which boss fights a player was in.
    # If a player misses one boss in the middle, that splits the block.
    # Map participation to canonical fight order
    parts_by_night_main: Dict[Tuple[str,str], List[Fight]] = {}
    # Build fight order per night for boss fights
    order_by_night: Dict[str, List[Fight]] = {nid: sorted(fs, key=lambda x: x.start_ms) for nid, fs in nights.items()}
    idx_by_night_key: Dict[str, Dict[str, int]] = {}
    for nid, fs in order_by_night.items():
        idx_by_night_key[nid] = {f.temp_key: i for i, f in enumerate(fs)}

    # assign mains via roster map
    def to_main(character: str) -> str:
        return char_to_main.get(character, character)

    for pr in parts:
        if not pr.in_mythic:
            continue
        canonical_fk = fk_map.get(pr.fight_key, pr.fight_key)
        f = fight_by_canon.get(canonical_fk)
        if not f or f.is_trash or not f.is_mythic:
            continue
        nid = night_of_fight[canonical_fk]
        m = to_main(pr.character if pr.main == "" else pr.main)
        parts_by_night_main.setdefault((nid, m), []).append(f)

    # Build blocks: consecutive indices
    blocks_rows: List[Dict[str, Any]] = []
    ntotals_acc: Dict[Tuple[str,str], Dict[str, float]] = {}  # (nid, main) -> {"total":sec, "pre":sec, "post":sec}

    for (nid, main), fs in parts_by_night_main.items():
        fs_sorted = sorted({f.temp_key: f for f in fs}.values(), key=lambda x: x.start_ms)  # unique & ordered
        if not fs_sorted:
            continue
        fight_order = order_by_night.get(nid, [])
        if not fight_order:
            continue
        # Build index map for contiguous detection
        index_map = {f.temp_key: i for i, f in enumerate(fight_order)}
        blocks: List[Tuple[Fight, Fight]] = []
        start_f = fs_sorted[0]
        prev_f = start_f
        for f in fs_sorted[1:]:
            if index_map.get(f.temp_key, -999) == index_map.get(prev_f.temp_key, -999) + 1:
                # still contiguous
                prev_f = f
                continue
            else:
                blocks.append((start_f, prev_f))
                start_f = f
                prev_f = f
        blocks.append((start_f, prev_f))

        # Night span (boss-only) used for pre/post split
        night_fs = order_by_night[nid]
        night_start = night_fs[0].start_pt
        night_end   = night_fs[-1].end_pt
        br = break_range_by_night.get(nid)
        for idx, (b_start_f, b_end_f) in enumerate(blocks, start=1):
            block_start = b_start_f.start_pt
            block_end   = b_end_f.end_pt
            block_dur = (block_end - block_start).total_seconds()
            # subtract break overlap
            break_overlap = 0.0
            if br:
                ovl_start = max(block_start, br[0])
                ovl_end   = min(block_end, br[1])
                if ovl_end > ovl_start:
                    break_overlap = (ovl_end - ovl_start).total_seconds()
            credited = max(0.0, block_dur - break_overlap)

            # pre/post split
            pre = 0.0
            post = 0.0
            if br:
                pre = max(0.0, (min(block_end, br[0]) - block_start).total_seconds()) if block_end > block_start else 0.0
                post = max(0.0, (block_end - max(block_start, br[1])).total_seconds()) if block_end > block_start else 0.0
            else:
                # no break detected: treat all as total
                pre = credited
                post = 0.0

            # Accumulate night totals
            key = (nid, main)
            acc = ntotals_acc.setdefault(key, {"total": 0.0, "pre": 0.0, "post": 0.0})
            acc["total"] += credited
            acc["pre"]   += pre
            acc["post"]  += post

            blocks_rows.append({
                "Night ID": nid,
                "Main": main,
                "Character(s) Used": "",  # optional: could fill by scanning parts rows for this block
                "Block Index": str(idx),
                "Block Start (PT)": block_start.isoformat(timespec="seconds"),
                "Block End (PT)":   block_end.isoformat(timespec="seconds"),
                "Block Duration (sec)": str(int(credited)),
                "Break Overlap (sec)":  str(int(break_overlap)),
                "First Fight Key in Block": b_start_f.temp_key,
                "Last Fight Key in Block":  b_end_f.temp_key,
                "Mythic Segment ID": "",      # reserved
                "Include Trash?": "FALSE",
                "Roster Map Version": "",     # reserved
            })

    # --- Write Blocks (UPSERT by Night ID + Main + First/Last fight keys) ---
    b_headers, _ = read_all(ws_blocks)
    if not b_headers:
        raise RuntimeError("Blocks sheet missing headers.")

    ins_b, upd_b = upsert_rows(
        ws_blocks, b_headers, blocks_rows,
        ["Night ID", "Main", "First Fight Key in Block", "Last Fight Key in Block"]
    )
    LOG.info("Blocks upserts: +%d / updated %d", ins_b, upd_b)

    # --- Night Totals ---
    nt_rows: List[Dict[str, Any]] = []
    for (nid, main), acc in ntotals_acc.items():
        nt_rows.append({
            "Night ID": nid,
            "Main": main,
            "Minutes (Total)": f"{acc['total']/60:.2f}",
            "Minutes Pre-Break": f"{acc['pre']/60:.2f}",
            "Minutes Post-Break": f"{acc['post']/60:.2f}",
            "Blocks Count": "",              # could compute len(blocks) per (nid, main) if you keep that mapping
            "Character(s) Used": "",
            "Notes": "",
        })

    nt_headers, _ = read_all(ws_ntotals)
    if not nt_headers:
        raise RuntimeError("Night Totals sheet missing headers.")

    ins_n, upd_n = upsert_rows(
        ws_ntotals, nt_headers, nt_rows,
        ["Night ID", "Main"]
    )
    LOG.info("Night Totals upserts: +%d / updated %d", ins_n, upd_n)

    # Log success
    log(ws_service, tz, "", "", "BLOCKS", f"Blocks +{ins_b}/{upd_b}; NightTotals +{ins_n}/{upd_n}")

def log(ws_service, tz: str, report_code: str, night_id: str, stage: str, message: str, details: Dict[str, Any] | None = None):
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
        "Details JSON": json.dumps(details or {}),
    }
    ws_service.append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
