from __future__ import annotations
import os
import sys
import json
import yaml
import logging
import time
from typing import Dict, Any, List

from dotenv import load_dotenv

from wcl_client import extract_report_code, get_token, fetch_report, stable_digest
from sheets_io import (
    open_sheet,
    ws_by_name,
    read_all,
    rows_to_dicts,
    upsert_rows,
    now_pt_iso,
    ms_to_pt_iso,
    to_int,
    col_letter,
)

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("runner")


def boolish(val: str, default: bool = False) -> bool:
    s = (val or "").strip().lower()
    if s in ("true", "t", "yes", "y", "1"):
        return True
    if s in ("false", "f", "no", "n", "0"):
        return False
    return default


def make_character(name: str, server: str) -> str:
    if not name:
        return ""
    return f"{name}-{server}" if server else name


def main():
    if len(sys.argv) < 2:
        print("Usage: python sprint1.py config/config.yaml")
        sys.exit(2)

    load_dotenv()
    cfg_path = sys.argv[1]
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    tz = cfg["app"]["timezone"]
    sheet_id = cfg["app"]["sheet_id"]
    sheets_cfg = cfg["app"]["sheets"]

    ss = open_sheet(cfg["google"]["service_account_json_path"], sheet_id)

    ws_reports = ws_by_name(ss, sheets_cfg["reports"])
    ws_fights = ws_by_name(ss, sheets_cfg["fights"])
    ws_part = ws_by_name(ss, sheets_cfg["participation"])
    ws_service = ws_by_name(ss, sheets_cfg["service_log"])

    sleep_ms = int(cfg["app"]["pacing"].get("sleep_between_requests_ms", 200))
    # optional control switch (won't exist unless you add it to Control)
    auto_mark_done = False

    # try reading Control key (best-effort; ignore if absent)
    try:
        ws_control = ws_by_name(ss, sheets_cfg["control"])
        c_headers, c_rows = read_all(ws_control)
        ctrl = rows_to_dicts(c_headers, c_rows)
        kv = {row["Key"]: (row.get("Value") or "") for row in ctrl if "Key" in row}
        auto_mark_done = kv.get("Auto Mark Done", "").strip().lower() in (
            "true",
            "t",
            "yes",
            "y",
            "1",
        )
    except Exception:
        pass

    rep_headers, rep_rows = read_all(ws_reports)
    if not rep_headers:
        raise RuntimeError("Reports sheet is empty or missing headers.")
    reports = rows_to_dicts(rep_headers, rep_rows)

    client_id = os.getenv("WCL_CLIENT_ID", cfg["wcl"].get("client_id"))
    client_secret = os.getenv("WCL_CLIENT_SECRET", cfg["wcl"].get("client_secret"))
    if not client_id or not client_secret:
        raise RuntimeError("Missing WCL_CLIENT_ID / WCL_CLIENT_SECRET env vars.")

    token = get_token(client_id, client_secret)

    processed = 0
    for r in reports:
        status = (r.get("Status") or "").strip().lower()
        if status == "done":
            continue

        url = r.get("Report URL", "")
        code = extract_report_code((r.get("Report Code") or url))
        if not code:
            continue

        try:
            rep = fetch_report(token, code)
        except Exception as e:
            log(
                ws_service,
                tz,
                code,
                r.get("Raid Night ID (override)", ""),
                "FETCH",
                f"Error fetching: {e}",
            )
            continue
        if rep is None:
            log(
                ws_service,
                tz,
                code,
                r.get("Raid Night ID (override)", ""),
                "FETCH",
                "No report returned",
            )
            continue

        report_start = int(rep.get("startTime") or 0)
        report_end = int(rep.get("endTime") or 0)
        fights = rep.get("fights") or []
        actors = (rep.get("masterData") or {}).get("actors") or []
        guild = rep.get("guild", {})
        guild_name = guild.get("name") or ""

        # build digests for change detection
        fights_digest_list = [
            {
                "id": int(f["id"]),
                "s": report_start + int(f["startTime"]),
                "e": report_start + int(f["endTime"]),
            }
            for f in fights
        ]
        computed_change_hash = stable_digest(
            {
                "F": fights_digest_list,
                "P_count": sum(
                    1
                    for f in fights
                    if int(f.get("difficulty") or 0) == 5
                    and int(f.get("encounterID") or 0) > 0
                ),
            }
        )

        # If Last Change Hash matches and we already have Report Code set, skip all writes for this row
        existing_lch = (r.get("Last Change Hash") or "").strip()
        if existing_lch and existing_lch == computed_change_hash:
            # Optionally auto-mark done if report_end > 0 and the switch is on
            if auto_mark_done and report_end and status != "done":
                r2 = dict(r)
                r2["Status"] = "done"
                write_back_reports_row(
                    ws_reports, rep_headers, r, r2
                )  # safe update for status only
                log(
                    ws_service,
                    tz,
                    code,
                    r.get("Raid Night ID (override)", ""),
                    "STAGE",
                    "No changes; auto-marked done.",
                )
            else:
                log(
                    ws_service,
                    tz,
                    code,
                    r.get("Raid Night ID (override)", ""),
                    "STAGE",
                    "No changes; skipped writes.",
                )
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
            continue

        # Build actor map
        actor_map = {int(a["id"]): a for a in actors}

        # FIGHTS rows (staging; temp Fight Key)
        fights_rows: List[Dict[str, Any]] = []
        fights_digest_list: List[Dict[str, int]] = []

        for f in fights:
            fight_id = int(f["id"])
            rel_s = int(f["startTime"])
            rel_e = int(f["endTime"])
            abs_start = report_start + rel_s
            abs_end = report_start + rel_e

            fights_rows.append(
                {
                    "Fight Key": f"{code}:{fight_id}",
                    "Report Code": code,
                    "Fight ID (in report)": str(fight_id),
                    "Night ID": "",
                    "Encounter Name": f.get("name") or "",
                    "Encounter ID": str(f.get("encounterID") or 0),
                    "Difficulty": str(f.get("difficulty") or ""),
                    "Is Mythic": (
                        "TRUE" if int(f.get("difficulty") or 0) == 5 else "FALSE"
                    ),
                    "Is Trash": (
                        "TRUE" if int(f.get("encounterID") or 0) == 0 else "FALSE"
                    ),
                    "Start (UTC ms)": str(abs_start),
                    "End (UTC ms)": str(abs_end),
                    "Start (PT)": ms_to_pt_iso(abs_start, tz),
                    "End (PT)": ms_to_pt_iso(abs_end, tz),
                    "Duration (sec)": str(max(0, (abs_end - abs_start) // 1000)),
                    "Within Raid Window": "",
                    "Mythic Block ID": "",
                    "Break Gap Member": "",
                }
            )
            fights_digest_list.append({"id": fight_id, "s": abs_start, "e": abs_end})

        # PARTICIPATION rows — **Mythic boss fights only** to keep size small
        part_rows: List[Dict[str, Any]] = []
        for f in fights:
            if int(f.get("difficulty") or 0) != 5:
                continue  # not Mythic
            if int(f.get("encounterID") or 0) <= 0:
                continue  # exclude trash / non-boss pulls

            fight_id = int(f["id"])
            abs_start = report_start + int(f["startTime"])
            abs_end = report_start + int(f["endTime"])
            dur = max(0, (abs_end - abs_start) // 1000)
            start_pt = ms_to_pt_iso(abs_start, tz)
            end_pt = ms_to_pt_iso(abs_end, tz)

            for aid in f.get("friendlyPlayers") or []:
                a = actor_map.get(int(aid))
                if not a:
                    continue
                char = make_character(a.get("name") or "", a.get("server") or "")
                part_rows.append(
                    {
                        "Fight Key": f"{code}:{fight_id}",
                        "Report Code": code,
                        "Fight ID (in report)": str(fight_id),
                        "Actor ID": str(aid),
                        "Character (Name-Realm)": char,
                        "Main": char,  # Roster map comes in Sprint 2
                        "Class": a.get("subType") or "",
                        "Spec": "",
                        "Role": "",
                        "In Mythic": "TRUE",
                        "Count Toward Trash": "FALSE",
                        "Start (PT)": start_pt,
                        "End (PT)": end_pt,
                        "Duration (sec)": str(dur),
                    }
                )

        # Upserts
        f_headers, _ = read_all(ws_fights)
        p_headers, _ = read_all(ws_part)
        if not f_headers or not p_headers:
            raise RuntimeError(
                "Fights/Participation sheets missing headers. Paste them first."
            )

        ins_f, upd_f = upsert_rows(
            ws_fights, f_headers, fights_rows, ["Report Code", "Fight ID (in report)"]
        )
        ins_p, upd_p = upsert_rows(
            ws_part,
            p_headers,
            part_rows,
            ["Report Code", "Fight ID (in report)", "Actor ID"],
        )

        # Update Reports cache
        reports_updates = {
            "Report Code": code,
            "Report Start (PT)": ms_to_pt_iso(report_start, tz) if report_start else "",
            "Report End (PT)": ms_to_pt_iso(report_end, tz) if report_end else "",
            "Report Start (UTC ms)": str(report_start),
            "Report End (UTC ms)": str(report_end),
            "Guild": guild_name,
            "Uploader": "",
            "Closed?": "TRUE" if report_end else "FALSE",
            "Last Processed Fight Index": str(
                max([int(f["id"]) for f in fights], default=0)
            ),
            "Digest": stable_digest(fights_digest_list),
            "Last Checked (PT)": now_pt_iso(tz),
            "Last Change Hash": computed_change_hash,
        }
        # Optional auto-mark done
        if auto_mark_done and report_end:
            reports_updates["Status"] = "done"

        write_back_reports_row(ws_reports, rep_headers, r, reports_updates)
        log(
            ws_service,
            tz,
            code,
            r.get("Raid Night ID (override)", ""),
            "STAGE",
            f"Updated report cache; wrote Fights/Participation diffs.",
        )

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        processed += 1

    if processed == 0:
        LOG.info("No reports to process (all 'done' or empty).")


def write_back_reports_row(
    ws, headers: List[str], current_row_dict: Dict[str, str], updates: Dict[str, str]
):
    all_vals = ws.get_all_values()
    header_index = {h: i for i, h in enumerate(headers)}
    target_row = None
    for idx in range(1, len(all_vals)):  # skip header
        row = all_vals[idx]
        if row and header_index.get("Report URL") is not None:
            if row[header_index["Report URL"]] == current_row_dict.get("Report URL"):
                target_row = idx + 1  # 1-based including header
                break

    merged = dict(current_row_dict)
    merged.update(updates)
    row_vals = [str(merged.get(h, "")) for h in headers]

    if target_row is None:
        logging.getLogger("sheets").info(
            "Reports: appending new row for URL %s", current_row_dict.get("Report URL")
        )
        ws.append_row(row_vals, value_input_option="USER_ENTERED")
        return

    rng = f"A{target_row}:{col_letter(len(headers))}{target_row}"
    logging.getLogger("sheets").info("Reports: updating row %d", target_row)
    ws.update(rng, [row_vals])


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
        raise RuntimeError("Service Log sheet missing headers.")
    row = {
        "Timestamp (PT)": now_pt_iso(tz),
        "Level": "info",
        "Report Code": report_code,
        "Night ID": night_id or "",
        "Stage": stage,
        "Message": message,
        "Details JSON": json.dumps(details or {}),
    }
    ws_service.append_row(
        [row.get(h, "") for h in headers], value_input_option="USER_ENTERED"
    )


if __name__ == "__main__":
    main()
