from __future__ import annotations
from typing import List, Dict, Any

from ..sheets_client import SheetsClient
from ..config_loader import Settings
from ..utils.sheets import update_last_processed

HEADERS = {
    "Reports": [
        "Report URL",
        "Status",
        "Last Checked (PT)",
        "Notes",
        "Break Override Start (PT)",
        "Break Override End (PT)",
        "Report Name",
        "Report Start (PT)",
        "Report End (PT)",
        "Created By",
    ],
    "Roster Map": ["Alt", "Main"],
    "Team Roster": [
        "Main",
        "Join Date",
        "Leave Date",
        "Active?",
        "Notes",
    ],
    "Availability Overrides": [
        "Night",
        "Main",
        "Avail Pre?",
        "Avail Post?",
        "Reason",
    ],
    "Night QA": [
        "Night ID",
        "Reports Involved",
        "Report Start (PT)",
        "Report End (PT)",
        "Night Start (PT)",
        "Night End (PT)",
        "Mythic Fights",
        "Break Start (PT)",
        "Break End (PT)",
        "Break Override Start (PT)",
        "Break Override End (PT)",
        "Break Duration (min)",
        "Mythic Start (PT)",
        "Mythic End (PT)",
        "Mythic Pre (min)",
        "Mythic Post (min)",
        "Gap Window",
        "Min/Max Break",
        "Largest Gap (min)",
        "Candidate Gaps (JSON)",
        "Override Used?",
    ],
    "Bench Night Totals": [
        "Night ID",
        "Main",
        "Played Pre (min)",
        "Played Post (min)",
        "Played Total (min)",
        "Bench Pre (min)",
        "Bench Post (min)",
        "Bench Total (min)",
        "Avail Pre?",
        "Avail Post?",
        "Status Source",
    ],
    "Bench Week Totals": [
        "Game Week",
        "Main",
        "Played Week (min)",
        "Bench Week (min)",
        "Bench Pre (min)",
        "Bench Post (min)",
    ],
    "Bench Rankings": [
        "Rank",
        "Main",
        "Bench Season-to-date (min)",
    ],
}


def _get_sheet_names(client: SheetsClient, sheet_id: str) -> List[str]:
    meta = client.execute(client.svc.spreadsheets().get(spreadsheetId=sheet_id))
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def _ensure_tab(client: SheetsClient, sheet_id: str, name: str):
    existing = _get_sheet_names(client, sheet_id)
    if name in existing:
        return False
    client.execute(
        client.svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
        )
    )
    return True


def _ensure_headers(
    client: SheetsClient,
    sheet_id: str,
    name: str,
    headers: list[str],
    start: str,
    creds_path: str,
    last_processed: str,
):
    rng = f"'{name}'!{start}"
    client.execute(
        client.svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [headers]},
        )
    )
    update_last_processed(sheet_id, name, creds_path, last_processed, client)


def bootstrap_sheets(settings: Settings) -> Dict[str, Any]:
    client = SheetsClient(settings.service_account_json)
    sheet_id = settings.sheets.spreadsheet_id
    desired = {
        settings.sheets.tabs.reports: (
            "Reports",
            settings.sheets.starts.reports,
            settings.sheets.last_processed.reports,
        ),
        settings.sheets.tabs.roster_map: (
            "Roster Map",
            settings.sheets.starts.roster_map,
            settings.sheets.last_processed.roster_map,
        ),
        settings.sheets.tabs.team_roster: (
            "Team Roster",
            settings.sheets.starts.team_roster,
            settings.sheets.last_processed.team_roster,
        ),
        settings.sheets.tabs.availability_overrides: (
            "Availability Overrides",
            settings.sheets.starts.availability_overrides,
            settings.sheets.last_processed.availability_overrides,
        ),
        settings.sheets.tabs.night_qa: (
            "Night QA",
            settings.sheets.starts.night_qa,
            settings.sheets.last_processed.night_qa,
        ),
        settings.sheets.tabs.bench_night_totals: (
            "Bench Night Totals",
            settings.sheets.starts.bench_night_totals,
            settings.sheets.last_processed.bench_night_totals,
        ),
        settings.sheets.tabs.bench_week_totals: (
            "Bench Week Totals",
            settings.sheets.starts.bench_week_totals,
            settings.sheets.last_processed.bench_week_totals,
        ),
        settings.sheets.tabs.bench_rankings: (
            "Bench Rankings",
            settings.sheets.starts.bench_rankings,
            settings.sheets.last_processed.bench_rankings,
        ),
    }
    tabs = []
    for name, (canonical, start, last_processed) in desired.items():
        _ensure_tab(client, sheet_id, name)
        _ensure_headers(
            client,
            sheet_id,
            name,
            HEADERS[canonical],
            start,
            settings.service_account_json,
            last_processed,
        )
        tabs.append(name)
    return {"ok": True, "tabs": tabs}
