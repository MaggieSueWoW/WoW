from __future__ import annotations

from datetime import datetime

from ..sheets_client import SheetsClient
from .time import PT, ms_to_pt_sheets


def update_last_processed(
    spreadsheet_id: str,
    tab: str,
    creds_path: str,
    cell: str = "B3",
    client: SheetsClient | None = None,
) -> None:
    """Write the current PT datetime to ``cell`` on ``tab``.

    The datetime is formatted so Google Sheets parses it as a proper datetime.
    """
    client = client or SheetsClient(creds_path)
    svc = client.svc
    now_ms = int(datetime.now(tz=PT).timestamp() * 1000)
    body = {"values": [[ms_to_pt_sheets(now_ms)]], "majorDimension": "ROWS"}
    rng = f"{tab}!{cell}"
    client.execute(
        svc.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body=body,
        )
    )
