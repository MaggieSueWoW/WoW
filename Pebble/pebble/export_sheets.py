from __future__ import annotations
from typing import List

from .sheets_client import SheetsClient
from .utils.sheets import update_last_processed


def replace_values(
    spreadsheet_id: str,
    tab: str,
    values: List[List],
    creds_path: str,
    start_cell: str = "A5",
    last_processed_cell: str = "B3",
) -> None:
    """Replace all values in ``tab`` with ``values``.

    ``USER_ENTERED`` is used so that any date/time strings are parsed by
    Google Sheets and treated as proper datetimes rather than plain text.
    """
    client = SheetsClient(creds_path)
    svc = client.svc
    rng = f"{tab}!{start_cell}"
    body = {"values": values, "majorDimension": "ROWS"}
    client.execute(
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{tab}!{start_cell}:Z"
        )
    )
    client.execute(
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="USER_ENTERED",
            body=body,
        )
    )
    update_last_processed(
        spreadsheet_id,
        tab,
        creds_path,
        last_processed_cell,
        client,
    )

