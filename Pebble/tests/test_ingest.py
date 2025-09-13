import mongomock
from types import SimpleNamespace
from pebble.ingest import ingest_roster


def test_ingest_roster_parses_sheet(monkeypatch):
    db = mongomock.MongoClient().db
    rows = [
        ["Main", "Join Date", "Leave Date", "Active?"],
        ["Alice-Illidan", "6/25/24", "", "Y"],
        ["Bob-Illidan", "June 25, 2024", "", "n"],
    ]

    monkeypatch.setattr(
        "pebble.ingest._sheet_values", lambda s, tab, start="A5", last_processed="B3": rows
    )
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: db)
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)

    s = SimpleNamespace(
        service_account_json="",
        sheets=SimpleNamespace(
            tabs=SimpleNamespace(team_roster="Team Roster"),
            starts=SimpleNamespace(team_roster="A5"),
            last_processed=SimpleNamespace(team_roster="B3"),
        ),
    )

    count = ingest_roster(s)
    docs = list(db["team_roster"].find({}, {"_id": 0}))
    assert count == 2
    assert sorted(docs, key=lambda r: r["main"]) == [
        {
            "main": "Alice-Illidan",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": True,
        },
        {
            "main": "Bob-Illidan",
            "join_night": "2024-06-25",
            "leave_night": "",
            "active": False,
        },
    ]
