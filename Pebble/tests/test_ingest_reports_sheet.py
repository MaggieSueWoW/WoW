import mongomock
from datetime import datetime
import logging
from pebble.config_loader import Settings, SheetsConfig, MongoConfig, WCLConfig
from pebble.ingest import ingest_reports
from pebble.utils.time import ms_to_pt_sheets, PT


def test_ingest_reports_updates_sheet(monkeypatch):
    rows = [
        [
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
        [
            "https://www.warcraftlogs.com/reports/ABC123",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    updates = []

    class DummyRequest:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class DummyValues:
        def get(self, spreadsheetId, range):
            return DummyRequest({"values": rows})

        def batchUpdate(self, spreadsheetId, body):
            updates.extend(body["data"])
            return DummyRequest({})

    class DummySpreadsheets:
        def values(self):
            return DummyValues()

    class DummySvc:
        def spreadsheets(self):
            return DummySpreadsheets()

    class DummySheetsClient:
        def __init__(self, *args, **kwargs):
            self.svc = DummySvc()

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr("pebble.ingest.SheetsClient", DummySheetsClient)

    sample_bundle = {
        "title": "Report One",
        "startTime": 1000,
        "endTime": 2000,
        "owner": {"name": "Creator"},
        "fights": [],
        "masterData": {"actors": []},
    }

    class DummyWCLClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            return sample_bundle

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.ingest.update_last_processed", lambda *a, **k: None)

    fixed_now = datetime(2025, 4, 2, 18, 50, 49, tzinfo=PT)

    class DummyDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr("pebble.ingest.datetime", DummyDateTime)

    settings = Settings(
        sheets=SheetsConfig(spreadsheet_id="1"),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    res = ingest_reports(settings)
    assert res["reports"] == 1
    update_map = {u["range"].split("!")[1]: u["values"][0][0] for u in updates}
    assert update_map["G6"] == "Report One"
    assert update_map["H6"] == ms_to_pt_sheets(1000)
    assert update_map["I6"] == ms_to_pt_sheets(2000)
    assert update_map["C6"] == ms_to_pt_sheets(int(fixed_now.timestamp() * 1000))
    assert update_map["J6"] == "Creator"


def test_ingest_reports_rejects_non_wcl_links(monkeypatch, caplog):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked PT",
            "Notes",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://example.com/notwcl",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    updates = []

    class DummyRequest:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class DummyValues:
        def get(self, spreadsheetId, range):
            return DummyRequest({"values": rows})

        def batchUpdate(self, spreadsheetId, body):
            updates.extend(body["data"])
            return DummyRequest({})

    class DummySpreadsheets:
        def values(self):
            return DummyValues()

    class DummySvc:
        def spreadsheets(self):
            return DummySpreadsheets()

    class DummySheetsClient:
        def __init__(self, *args, **kwargs):
            self.svc = DummySvc()

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr("pebble.ingest.SheetsClient", DummySheetsClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.ingest.update_last_processed", lambda *a, **k: None)

    settings = Settings(
        sheets=SheetsConfig(spreadsheet_id="1"),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    with caplog.at_level(logging.WARNING):
        res = ingest_reports(settings)

    assert res["reports"] == 0
    update_map = {u["range"].split("!")[1]: u["values"][0][0] for u in updates}
    assert update_map["B6"] == "Bad report link"
    assert "Bad report link at row" in caplog.text


def test_ingest_reports_marks_bad_links_on_fetch_error(monkeypatch, caplog):
    rows = [
        [
            "Report URL",
            "Status",
            "Last Checked PT",
            "Notes",
            "Break Override Start (PT)",
            "Break Override End (PT)",
            "Report Name",
            "Report Start (PT)",
            "Report End (PT)",
            "Created By",
        ],
        [
            "https://www.warcraftlogs.com/reports/ABC123",  # missing last char
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    ]
    updates = []

    class DummyRequest:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class DummyValues:
        def get(self, spreadsheetId, range):
            return DummyRequest({"values": rows})

        def batchUpdate(self, spreadsheetId, body):
            updates.extend(body["data"])
            return DummyRequest({})

    class DummySpreadsheets:
        def values(self):
            return DummyValues()

    class DummySvc:
        def spreadsheets(self):
            return DummySpreadsheets()

    class DummySheetsClient:
        def __init__(self, *args, **kwargs):
            self.svc = DummySvc()

        def execute(self, req):
            return req.execute()

    monkeypatch.setattr("pebble.ingest.SheetsClient", DummySheetsClient)
    monkeypatch.setattr("pebble.ingest.get_db", lambda s: mongomock.MongoClient().db)
    monkeypatch.setattr("pebble.ingest.ensure_indexes", lambda db: None)
    monkeypatch.setattr("pebble.ingest.update_last_processed", lambda *a, **k: None)

    class DummyWCLClient:
        def __init__(self, *args, **kwargs):
            pass

        def fetch_report_bundle(self, code):
            raise RuntimeError([{"message": "Unknown report"}])

    monkeypatch.setattr("pebble.ingest.WCLClient", DummyWCLClient)

    settings = Settings(
        sheets=SheetsConfig(spreadsheet_id="1"),
        mongo=MongoConfig(uri="mongodb://example"),
        wcl=WCLConfig(client_id="id", client_secret="secret"),
    )

    with caplog.at_level(logging.WARNING):
        res = ingest_reports(settings)

    assert res["reports"] == 0
    update_map = {u["range"].split("!")[1]: u["values"][0][0] for u in updates}
    assert update_map["B6"] == "Bad report link"
    assert "Failed to fetch WCL report bundle" in caplog.text
