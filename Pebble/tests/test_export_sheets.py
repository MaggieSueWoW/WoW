import pebble.export_sheets as es


def test_replace_values_user_entered(monkeypatch):
    updates = []

    class FakeReq:
        def __init__(self, bucket):
            self.bucket = bucket

        def execute(self):
            return None

    class FakeValues:
        def clear(self, **kwargs):
            updates.append(("clear", kwargs))
            return FakeReq(updates)

        def update(self, **kwargs):
            updates.append(("update", kwargs))
            return FakeReq(updates)

    class FakeSpreadsheets:
        def values(self):
            return FakeValues()

    class FakeSvc:
        def spreadsheets(self):
            return FakeSpreadsheets()

    class FakeClient:
        def __init__(self, path):
            self.svc = FakeSvc()
        def execute(self, req):
            req.execute()

    monkeypatch.setattr(es, "SheetsClient", FakeClient)

    es.replace_values(
        "sid",
        "Sheet1",
        [["2024-07-02 20:00:00"]],
        "creds.json",
        start_cell="B2",
    )

    # First call clears, second updates data, third updates last processed
    assert updates[0][0] == "clear"
    assert updates[0][1]["range"] == "Sheet1!B2:Z"
    assert updates[1][0] == "update"
    assert updates[1][1]["valueInputOption"] == "USER_ENTERED"
    assert updates[1][1]["range"] == "Sheet1!B2"
    assert updates[2][0] == "update"
    assert updates[2][1]["range"] == "Sheet1!B3"
