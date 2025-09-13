from googleapiclient.errors import HttpError
from tenacity import nap

from pebble.sheets_client import SheetsClient


def test_execute_retries(monkeypatch):
    monkeypatch.setattr(nap, "sleep", lambda _: None)
    monkeypatch.setattr(
        "pebble.sheets_client.Credentials.from_service_account_file",
        lambda *a, **kw: object(),
    )
    monkeypatch.setattr("pebble.sheets_client.build", lambda *a, **kw: object())

    client = SheetsClient("creds.json")
    attempts = {"count": 0}

    class Req:
        def execute(self):
            attempts["count"] += 1
            if attempts["count"] < 3:
                resp = type("resp", (), {"status": 500, "reason": "boom"})()
                raise HttpError(resp=resp, content=b"boom")
            return {"ok": True}

    req = Req()
    data = client.execute(req)
    assert data == {"ok": True}
    assert attempts["count"] == 3

