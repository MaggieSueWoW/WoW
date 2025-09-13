import requests
import time
from tenacity import nap

from pebble import wcl_client
from pebble.wcl_client import WCLClient


def test_post_retries(monkeypatch):
    monkeypatch.setattr(nap, "sleep", lambda _: None)
    client = WCLClient("id", "secret")
    client._ensure_token = lambda: None  # bypass token retrieval

    attempts = {"count": 0}

    def fake_post(url, *args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise requests.ConnectionError("boom")

        class Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"data": {"ok": True}}

        return Resp()

    client._session.post = fake_post
    data = client._post("query")
    assert data == {"data": {"ok": True}}
    assert attempts["count"] == 3


class DummyRedis:
    def __init__(self):
        self.store = {}
        self.last_ttl = None

    def get(self, key):
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.store[key] = value
        self.last_ttl = ttl


def test_fetch_report_bundle_caches(monkeypatch):
    dr = DummyRedis()
    client = WCLClient("id", "secret", redis_client=dr, cache_prefix="test:")
    client._ensure_token = lambda: None
    calls = {"n": 0}

    def fake_post(query, variables=None):
        calls["n"] += 1
        return {"data": {"reportData": {"report": {"code": variables["code"], "startTime": 0}}}}

    client._post = fake_post
    client.fetch_report_bundle("ABC")
    client.fetch_report_bundle("ABC")
    assert calls["n"] == 1


def test_fetch_report_bundle_ttl(monkeypatch):
    now_ms = int(time.time() * 1000)
    recent_start = now_ms - 12 * 60 * 60 * 1000
    dr = DummyRedis()
    client = WCLClient("id", "secret", redis_client=dr, cache_prefix="test:")
    client._ensure_token = lambda: None

    def fake_post(query, variables=None):
        return {"data": {"reportData": {"report": {"code": variables["code"], "startTime": recent_start}}}}

    client._post = fake_post
    client.fetch_report_bundle("NEW")
    assert dr.last_ttl == wcl_client.CACHE_TTL_SHORT

    old_start = now_ms - 2 * 24 * 60 * 60 * 1000
    dr2 = DummyRedis()
    client2 = WCLClient("id", "secret", redis_client=dr2, cache_prefix="test:")
    client2._ensure_token = lambda: None

    def fake_post2(query, variables=None):
        return {"data": {"reportData": {"report": {"code": variables["code"], "startTime": old_start}}}}

    client2._post = fake_post2
    client2.fetch_report_bundle("OLD")
    assert dr2.last_ttl == wcl_client.CACHE_TTL_LONG
