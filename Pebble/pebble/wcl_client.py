from __future__ import annotations

import json
import logging
import time
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

import redis

logger = logging.getLogger(__name__)

_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

CACHE_TTL_SHORT = 5 * 60  # 5 minutes
CACHE_TTL_LONG = 60 * 60 * 24 * 30 * 6  # ~6 months
_FRESH_MS = 24 * 60 * 60 * 1000


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        return (
            exc.response is not None and exc.response.status_code in _RETRY_STATUS_CODES
        )
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class WCLClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        base_url: str = "https://www.warcraftlogs.com/api/v2/client",
        token_url: str = "https://www.warcraftlogs.com/oauth/token",
        redis_url: str | None = None,
        redis_client: Optional[redis.Redis] = None,
        cache_prefix: str = "pebble:wcl:",
    ):
        self._session = requests.Session()
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = base_url
        self._token_url = token_url
        self._token: Optional[str] = None
        self._token_exp: float = 0.0
        if redis_client is not None:
            self._redis = redis_client
        elif redis_url:
            self._redis = redis.from_url(redis_url)
        else:
            self._redis = None
        self._cache_prefix = cache_prefix

    @retry(
        reraise=True,
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _ensure_token(self) -> None:
        now = time.time()
        if self._token and now < (self._token_exp - 60):
            return
        logger.info("Requesting WCL access token", extra={"url": self._token_url})
        try:
            r = self._session.post(
                self._token_url,
                data={"grant_type": "client_credentials"},
                auth=HTTPBasicAuth(self._client_id, self._client_secret),
                timeout=30,
            )
            r.raise_for_status()
        except Exception:
            logger.warning("WCL token request failed", exc_info=True)
            raise
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"WCL token response missing access_token: {data}")
        expires_in = int(data.get("expires_in", 3600))
        self._token = token
        self._token_exp = now + max(60, expires_in)
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})
        logger.info(
            "Obtained WCL access token",
            extra={"expires_in": expires_in},
        )

    @retry(
        reraise=True,
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _post(self, query: str, variables: Optional[dict] = None) -> dict:
        self._ensure_token()
        payload = {"query": query, "variables": variables or {}}
        logger.info(
            "WCL request",
            extra={"url": self._base_url, "has_variables": bool(variables)},
        )
        start = time.time()
        try:
            r = self._session.post(self._base_url, json=payload, timeout=60)
            r.raise_for_status()
        except Exception:
            logger.warning("WCL request failed", exc_info=True)
            raise
        data = r.json()
        if "errors" in data:
            logger.warning("WCL GraphQL errors", extra={"errors": data["errors"]})
            raise RuntimeError(data["errors"])  # surface graph errors
        dur = time.time() - start
        logger.info("WCL request succeeded", extra={"elapsed": round(dur, 3)})
        return data

    def fetch_report_bundle(self, code: str, translate: bool = True) -> dict:
        """Report meta + fights + masterData actors in one call.
        NOTE: fight start/end are relative ms to report.startTime; we normalize in ingest.
        """
        cache_key = f"{self._cache_prefix}{code}"
        if self._redis:
            cached = self._redis.get(cache_key)
            if cached:
                logger.info(
                    "WCL cache hit",
                    extra={"code": code, "cache_key": cache_key},
                )
                return json.loads(cached)
        logger.info("Fetching WCL report bundle", extra={"code": code})
        q = """
        query ReportFightsAndActors($code: String!, $translate: Boolean = true) {
          reportData {
            report(code: $code) {
              code
              title
              startTime
              endTime
              owner { name }
              zone { name }
              region { id name compactName }
              guild { id name server { name region { id name compactName } } }
              fights { id encounterID name difficulty startTime endTime friendlyPlayers kill }
              masterData(translate: $translate) { actors(type: "Player") { id name server subType type } }
            }
          }
        }
        """
        report = self._post(q, {"code": code, "translate": translate})["data"][
            "reportData"
        ]["report"]
        if self._redis:
            start_ms = int(report.get("startTime") or 0)
            now_ms = int(time.time() * 1000)
            ttl = CACHE_TTL_SHORT if start_ms and (now_ms - start_ms) < _FRESH_MS else CACHE_TTL_LONG
            try:
                self._redis.setex(cache_key, ttl, json.dumps(report))
                logger.info(
                    "Cached WCL report",
                    extra={"code": code, "cache_key": cache_key, "ttl": ttl},
                )
            except Exception:
                logger.warning(
                    "Failed to cache WCL report",
                    extra={"code": code, "cache_key": cache_key},
                    exc_info=True,
                )
                raise
        return report


def flush_cache(redis_url: str, prefix: str) -> int:
    """Delete cached WCL reports with the given prefix."""
    r = redis.from_url(redis_url)
    keys = list(r.scan_iter(f"{prefix}*"))
    if keys:
        r.delete(*keys)
    return len(keys)
