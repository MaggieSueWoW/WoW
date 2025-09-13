from __future__ import annotations

import logging
from typing import Any

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, HttpError):
        return exc.resp is not None and exc.resp.status in _RETRY_STATUS_CODES
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class SheetsClient:
    def __init__(self, creds_path: str):
        creds = Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self._svc = build("sheets", "v4", credentials=creds)

    @property
    def svc(self) -> Any:
        return self._svc

    @retry(
        reraise=True,
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def execute(self, req):
        desc = getattr(req, "uri", None) or getattr(req, "_rest_path", "unknown")
        logger.info("Google Sheets request", extra={"request": desc})
        try:
            resp = req.execute()
            logger.info("Google Sheets request succeeded", extra={"request": desc})
            return resp
        except Exception:
            logger.warning(
                "Google Sheets request failed",
                extra={"request": desc},
                exc_info=True,
            )
            raise
