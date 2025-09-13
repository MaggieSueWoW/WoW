from __future__ import annotations
import logging, json, sys
from .utils.time import ms_to_pt_iso

# attributes that are always present on ``LogRecord`` instances.  Any additional
# attributes supplied via the ``extra`` keyword of ``logger.log`` calls are added
# directly to the record, so we capture those by comparing against this set.
_BASE_LOG_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": ms_to_pt_iso(int(record.created * 1000)),
            "lvl": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
        }

        # Merge any additional attributes (those not part of the base
        # ``LogRecord``) into the payload so callers can provide structured
        # data via the ``extra`` argument to ``logger.log`` calls.
        for key, value in record.__dict__.items():
            if key not in _BASE_LOG_RECORD_KEYS and key not in payload:
                payload[key] = value

        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger("pebble")
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root.addHandler(h)
    return root
