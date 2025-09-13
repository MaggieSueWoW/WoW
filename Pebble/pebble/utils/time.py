from __future__ import annotations
from datetime import datetime, timezone, timedelta
import zoneinfo
from dateutil import parser

PT = zoneinfo.ZoneInfo("America/Los_Angeles")


def ms_to_dt_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def utc_to_pt(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(PT)


def ms_to_pt(ms: int) -> datetime:
    return utc_to_pt(ms_to_dt_utc(ms))


def ms_to_pt_iso(ms: int) -> str:
    """Return ISO-8601 string of the given epoch ms in PT."""
    return ms_to_pt(ms).isoformat()


def ms_to_pt_sheets(ms: int) -> str:
    """Return a PT datetime string Google Sheets parses natively.

    The format produced is ``YYYY-MM-DD HH:MM:SS`` so Sheets interprets the
    value as a real datetime rather than plain text.
    """
    return ms_to_pt(ms).strftime("%Y-%m-%d %H:%M:%S")


def night_id_from_ms(ms: int) -> str:
    # Night ID = local PT calendar date (YYYY-MM-DD) of the night start
    return ms_to_pt(ms).strftime("%Y-%m-%d")


def pt_iso_to_ms(txt: str) -> int | None:
    """Parse a PT datetime string in any Google Sheets format into epoch ms.

    Returns ``None`` if parsing fails or the input is falsy.
    """
    if not txt:
        return None
    try:
        dt = parser.parse(txt)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=PT)
    else:
        dt = dt.astimezone(PT)
    return int(dt.timestamp() * 1000)


def sheets_date_str(txt: str) -> str:
    """Normalize a Sheets date string to ``YYYY-MM-DD`` in PT.

    Returns an empty string if parsing fails or the input is falsy.
    """
    if not txt:
        return ""
    try:
        dt = parser.parse(txt)
    except Exception:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=PT)
    else:
        dt = dt.astimezone(PT)
    return dt.strftime("%Y-%m-%d")


def pt_time_to_ms(txt: str, ref_ms: int) -> int | None:
    """Convert a PT date/time string to epoch ms using ``ref_ms`` as reference.

    ``txt`` may be any format Google Sheets produces for dates or times.  The
    returned timestamp is the first occurrence of the parsed time on or after
    ``ref_ms``. ``None`` is returned if parsing fails or the adjusted time falls
    more than 24 hours after ``ref_ms``.
    """
    if not txt:
        return None

    dt_ref = ms_to_pt(ref_ms)
    try:
        dt = parser.parse(txt, default=dt_ref)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=PT)
    else:
        dt = dt.astimezone(PT)

    while dt.timestamp() * 1000 < ref_ms:
        dt += timedelta(hours=12)
        if dt.timestamp() * 1000 - ref_ms > 24 * 3600 * 1000:
            return None

    if dt.timestamp() * 1000 - ref_ms > 24 * 3600 * 1000:
        return None

    return int(dt.timestamp() * 1000)
