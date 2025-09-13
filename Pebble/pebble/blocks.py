from __future__ import annotations
from typing import List, Dict, Tuple
from .utils.time import ms_to_pt_iso


def build_blocks(
    participation_rows: List[dict], *, break_range: tuple[int, int] | None, fights_all: List[dict] | None = None
) -> List[dict]:
    """Collapse per‑fight rows into contiguous blocks per (main, night_id, half).
    Trash between fights does not split blocks, regardless of time spent.
    Only non‑Mythic boss fights occurring between Mythic pulls break blocks.
    """
    if not participation_rows:
        return []

    # group by main+night
    from collections import defaultdict

    groups: Dict[tuple, list] = defaultdict(list)
    for r in participation_rows:
        groups[(r["main"], r["night_id"])].append(r)

    # Pre-compute non-Mythic boss intervals for block splitting
    nm_boss_intervals: List[Tuple[int, int]] = []
    if fights_all:
        for f in fights_all:
            if not f.get("is_mythic") and f.get("encounter_id", 0) > 0:
                nm_boss_intervals.append(
                    (f.get("fight_abs_start_ms", 0), f.get("fight_abs_end_ms", 0))
                )

    def has_nm_boss_between(s: int, e: int) -> bool:
        for bs, be in nm_boss_intervals:
            if s <= bs and be <= e:
                return True
        return False

    blocks: List[dict] = []
    for (main, night), rows in groups.items():
        rows.sort(key=lambda r: r["start_ms"])
        current = None
        for r in rows:
            half = None
            if break_range:
                bs, be = break_range
                mid = (r["start_ms"] + r["end_ms"]) // 2
                half = "pre" if mid < bs else "post"
            else:
                half = "pre"

            if (
                current
                and current["half"] == half
                and not has_nm_boss_between(current["end_ms"], r["start_ms"])
            ):
                current["end_ms"] = max(current["end_ms"], r["end_ms"])
                current["end_pt"] = ms_to_pt_iso(current["end_ms"])
            else:
                if current:
                    blocks.append(current)
                current = {
                    "main": main,
                    "night_id": night,
                    "half": half,
                    "start_ms": r["start_ms"],
                    "end_ms": r["end_ms"],
                    "start_pt": ms_to_pt_iso(r["start_ms"]),
                    "end_pt": ms_to_pt_iso(r["end_ms"]),
                }
        if current:
            blocks.append(current)
    return blocks
