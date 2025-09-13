from __future__ import annotations
from typing import List, Optional, Tuple, Dict, Any


def detect_break(
    all_fights: List[dict],
    *,
    window_start_min: int = 30,
    window_end_min: int = 120,
    min_break_min: int = 10,
    max_break_min: int = 30,
) -> Tuple[Optional[Tuple[int, int]], Dict[str, Any]]:
    """Identify the raid break window.

    Only boss fights (``encounter_id`` > 0) are considered when determining
    the break. Returns ``(break_range, meta)`` where ``break_range`` is a
    tuple of ``(start_ms, end_ms)`` or ``None`` if no candidate satisfied the
    criteria. ``meta`` contains the largest candidate gap (in minutes) and a
    list of all candidate gaps whose midpoints fell within the configured
    window.
    """
    if not all_fights:
        return None, {"largest_gap_min": 0, "candidates": []}
    # Filter to boss pulls only. A valid boss fight has a positive encounter id.
    fights = [f for f in all_fights if f.get("encounter_id", 0) > 0]
    if not fights:
        return None, {"largest_gap_min": 0, "candidates": []}
    fights = sorted(fights, key=lambda f: f["fight_abs_start_ms"])  # expects absolute times
    night0 = fights[0]["fight_abs_start_ms"]

    best = None
    best_gap = 0.0
    candidates: List[Dict[str, Any]] = []
    for a, b in zip(fights, fights[1:]):
        gap = (b["fight_abs_start_ms"] - a["fight_abs_end_ms"]) / 60000.0  # minutes
        if gap <= 0:
            continue
        mid_min = (
            ((a["fight_abs_end_ms"] + b["fight_abs_start_ms"]) / 2 - night0)
            / 60000.0
        )
        if window_start_min <= mid_min <= window_end_min:
            cand = {
                "start_ms": a["fight_abs_end_ms"],
                "end_ms": b["fight_abs_start_ms"],
                "gap_min": gap,
            }
            candidates.append(cand)
            if gap > best_gap:
                best_gap = gap
                best = (cand["start_ms"], cand["end_ms"])

    largest_gap = max((c["gap_min"] for c in candidates), default=0.0)

    if not best:
        return None, {"largest_gap_min": largest_gap, "candidates": candidates}

    start, end = best
    length_min = (end - start) / 60000.0
    if length_min < min_break_min or length_min > max_break_min:
        return None, {"largest_gap_min": largest_gap, "candidates": candidates}
    return (start, end), {"largest_gap_min": largest_gap, "candidates": candidates}
