from __future__ import annotations
from typing import Optional, Tuple, List


def mythic_envelope(fights_mythic: List[dict]) -> Optional[Tuple[int, int]]:
    if not fights_mythic:
        return None
    s = min(f["fight_abs_start_ms"] for f in fights_mythic)
    e = max(f["fight_abs_end_ms"] for f in fights_mythic)
    return (s, e)


def split_pre_post(envelope: Tuple[int, int], break_range):
    s, e = envelope
    if not break_range:
        return {"pre_ms": e - s, "post_ms": 0}
    bs, be = break_range
    pre = max(0, min(bs, e) - s)
    post = max(0, e - max(be, s))
    return {"pre_ms": pre, "post_ms": post}
