from __future__ import annotations
from typing import Dict, Iterable, List, Optional, Set

# Availability inference policy (V2):
# - If a player has *any* block in pre, we infer availability for the *entire* post (benched when not playing).
# - If they have any block in post, we infer availability for the entire pre.
# - If present in last non‑Mythic fight pre‑switch, treat as available for full envelope.
# - Being on the roster alone **does not** imply availability; rostered no‑shows are
#   assumed to be on vacation and receive no bench credit unless officers
#   explicitly override their availability.
# - Officers can override via Availability Overrides sheet; overrides win.


def bench_minutes_for_night(
    blocks: List[dict],
    pre_ms: int,
    post_ms: int,
    *,
    overrides: Optional[Dict[str, Dict[str, Optional[bool]]]] = None,
    last_fight_mains: Iterable[str] | None = None,
    roster_map: Optional[Dict[str, str]] = None,
) -> List[dict]:
    """Aggregate bench/played minutes for a night.

    ``pre_ms`` and ``post_ms`` are the durations of the pre- and post-break
    halves of the night expressed in milliseconds.  ``overrides`` is an
    optional mapping of ``main`` → ``{"pre": bool|None, "post": bool|None}``
    representing officer availability overrides per half. ``last_fight_mains``
    lists mains who appeared in the final non‑Mythic fight and are therefore
    treated as available for the entire night. ``roster_map`` maps alt names to
    main names.
    """

    roster_map = roster_map or {}
    overrides = overrides or {}
    last_fight_mains = set(last_fight_mains or [])

    from collections import defaultdict

    # aggregate playtime per main+half in milliseconds
    agg = defaultdict(lambda: {"pre": 0, "post": 0})
    for b in blocks:
        main = roster_map.get(b["main"], b["main"])
        duration = b["end_ms"] - b["start_ms"]
        agg[main][b["half"]] += duration

    # include mains referenced only in overrides or last fight
    all_mains = set(agg.keys()) | set(overrides.keys()) | last_fight_mains

    out: List[dict] = []
    pre_full = pre_ms
    post_full = post_ms
    for main in sorted(all_mains):
        halves = agg.get(main, {})
        pre_played_ms = halves.get("pre", 0)
        post_played_ms = halves.get("post", 0)

        # infer availability
        pre_avail = pre_played_ms > 0 or post_played_ms > 0
        post_avail = pre_played_ms > 0 or post_played_ms > 0

        if main in last_fight_mains:
            pre_avail = True
            post_avail = True

        ov = overrides.get(main)
        if ov:
            if ov.get("pre") is not None:
                pre_avail = bool(ov["pre"])
            if ov.get("post") is not None:
                post_avail = bool(ov["post"])

        pre_bench_ms = max(0, pre_full - pre_played_ms) if pre_avail else 0
        post_bench_ms = max(0, post_full - post_played_ms) if post_avail else 0

        played_pre_min = pre_played_ms // 60000
        played_post_min = post_played_ms // 60000
        bench_pre_min = pre_bench_ms // 60000
        bench_post_min = post_bench_ms // 60000

        played_total_min = played_pre_min + played_post_min
        bench_total_min = bench_pre_min + bench_post_min

        # Determine status source: override > last_fight > blocks > none
        status_source = "none"
        if ov and (ov.get("pre") is not None or ov.get("post") is not None):
            status_source = "override"
        elif main in last_fight_mains:
            status_source = "last_fight"
        elif pre_played_ms > 0 or post_played_ms > 0:
            status_source = "blocks"

        out.append(
            {
                "main": main,
                "bench_pre_min": bench_pre_min,
                "bench_post_min": bench_post_min,
                "bench_total_min": bench_total_min,
                "played_pre_min": played_pre_min,
                "played_post_min": played_post_min,
                "played_total_min": played_total_min,
                "avail_pre": pre_avail,
                "avail_post": post_avail,
                "status_source": status_source,
            }
        )

    return out


def last_non_mythic_boss_mains(
    fights_all: List[dict],
    mythic_start_ms: int,
    roster_map: Optional[Dict[str, str]] = None,
) -> Set[str]:
    """Return mains who appeared in the last non-Mythic boss fight before Mythic."""

    roster_map = roster_map or {}
    # Only consider non-Mythic fights with a valid encounter id (boss pulls).
    non_mythic_pre = [
        f
        for f in fights_all
        if not f.get("is_mythic")
        and f.get("encounter_id", 0) > 0
        and f.get("fight_abs_start_ms", 0) < mythic_start_ms
    ]
    if not non_mythic_pre:
        return set()

    last_nm = max(non_mythic_pre, key=lambda f: f.get("fight_abs_start_ms", 0))
    return {
        roster_map.get(p.get("name"), p.get("name"))
        for p in last_nm.get("participants", [])
        if p.get("name")
    }
