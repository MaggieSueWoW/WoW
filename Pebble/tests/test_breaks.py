from pebble.breaks import detect_break


def test_detect_break_candidates():
    fights = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 600000, "encounter_id": 1},
        {"fight_abs_start_ms": 1200000, "fight_abs_end_ms": 1800000, "encounter_id": 2},
        {"fight_abs_start_ms": 1800000, "fight_abs_end_ms": 2000000, "encounter_id": 0},  # trash
        {"fight_abs_start_ms": 3000000, "fight_abs_end_ms": 3600000, "encounter_id": 3},
    ]
    br, meta = detect_break(
        fights,
        window_start_min=0,
        window_end_min=60,
        min_break_min=5,
        max_break_min=30,
    )
    assert br == (1800000, 3000000)
    assert meta["largest_gap_min"] == 20
    assert len(meta["candidates"]) == 2
