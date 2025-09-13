import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from pebble.blocks import build_blocks


def test_blocks_split_on_non_mythic_boss():
    rows = [
        {"main": "A", "night_id": "N1", "start_ms": 0, "end_ms": 1000},
        {"main": "A", "night_id": "N1", "start_ms": 3000, "end_ms": 4000},
    ]
    fights_all = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 1000, "is_mythic": True, "encounter_id": 1},
        {"fight_abs_start_ms": 1500, "fight_abs_end_ms": 2500, "is_mythic": False, "encounter_id": 2},
        {"fight_abs_start_ms": 3000, "fight_abs_end_ms": 4000, "is_mythic": True, "encounter_id": 3},
    ]
    blocks = build_blocks(rows, break_range=None, fights_all=fights_all)
    assert len(blocks) == 2


def test_trash_does_not_split_blocks():
    rows = [
        {"main": "A", "night_id": "N1", "start_ms": 0, "end_ms": 1000},
        {"main": "A", "night_id": "N1", "start_ms": 3000, "end_ms": 4000},
    ]
    fights_all = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 1000, "is_mythic": True, "encounter_id": 1},
        {"fight_abs_start_ms": 1500, "fight_abs_end_ms": 2500, "is_mythic": False, "encounter_id": 0},
        {"fight_abs_start_ms": 3000, "fight_abs_end_ms": 4000, "is_mythic": True, "encounter_id": 2},
    ]
    blocks = build_blocks(rows, break_range=None, fights_all=fights_all)
    assert len(blocks) == 1
    assert blocks[0]["start_ms"] == 0
    assert blocks[0]["end_ms"] == 4000


def test_long_gap_does_not_split_blocks():
    rows = [
        {"main": "A", "night_id": "N1", "start_ms": 0, "end_ms": 1000},
        {
            "main": "A",
            "night_id": "N1",
            "start_ms": 11 * 60 * 1000 + 1000,
            "end_ms": 11 * 60 * 1000 + 2000,
        },
    ]
    fights_all = [
        {"fight_abs_start_ms": 0, "fight_abs_end_ms": 1000, "is_mythic": True, "encounter_id": 1},
        {
            "fight_abs_start_ms": 11 * 60 * 1000 + 1000,
            "fight_abs_end_ms": 11 * 60 * 1000 + 2000,
            "is_mythic": True,
            "encounter_id": 2,
        },
    ]
    blocks = build_blocks(rows, break_range=None, fights_all=fights_all)
    assert len(blocks) == 1
    assert blocks[0]["start_ms"] == 0
    assert blocks[0]["end_ms"] == 11 * 60 * 1000 + 2000
