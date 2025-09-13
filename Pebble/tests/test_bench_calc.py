import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from pebble.bench_calc import bench_minutes_for_night, last_non_mythic_boss_mains


def test_bench_minutes_uses_split_durations():
    blocks = [
        {"main": "A-Illidan", "half": "pre", "start_ms": 0, "end_ms": 5 * 60000},
        {"main": "A-Illidan", "half": "post", "start_ms": 0, "end_ms": 10 * 60000},
        {"main": "B-Illidan", "half": "post", "start_ms": 0, "end_ms": 10 * 60000},
    ]
    res = bench_minutes_for_night(blocks, pre_ms=10 * 60000, post_ms=20 * 60000)
    res_by_main = {r["main"]: r for r in res}

    a = res_by_main["A-Illidan"]
    assert a["played_pre_min"] == 5
    assert a["played_post_min"] == 10
    assert a["played_total_min"] == 15
    assert a["bench_pre_min"] == 5  # 10 - 5
    assert a["bench_post_min"] == 10  # 20 - 10
    assert a["bench_total_min"] == 15
    assert a["avail_pre"] is True
    assert a["avail_post"] is True
    assert a["status_source"] == "blocks"

    b = res_by_main["B-Illidan"]
    assert b["played_pre_min"] == 0
    assert b["played_post_min"] == 10
    assert b["played_total_min"] == 10
    assert b["bench_pre_min"] == 10  # pre half inferred available via post
    assert b["bench_post_min"] == 10
    assert b["bench_total_min"] == 20
    assert b["avail_pre"] is True
    assert b["avail_post"] is True
    assert b["status_source"] == "blocks"


def test_last_fight_overrides_and_roster_map():
    blocks = [
        {"main": "Alt1-Illidan", "half": "pre", "start_ms": 0, "end_ms": 5 * 60000},
    ]
    roster_map = {"Alt1-Illidan": "Main1-Illidan"}
    overrides = {"Main1-Illidan": {"post": False}}
    last_fight_mains = {"Main1-Illidan", "Main2-Illidan"}
    res = bench_minutes_for_night(
        blocks,
        pre_ms=10 * 60000,
        post_ms=10 * 60000,
        overrides=overrides,
        last_fight_mains=last_fight_mains,
        roster_map=roster_map,
    )
    res_by_main = {r["main"]: r for r in res}

    m1 = res_by_main["Main1-Illidan"]
    assert m1["played_pre_min"] == 5
    assert m1["played_post_min"] == 0
    assert m1["played_total_min"] == 5
    assert m1["bench_pre_min"] == 5  # available via last fight
    assert m1["bench_post_min"] == 0  # override removes post availability
    assert m1["bench_total_min"] == 5
    assert m1["avail_pre"] is True
    assert m1["avail_post"] is False
    assert m1["status_source"] == "override"

    m2 = res_by_main["Main2-Illidan"]  # no blocks but in last fight
    assert m2["played_pre_min"] == 0
    assert m2["played_post_min"] == 0
    assert m2["played_total_min"] == 0
    assert m2["bench_pre_min"] == 10
    assert m2["bench_post_min"] == 10
    assert m2["bench_total_min"] == 20
    assert m2["avail_pre"] is True
    assert m2["avail_post"] is True
    assert m2["status_source"] == "last_fight"


def test_no_show_roster_player_excluded():
    res = bench_minutes_for_night([], pre_ms=10 * 60000, post_ms=10 * 60000)
    assert res == []


def test_override_adds_no_show_player():
    overrides = {"Main-Illidan": {"pre": True, "post": True}}
    res = bench_minutes_for_night(
        [],
        pre_ms=10 * 60000,
        post_ms=10 * 60000,
        overrides=overrides,
    )
    assert len(res) == 1
    m = res[0]
    assert m["main"] == "Main-Illidan"
    assert m["played_pre_min"] == 0
    assert m["played_post_min"] == 0
    assert m["bench_pre_min"] == 10
    assert m["bench_post_min"] == 10
    assert m["bench_total_min"] == 20
    assert m["avail_pre"] is True
    assert m["avail_post"] is True
    assert m["status_source"] == "override"


def test_last_non_mythic_boss_mains_excludes_trash():
    fights_all = [
        {
            "is_mythic": False,
            "encounter_id": 123,
            "fight_abs_start_ms": 1000,
            "participants": [{"name": "Alt-Illidan"}],
        },
        {
            "is_mythic": False,
            "encounter_id": 0,  # trash after boss
            "fight_abs_start_ms": 1500,
            "participants": [{"name": "Trash-Illidan"}],
        },
    ]
    roster_map = {"Alt-Illidan": "Main-Illidan"}
    mains = last_non_mythic_boss_mains(
        fights_all, mythic_start_ms=2000, roster_map=roster_map
    )
    assert mains == {"Main-Illidan"}
