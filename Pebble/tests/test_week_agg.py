import mongomock

from pebble.week_agg import (
    materialize_rankings,
    materialize_week_totals,
    week_id_from_night_id,
)


def test_week_id_maps_to_tuesday():
    assert week_id_from_night_id("2024-07-04") == "2024-07-02"
    assert week_id_from_night_id("2024-07-02") == "2024-07-02"


def test_materialize_week_totals_fills_roster():
    db = mongomock.MongoClient().db
    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-02",
                "main": "Alice-Illidan",
                "played_pre_min": 5,
                "played_post_min": 5,
                "bench_pre_min": 5,
                "bench_post_min": 5,
            },
            {
                "night_id": "2024-07-04",
                "main": "Bob-Illidan",
                "played_pre_min": 10,
                "played_post_min": 10,
                "bench_pre_min": 0,
                "bench_post_min": 0,
            },
        ]
    )
    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "join_night": "2024-06-25"},
            {"main": "Bob-Illidan", "join_night": "2024-06-25"},
            {"main": "Charlie-Illidan", "join_night": "2024-06-25"},
            {
                "main": "Eve-Illidan",
                "join_night": "2024-07-09",
            },  # joins later
            {
                "main": "Frank-Illidan",
                "join_night": "2024-06-18",
                "leave_night": "2024-06-25",
            },  # left before
        ]
    )

    count = materialize_week_totals(db)
    rows = list(
        db["bench_week_totals"].find(
            {},
            {
                "_id": 0,
                "game_week": 1,
                "main": 1,
                "played_min": 1,
                "bench_min": 1,
                "bench_pre_min": 1,
                "bench_post_min": 1,
            },
        )
    )
    assert count == 3
    assert sorted(rows, key=lambda r: r["main"]) == [
        {
            "game_week": "2024-07-02",
            "main": "Alice-Illidan",
            "played_min": 10,
            "bench_min": 10,
            "bench_pre_min": 5,
            "bench_post_min": 5,
        },
        {
            "game_week": "2024-07-02",
            "main": "Bob-Illidan",
            "played_min": 20,
            "bench_min": 0,
            "bench_pre_min": 0,
            "bench_post_min": 0,
        },
        {
            "game_week": "2024-07-02",
            "main": "Charlie-Illidan",
            "played_min": 0,
            "bench_min": 0,
            "bench_pre_min": 0,
            "bench_post_min": 0,
        },
    ]

    # rankings
    rc = materialize_rankings(db)
    ranks = list(
        db["bench_rankings"].find(
            {}, {"_id": 0, "rank": 1, "main": 1, "bench_min": 1}
        ).sort([("rank", 1)])
    )
    assert rc == 3
    assert ranks == [
        {"rank": 1, "main": "Alice-Illidan", "bench_min": 10},
        {"rank": 2, "main": "Bob-Illidan", "bench_min": 0},
        {"rank": 3, "main": "Charlie-Illidan", "bench_min": 0},
    ]


def test_materialize_week_totals_removes_stale_players():
    db = mongomock.MongoClient().db
    db["bench_night_totals"].insert_many(
        [
            {
                "night_id": "2024-07-02",
                "main": "Alice-Illidan",
                "played_pre_min": 5,
                "played_post_min": 5,
                "bench_pre_min": 5,
                "bench_post_min": 5,
            },
            {
                "night_id": "2024-07-02",
                "main": "Bob-Illidan",
                "played_pre_min": 10,
                "played_post_min": 10,
                "bench_pre_min": 0,
                "bench_post_min": 0,
            },
        ]
    )
    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "join_night": "2024-06-25"},
            {"main": "Bob-Illidan", "join_night": "2024-06-25"},
        ]
    )
    materialize_week_totals(db)
    # Now remove Bob from sources
    db["bench_night_totals"].delete_many({"main": "Bob-Illidan"})
    db["team_roster"].delete_many({"main": "Bob-Illidan"})
    materialize_week_totals(db)
    rows = list(db["bench_week_totals"].find({}, {"_id": 0}))
    assert len(rows) == 1
    rec = rows[0]
    assert rec["game_week"] == "2024-07-02"
    assert rec["main"] == "Alice-Illidan"
    assert rec["played_min"] == 10
    assert rec["bench_min"] == 10
    assert rec["bench_pre_min"] == 5
    assert rec["bench_post_min"] == 5


def test_materialize_rankings_removes_stale_players():
    db = mongomock.MongoClient().db
    db["bench_week_totals"].insert_many(
        [
            {"game_week": "2024-07-02", "main": "Alice-Illidan", "bench_min": 10},
            {"game_week": "2024-07-02", "main": "Bob-Illidan", "bench_min": 0},
        ]
    )
    db["team_roster"].insert_many(
        [
            {"main": "Alice-Illidan", "join_night": "2024-06-25"},
            {"main": "Bob-Illidan", "join_night": "2024-06-25"},
        ]
    )
    materialize_rankings(db)
    db["bench_week_totals"].delete_many({"main": "Bob-Illidan"})
    materialize_rankings(db)
    ranks = list(
        db["bench_rankings"].find({}, {"_id": 0, "rank": 1, "main": 1, "bench_min": 1})
    )
    assert ranks == [
        {"rank": 1, "main": "Alice-Illidan", "bench_min": 10}
    ]


def test_materialize_rankings_skips_non_roster_players():
    db = mongomock.MongoClient().db
    db["bench_week_totals"].insert_many(
        [
            {"game_week": "2024-07-02", "main": "Alice-Illidan", "bench_min": 10},
            {"game_week": "2024-07-02", "main": "Merc-Illidan", "bench_min": 5},
        ]
    )
    db["team_roster"].insert_one(
        {"main": "Alice-Illidan", "join_night": "2024-06-25", "active": True}
    )
    rc = materialize_rankings(db)
    ranks = list(
        db["bench_rankings"].find({}, {"_id": 0, "rank": 1, "main": 1, "bench_min": 1})
    )
    assert rc == 1
    assert ranks == [
        {"rank": 1, "main": "Alice-Illidan", "bench_min": 10}
    ]
