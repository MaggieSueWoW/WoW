import sys
from pathlib import Path

import mongomock

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pebble.mongo_client import ensure_indexes
from pebble.ingest import canonical_fight_key


def _upsert(
    db,
    encounter_id,
    difficulty,
    start_ms,
    end_ms,
    report_code,
    fight_id,
    participants=None,
):
    key = canonical_fight_key(
        {"encounterID": encounter_id, "difficulty": difficulty}, start_ms, end_ms
    )
    doc = {
        **key,
        "report_code": report_code,
        "id": fight_id,
        "fight_abs_start_ms": start_ms,
        "fight_abs_end_ms": end_ms,
    }
    update = {"$setOnInsert": doc}
    if participants:
        update["$addToSet"] = {"participants": {"$each": participants}}
    db["fights_all"].update_one(key, update, upsert=True)


def test_overlapping_reports_deduped():
    client = mongomock.MongoClient()
    db = client.db
    ensure_indexes(db)

    encounter_id = 123
    diff = 5
    start1 = 1_000_000
    end1 = 1_060_000
    start2 = start1 + 40  # same fight in another report, slight drift
    end2 = end1 + 40

    _upsert(db, encounter_id, diff, start1, end1, "R1", 1)
    _upsert(db, encounter_id, diff, start2, end2, "R2", 2)

    fights = list(db["fights_all"].find())
    assert len(fights) == 1
    assert (
        fights[0]["start_rounded_ms"]
        == canonical_fight_key(
            {"encounterID": encounter_id, "difficulty": diff}, start1, end1
        )["start_rounded_ms"]
    )
    # first report's metadata retained
    assert fights[0]["report_code"] == "R1"


def test_close_fights_no_collision():
    client = mongomock.MongoClient()
    db = client.db
    ensure_indexes(db)

    encounter_id = 123
    diff = 5
    start1 = 1_000_000
    end1 = 1_060_000
    start2 = start1 + 400  # distinct fight starting shortly after
    end2 = end1 + 400

    _upsert(db, encounter_id, diff, start1, end1, "R1", 1)
    _upsert(db, encounter_id, diff, start2, end2, "R1", 2)

    fights = list(db["fights_all"].find())
    assert len(fights) == 2


def test_participants_upsert_no_conflict():
    client = mongomock.MongoClient()
    db = client.db
    ensure_indexes(db)

    encounter_id = 123
    diff = 5
    start = 1_000_000
    end = 1_060_000

    p1 = [{"actor_id": 1, "name": "A", "class": "Mage", "server": "S"}]
    p2 = [{"actor_id": 2, "name": "B", "class": "Druid", "server": "S"}]

    _upsert(db, encounter_id, diff, start, end, "R1", 1, participants=p1)
    _upsert(db, encounter_id, diff, start, end, "R1", 1, participants=p2)

    doc = db["fights_all"].find_one(
        canonical_fight_key({"encounterID": encounter_id, "difficulty": diff}, start, end)
    )
    assert {p["actor_id"] for p in doc["participants"]} == {1, 2}
