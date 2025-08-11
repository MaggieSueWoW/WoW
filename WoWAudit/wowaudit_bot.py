import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
import yaml
from pymongo import MongoClient

import common.misc
import common.request_helpers
from common.request_helpers import ONE_HOUR_IN_SECONDS, retry

# TODO: add a longer delay or some backoff on restarting the container
# TODO: config a backup task for the db from the databases volume

APP_NAME = "WOWAuditBot v0.1"

PARSER = argparse.ArgumentParser(description=APP_NAME)
PARSER.add_argument("--loop", action="store_true", help="loop to keep pulling and storing data")
PARSER.add_argument("--config_file", help="config file", required=True)
PARSER.add_argument("--season", help="current season (S1, S2, ...)", required=True)
PARSER.add_argument("--expansion", help="current expansion (DF, TWW, ...)", required=True)
ARGS = PARSER.parse_args()

metadata_cols = {
    "guild": 1,
    "team_realm": 2,
    "team_region": 3,
    "team_name": 7,
}

WOWAUDIT_TIMESTAMP_COL = 9

# WoWAudit... because a separate row for the metadata would have been a bridge too far.
corrected_field_names = [
    "name",
    "class",
    "realm",
    "ilvl",
    "gender",
    "faction",
    "head_ilvl",
    "head_id",
    "head_name",
    "head_quality",
    "neck_ilvl",
]

MEASUREMENT_NAME = "character_data"


def sort_metadata(metadata):
    return dict(sorted(metadata.items()))


def build_doc(tags, field_names, timestamp, row):
    d = {
        "metadata": tags.copy(),
        "timestamp": timestamp,
    }

    for name, value in zip(field_names, row):
        if name is not None and len(name):
            if value is not None and (not isinstance(value, str) or value != ""):
                d[name] = value

    d["metadata"]["name"] = d["name"]
    d["metadata"]["realm"] = d["realm"]
    d["metadata"] = sort_metadata(d["metadata"])
    return d


def wowaudit_to_docs(d):
    field_names = d[0]

    tags = {
        "expansion": ARGS.expansion,
        "season": ARGS.season,
    }

    for tag_name, col in metadata_cols.items():
        tags[tag_name] = field_names[col]

    dt = datetime.strptime(field_names[WOWAUDIT_TIMESTAMP_COL], "%Y-%m-%d %H:%M:%S %z")
    dt_utc = dt.astimezone(timezone.utc)
    field_names[: len(corrected_field_names)] = corrected_field_names
    field_names = [str(n) for n in field_names]  # Bad data: sometimes they put numbers as headers for no good reason

    docs = []
    for row in d[1:]:
        d = build_doc(tags, field_names, dt_utc, row)
        docs.append(d)

    return docs


@retry(
    exceptions=(requests.Timeout, requests.HTTPError, ConnectionError, TimeoutError),
    delay=1.0,
    times=3,
)
def get_wowaudit(sheet_key):
    url = f"https://data.wowaudit.com/dragonflight/{sheet_key}.json"
    headers = {"Cache-Control": "max-age=0", "Content-Type": "application/json"}
    logging.info("Fetching %s %s", url, headers)
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_wowaudit_collection(db):
    collection_name = "wowaudit_hourly"

    if collection_name in db.list_collection_names():
        logging.debug(f"Collection '{collection_name}' already exists.")
        return db[collection_name]

    timeseries_options = {
        "timeseries": {
            "timeField": "timestamp",
            "metaField": "metadata",
            "granularity": "hours",
        }
    }
    db.create_collection(collection_name, **timeseries_options)
    logging.debug(f"Time-series collection '{collection_name}' created successfully.")
    return db[collection_name]


def store_in_mongo(collection, data):
    docs = wowaudit_to_docs(data)
    logging.info(f"Processing {len(docs)} docs from {docs[0]['timestamp']} -- {docs[0]['metadata']}")

    bulk_inserts = []
    for doc in docs:
        existing_doc = collection.find_one(
            {
                "metadata": sort_metadata(doc["metadata"]),
                "timestamp": doc["timestamp"],
            }
        )
        if not existing_doc:
            bulk_inserts.append(doc)

    if bulk_inserts:
        collection.insert_many(bulk_inserts)
        logging.info(f"Inserted {len(bulk_inserts)} new documents.")
    else:
        logging.info("No new documents to insert.")


def main():
    common.misc.setup_logging_and_temp(APP_NAME, "WoWAudit Bot", "wowaudit_bot")
    logging.info("ARGS: " + str(vars(ARGS)))

    if not os.path.exists(ARGS.config_file):
        raise ValueError("config file does not exists {}".format(ARGS.config_file))
    with open(ARGS.config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        logging.info("Config file %s: %s" % (ARGS.config_file, str(config)))

    logging.info("Connecting to MongoDB...")
    uri = config["mongodb_uri"]
    mongo_client = MongoClient(uri)
    logging.debug(mongo_client.admin.command({"ping": 1}))
    logging.debug(mongo_client.server_info())
    db = mongo_client["wowaudit_database"]
    collection = get_wowaudit_collection(db)

    while True:
        data = get_wowaudit(config["wowaudit_sheet_key"])
        store_in_mongo(collection, data)

        if not ARGS.loop:
            break

        delay = ONE_HOUR_IN_SECONDS
        logging.info("Waiting %ds..." % delay)
        time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except:
        logging.exception("Unexpected error, exiting.", exc_info=True)
        exit(-1)
