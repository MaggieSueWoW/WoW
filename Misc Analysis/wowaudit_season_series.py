#!/usr/bin/env python3
"""
Build season-aligned time series for any numeric field from WoWAudit hourly data.

New:
- --config seasons.yaml to load season starts per expansion
- --env-file .env to load MONGODB_URI when --mongo not provided

Example:
  python wowaudit_season_series.py \
    --config seasons.yaml --env-file .env \
    --db wowaudit_database --coll wowaudit_hourly \
    --field "season_mythic_dungeons" \
    --names Nyph \
    --expansion TWW \
    --freq D --agg last --tz America/Los_Angeles \
    --out nyph_season_mythic_dungeons_by_season.csv
"""

from __future__ import annotations
import argparse
from typing import Dict, List, Optional
import sys
import pandas as pd
from pymongo import MongoClient
from dateutil import parser as dtparser
import pytz
import yaml
from dotenv import load_dotenv
import os

SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600


def load_season_starts_from_yaml(config_path: str,
                                 expansion: str,
                                 default_tz: Optional[str]) -> Dict[str, pd.Timestamp]:
    """
    YAML schema:
    expansions:
      <EXP>:
        timezone: America/Los_Angeles   # optional
        seasons:
          S1: "2024-08-26T07:00:00-07:00"
          S2: "2024-11-18T07:00:00-08:00"
    """
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    exps = cfg.get("expansions") or {}
    if expansion not in exps:
        raise ValueError(f"Expansion '{expansion}' not found in {config_path}. "
                         f"Available: {', '.join(exps.keys()) or '(none)'}")

    block = exps[expansion] or {}
    seasons = block.get("seasons") or {}
    if not seasons:
        raise ValueError(f"No 'seasons' defined for expansion '{expansion}' in {config_path}.")

    tz_for_exp = block.get("timezone") or default_tz
    tzinfo = pytz.timezone(tz_for_exp) if tz_for_exp else None

    out: Dict[str, pd.Timestamp] = {}
    for key, value in seasons.items():
        ts = dtparser.isoparse(str(value))
        if ts.tzinfo is None:
            if not tzinfo:
                raise ValueError(f"Naive datetime for {key} but no timezone provided in YAML or --tz.")
            ts = tzinfo.localize(ts)
        out[key] = pd.Timestamp(ts)
    return out


def get_mongo_uri(cli_mongo: Optional[str], env_file: Optional[str]) -> str:
    if cli_mongo:
        return cli_mongo
    # Load from .env
    if env_file:
        load_dotenv(env_file)
    else:
        load_dotenv()  # default .env if present
    uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MongoDB URI not provided. Use --mongo or set MONGODB_URI in your .env.")
    return uri


def build_query(field: str,
                names: Optional[List[str]],
                team: Optional[str],
                expansion: Optional[str]) -> Dict:
    q: Dict = {
        field: {"$exists": True},
        "metadata.season": {"$exists": True},
        "metadata.name": {"$exists": True},
        "timestamp": {"$exists": True},
    }
    if names:
        q["metadata.name"] = {"$in": names}
    if team:
        q["metadata.team_name"] = team
    if expansion:
        q["metadata.expansion"] = expansion
    return q


def fetch_df(mongo_uri: str, db: str, coll: str, query: Dict, field: str) -> pd.DataFrame:
    client = MongoClient(mongo_uri, tz_aware=True)
    try:
        projection = {
            "_id": 0,
            "timestamp": 1,
            field: 1,
            "metadata.season": 1,
            "metadata.name": 1,
            "metadata.expansion": 1,
            "metadata.team_name": 1,
        }
        rows = list(client[db][coll].find(query, projection=projection))
    finally:
        client.close()

    if not rows:
        return pd.DataFrame(columns=["timestamp", field, "season", "name", "expansion", "team"])

    df = pd.DataFrame(rows)
    md = df["metadata"].apply(pd.Series)
    df = pd.concat(
        [df.drop(columns=["metadata"]),
         md.rename(columns={"season": "season", "name": "name",
                            "expansion": "expansion", "team_name": "team"})],
        axis=1,
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df[["timestamp", field, "season", "name", "expansion", "team"]]


def coerce_numeric(df: pd.DataFrame, field: str) -> pd.DataFrame:
    df = df.copy()
    df[field] = pd.to_numeric(df[field], errors="coerce")
    return df.dropna(subset=[field])


def iqr_filter(df: pd.DataFrame, field: str, k: float) -> pd.DataFrame:
    if df.empty or k <= 0:
        return df
    q1 = df[field].quantile(0.25)
    q3 = df[field].quantile(0.75)
    iqr = q3 - q1
    if iqr <= 0:
        return df
    lo, hi = q1 - k * iqr, q3 + k * iqr
    return df[(df[field] >= lo) & (df[field] <= hi)]


def attach_elapsed_seconds(df: pd.DataFrame,
                           season_starts: Dict[str, pd.Timestamp],
                           tz: Optional[str]) -> pd.DataFrame:
    """
    Make both timestamps tz-aware and subtract as Series (no .values).
    We convert both sides to UTC to avoid tz-mismatch edge cases.
    """
    if df.empty:
        return df

    # Guard: every season we see must be in the provided starts
    missing = [s for s in df["season"].unique() if s not in season_starts]
    if missing:
        raise ValueError(f"Missing season starts for seasons: {missing}")

    # View of sample timestamps in requested local tz (for other ops),
    # but convert to UTC for the subtraction to be 100% consistent.
    ts_local = df["timestamp"].dt.tz_convert(tz) if tz else df["timestamp"]
    ts_utc = ts_local.dt.tz_convert("UTC")

    # Map each row to its season start; ensure tz-aware and convert to UTC
    starts_series = df["season"].map(season_starts)                 # Series of Timestamps
    starts_series = pd.to_datetime(starts_series, utc=True)         # force tz-aware (UTC)
    # (If you prefer to align to local day boundaries elsewhere, keep ts_local around.)

    # Subtract Series-from-Series (no .values), then get seconds
    elapsed = (ts_utc - starts_series).dt.total_seconds()

    out = df.copy()
    out["elapsed_seconds"] = elapsed
    # Keep only records on/after season start
    return out[out["elapsed_seconds"] >= 0]


def aggregate_to_bins(df: pd.DataFrame,
                      field: str,
                      freq: str,
                      agg: str,
                      group_by: Optional[str]) -> pd.DataFrame:
    if df.empty:
        return df
    sec_per = SECONDS_PER_DAY if freq.upper() == "D" else SECONDS_PER_HOUR
    df = df.sort_values("timestamp").copy()
    df["bin"] = (df["elapsed_seconds"] // sec_per).astype("int64")

    keys = ["season", "bin"] + ([group_by] if group_by else [])
    if agg == "last":
        idx = df.groupby(keys)["timestamp"].idxmax()
        step = df.loc[idx, keys + [field]].copy()
    else:
        func = {"max": "max", "mean": "mean", "sum": "sum"}[agg]
        step = df.groupby(keys, as_index=False)[field].agg(func)

    if group_by:
        cross_keys = ["season", "bin"]
        if agg == "last":
            step = step.groupby(cross_keys, as_index=False)[field].max()
        else:
            step = step.groupby(cross_keys, as_index=False)[field].agg(func)

    wide = step.pivot(index="bin", columns="season", values=field).sort_index()
    wide.index.name = "elapsed_days" if freq.upper() == "D" else "elapsed_hours"
    return wide.ffill().bfill()


def main():
    ap = argparse.ArgumentParser(description="Season-aligned series from WoWAudit hourly data")
    ap.add_argument("--config", required=True, help="Path to seasons.yaml (see README)")
    ap.add_argument("--env-file", default=".env", help="Path to .env file with MONGODB_URI (default .env)")
    ap.add_argument("--mongo", default=None, help="Override MongoDB URI; otherwise read from .env")
    ap.add_argument("--db", default="wowaudit_database")
    ap.add_argument("--coll", default="wowaudit_hourly")
    ap.add_argument("--field", required=True, help="Numeric field to extract, e.g., 'm+_score'")
    ap.add_argument("--names", nargs="*", default=None, help="Filter: player names (metadata.name)")
    ap.add_argument("--team", default=None, help="Filter: team name (metadata.team_name), e.g., 'Quartz'")
    ap.add_argument("--expansion", default="TWW", help="Expansion key in YAML (default TWW)")
    ap.add_argument("--tz", default="America/Los_Angeles", help="Local tz for binning (used if YAML lacks tz)")
    ap.add_argument("--freq", default="D", choices=["D", "H"], help="Bin size: D=days, H=hours")
    ap.add_argument("--agg", default="last", choices=["last", "max", "mean", "sum"],
                    help="Aggregator within bins (and across names if --group-by)")
    ap.add_argument("--group-by", default=None, choices=[None, "name"],
                    help="Aggregate across this dimension (e.g., 'name') after per-bin aggregation")
    ap.add_argument("--iqr-denoise", type=float, default=0.0,
                    help=">0 drops outliers via IQR*k (e.g., 3.0). Default disabled.")
    ap.add_argument("--out", required=True, help="Output CSV filename")

    args = ap.parse_args()

    mongo_uri = get_mongo_uri(args.mongo, args.env_file)
    season_starts = load_season_starts_from_yaml(args.config, args.expansion, args.tz)

    # If YAML specifies a timezone for the expansion, prefer it for binning
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    yaml_tz = (cfg.get("expansions") or {}).get(args.expansion, {}).get("timezone")
    bin_tz = yaml_tz or args.tz

    query = build_query(args.field, args.names, args.team, args.expansion)
    df = fetch_df(mongo_uri, args.db, args.coll, query, args.field)
    if df.empty:
        raise SystemExit("No rows returned for the given filters.")

    df = coerce_numeric(df, args.field)
    if args.iqr_denoise > 0:
        df = iqr_filter(df, args.field, args.iqr_denoise)

    df = attach_elapsed_seconds(df, season_starts, tz=bin_tz)
    if df.empty:
        raise SystemExit("All rows are before the configured season starts.")

    wide = aggregate_to_bins(df, args.field, args.freq, args.agg, args.group_by)
    if wide.empty:
        raise SystemExit("No data after binning/aggregation.")

    wide.to_csv(args.out, index=True)
    print(f"Wrote {args.out} with seasons {list(wide.columns)}; rows={len(wide):,}; index={wide.index.name}")


if __name__ == "__main__":
    main()
