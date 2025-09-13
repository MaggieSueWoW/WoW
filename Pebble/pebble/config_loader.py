from __future__ import annotations
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from pathlib import Path
import os, yaml


class SheetsTabs(BaseModel):
    reports: str = Field(default="Reports")
    roster_map: str = Field(default="Roster Map")
    team_roster: str = Field(default="Team Roster")
    availability_overrides: str = Field(default="Availability Overrides")
    night_qa: str = Field(default="Night QA")
    bench_night_totals: str = Field(default="Bench Night Totals")
    bench_week_totals: str = Field(default="Bench Week Totals")
    bench_rankings: str = Field(default="Bench Rankings")


class SheetsStarts(BaseModel):
    reports: str = Field(default="A5")
    roster_map: str = Field(default="A5")
    team_roster: str = Field(default="A5")
    availability_overrides: str = Field(default="A5")
    night_qa: str = Field(default="A5")
    bench_night_totals: str = Field(default="A5")
    bench_week_totals: str = Field(default="A5")
    bench_rankings: str = Field(default="A5")


class SheetsLastProcessed(BaseModel):
    reports: str = Field(default="B3")
    roster_map: str = Field(default="B3")
    team_roster: str = Field(default="B3")
    availability_overrides: str = Field(default="B3")
    night_qa: str = Field(default="B3")
    bench_night_totals: str = Field(default="B3")
    bench_week_totals: str = Field(default="B3")
    bench_rankings: str = Field(default="B3")


class SheetsConfig(BaseModel):
    spreadsheet_id: str
    tabs: SheetsTabs = Field(default_factory=SheetsTabs)
    starts: SheetsStarts = Field(default_factory=SheetsStarts)
    last_processed: SheetsLastProcessed = Field(default_factory=SheetsLastProcessed)


class MongoConfig(BaseModel):
    uri: str
    db: str = Field(default="pebble")


class WCLConfig(BaseModel):
    client_id: str
    client_secret: str
    base_url: str = Field(default="https://www.warcraftlogs.com/api/v2/client")
    token_url: str = Field(default="https://www.warcraftlogs.com/oauth/token")


class RedisConfig(BaseModel):
    url: str = Field(default="redis://localhost:6379/0")
    key_prefix: str = Field(default="pebble:wcl:")


class BreakWindowConfig(BaseModel):
    start_pt: str = Field(default="20:50")
    end_pt: str = Field(default="21:30")
    min_gap_minutes: int = 10
    max_gap_minutes: int = 30


class TimeConfig(BaseModel):
    tz: str = Field(default="America/Los_Angeles")
    break_window: BreakWindowConfig = Field(default_factory=BreakWindowConfig)


class Settings(BaseModel):
    sheets: SheetsConfig
    mongo: MongoConfig
    wcl: WCLConfig
    redis: RedisConfig = Field(default_factory=RedisConfig)
    time: TimeConfig = Field(default_factory=TimeConfig)
    service_account_json: str = Field(default="service-account.json")


def _load_yaml(path: str | os.PathLike) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings(config_path: str = "config.yaml") -> Settings:
    load_dotenv(override=False)

    data = _load_yaml(config_path)

    # Allow env overrides for secrets
    env_overrides = {
        "sheets": {
            "spreadsheet_id": os.getenv(
                "SHEETS_SPREADSHEET_ID", data.get("sheets", {}).get("spreadsheet_id")
            ),
        },
        "mongo": {
            "uri": os.getenv("MONGODB_URI", data.get("mongo", {}).get("uri")),
            "db": data.get("mongo", {}).get("db", "pebble"),
        },
        "wcl": {
            "client_id": os.getenv(
                "WCL_CLIENT_ID", data.get("wcl", {}).get("client_id")
            ),
            "client_secret": os.getenv(
                "WCL_CLIENT_SECRET", data.get("wcl", {}).get("client_secret")
            ),
        },
        "redis": {
            "url": os.getenv(
                "REDIS_URL", data.get("redis", {}).get("url", "redis://localhost:6379/0")
            ),
            "key_prefix": os.getenv(
                "REDIS_KEY_PREFIX", data.get("redis", {}).get("key_prefix", "pebble:wcl:")
            ),
        },
        "service_account_json": os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS",
            data.get("service_account_json", "service-account.json"),
        ),
    }

    # Merge shallowly
    merged = {
        **data,
        "sheets": {**data.get("sheets", {}), **env_overrides["sheets"]},
        "mongo": {**data.get("mongo", {}), **env_overrides["mongo"]},
        "wcl": {**data.get("wcl", {}), **env_overrides["wcl"]},
        "redis": {**data.get("redis", {}), **env_overrides["redis"]},
        "service_account_json": env_overrides["service_account_json"],
    }

    return Settings(**merged)
