# Pebble – Raid Time Fairness (Living Spec)

*Last updated: 2025-09-11 PT*

> **Purpose**: Fairly allocate Mythic raid time across team members by measuring **time on the bench** (not just time played), with fully auditable calculations and low‑friction officer controls. This document tracks the current V1 design, decisions, and implementation details. It’s meant to be a living spec you can hand to new contributors or use to spin up a fresh chat/thread.

---

## 1) Project Overview

**Problem**: In Mythic raiding we rotate players. We want fairness over time, but logs get messy: multiple reports, Heroic/Mythic mixing, breaks, and partial attendance.

**Goal**: Produce transparent per‑night and per‑week **bench minutes** for each main (with main↔alt mapping), ranked by **least bench time** so officers can make equitable swap decisions pre‑break and post‑break.

**Approach**:

- Pull WarcraftLogs data for selected reports.
- Derive **Mythic envelope** per night (first Mythic pull → last Mythic pull). Count downtime between pulls as “Mythic time.”
- Detect the **break** from the **All‑Fights timeline** (Normal+Heroic+Mythic, including trash) and **split** the Mythic envelope into **pre** and **post** halves.
- Compute each player’s **played minutes** per half (based on contiguous Mythic participation blocks) and **bench minutes** = (Mythic half minutes − played half minutes) when available.
- Aggregate into per‑night and per‑week tables, rank by least season‑to‑date minutes.
- **Storage model**: Google Sheets remains the UI layer (inputs/outputs). All intermediate/state data is persisted in MongoDB (container). This minimizes WCL and Sheets I/O. The Python service runs locally (container) on a poll loop, syncing results between DB and Sheets.

**Time zone**: America/Los\_Angeles (Proudmoore server time). All timestamps stored/displayed in PT.

---

## 2) Definitions & Rules

- **Night**: Identified by `YYYY-MM-DD` (PT) based on raid activity timeframe.
- **Break**: \~21:00 PT window. Auto‑detected as the **largest gap** between boss fights in All‑Boss timeline that falls within a tunable window (e.g., 20:50–21:30 PT) and within min/max length (e.g., 8–30 min). Manual override supported per report.
- **All‑Fights timeline**: All fights (boss and trash) at difficulties **3/4/5** (Normal/Heroic/Mythic).
- **Mythic envelope**: From **first Mythic pull start** to **last Mythic pull end**. **Includes downtime** between Mythic pulls (trash time counts as played for players present in adjacent pulls).
- **Played block (per main)**: From first Mythic pull they participate in to last **contiguous** Mythic pull they participate in within a half (pre or post). If they step out and come back, that becomes **another block**. Blocks bridge downtime/trash between Mythic pulls. Mid‑trash swaps are inferred at the next/previous boss boundary.
- **Availability inference**:
  - If a main appears on the **last non‑Mythic boss** (e.g., last Heroic) that ends **on/before** the first Mythic start, they are treated as **available for the entire Mythic envelope** (thus accrue bench if they do not play Mythic).
  - If a main appears in **either half** of the Mythic envelope (Pre or Post), they are treated as **available for both halves** of that envelope. Their played minutes are credited only in the half they actually participated, but their **bench minutes** accrue in the half they did not.
  - Rostered mains who **never appear** in the log are presumed on **vacation** and receive **no bench credit**. They are omitted from Bench Night Totals unless an Availability Override explicitly marks them available.
  - Officer overrides can always force availability (or non‑availability) per half.
- **Bench Minutes** (per main, per night):
  - `Bench Pre = max(0, MythicPreMinutes − PlayedPreMinutes)` **if available pre** else `0`.
  - `Bench Post = max(0, MythicPostMinutes − PlayedPostMinutes)` **if available post** else `0`.
  - `Bench Total = Bench Pre + Bench Post`.
- **Roster membership windows** (Team Roster): A main’s `Join Date` and `Leave Date` bound which weeks/nights they are considered. `Active?` allows retaining history but excluding after leave.
- **Main↔Alt**: Officers maintain a mapping; all alt participation credits the mapped **Main**.

**Difficulty codes**: Normal=3, Heroic=4, Mythic=5.

---

## 3) Data Flow (V1)

1. **Input (Reports sheet)** (officers)
   - Paste report links, set status: *blank*=new, `in‑progress`, `done`.
   - Optional: specify break override start/end times in PT.
2. **Ingest**
   - Fetch fights for each report; dedupe with canonical key `(encounter_id, difficulty, rounded start/end)`.
   - Convert to PT. Persist raw fights in `fights_all` (Mongo).
3. **Participation (Mythic‑only)**
   - Emit one row **per player per Mythic boss fight**. Trash not separately recorded; trash time is bridged between adjacent pulls.
4. **Blocks**
   - Build contiguous Mythic participation blocks (per night, per main, split pre/post by break). Blocks include trash time between pulls.
5. **Night QA**
   - From **All‑Fights timeline**: Night Start/End, Break detection + candidates, compact fight timeline.
   - From **Mythic fights only**: Mythic Start/End, **Mythic Pre/Post minutes** via envelope split around break.
   - **Manual override precedence**: If `Break Override Start (PT)` / `Break Override End (PT)` are present on Reports, they fully replace auto‑detection for that report/night and are recorded in `night_qa` with `override_used = true`.
6. **Bench Night Totals**
   - For each night, include: everyone who **played Mythic**, was on the **last non‑Mythic boss before Mythic**, or has an explicit **Availability Override**.
   - Apply availability overrides; compute Bench Pre/Post/Total.
7. **Bench Week Totals**
   - For each observed week (nights present in QA), include **every roster main** within membership window—even if they didn’t play that week. Sum Bench and Played per week.
8. **Rankings**
   - Officers sort by **season‑to‑date Bench Minutes** (least first) to guide swaps.

**Idempotency**: All writers use stable keys and value normalization to avoid churn. Deleting a status can safely trigger reprocessing without duplicated rows.

---

## 3b) Idempotent Processing Contract (cross‑referenced)

Each stage is crash‑safe and idempotent. Every write uses a stable natural key with **upsert** semantics; outputs are reproducible from their source of truth. Stages map 1‑to‑1 with **Data Flow (V1)** steps.

- **Stage 1: Reports** — Data Flow #1. Inputs: officer-managed entries in Sheets; Keys: report_code (from URL), status. Idempotent by definition; manual changes are authoritative; service never overwrites.
- **Stage 2: Ingest (All‑Fights)** — Data Flow #2. Inputs: WCL API; Keys: `{report_code, encounter_id, difficulty, start_rounded_ms, end_rounded_ms}`; Output: `fights_all`, `reports`.
- **Stage 3: Participation (Mythic‑only)** — Data Flow #3. Inputs: `fights_mythic`; Keys: `{night_id, report_code, encounter_id, main, start_ms}`; Output: `participation_m`.
- **Stage 4: Blocks** — Data Flow #4. Inputs: `participation_m`, `night_qa.break_start/end`; Keys: `{night_id, main, half, block_seq}`; Output: `blocks`.
- **Stage 5: Night QA** — Data Flow #5. Inputs: `fights_all`, config knobs, overrides; Keys: `{night_id}`; Output: `night_qa`.
- **Stage 6: Bench Night Totals** — Data Flow #6. Inputs: `night_qa`, `blocks`, `roster_map`, `team_roster`, `availability_overrides`; Keys: `{night_id, main}`; Output: `bench_night_totals`.
- **Stage 7: Bench Week Totals** — Data Flow #7. Inputs: `bench_night_totals`, `team_roster`; Keys: `{game_week, main}`; Output: `bench_week_totals`.
- **Stage 8: Rankings** — Data Flow #8. Inputs: `bench_week_totals`; Output: sorted views.
- **Export to Sheets (UI)** — Inputs: materialized Mongo tables; Keys: natural per sheet; Output: Sheets ranges (read‑only).

Cross‑cutting: Times stored PT ISO + UTC ms; deterministic sort; only Export touches Sheets.

---

## 4) Google Sheets (UI‑only)

> **Principle: one source of truth per datum.** Officer‑managed inputs live in Sheets; all intermediate/working data lives in MongoDB; derived tables are materialized to Sheets **read‑only** and can be regenerated at any time. No output range should be hand‑edited. **All inputs and outputs live in a single Google Sheets document, split across multiple worksheets (Reports, Roster Map, Team Roster, Availability Overrides, Night QA, Bench Night Totals, Bench Week Totals).**

### Inputs (authoritative in Sheets)

- **Reports**
  - `Report URL`, `Status` (*blank* | `in-progress` | `done`), `Last Checked (PT)`, `Notes`, `Break Override Start (PT)`, `Break Override End (PT)`.
- **Roster Map**
  - `Alt`, `Main`.
- **Team Roster**
  - `Main`, `Join Date`, `Leave Date`, `Active?`, `Notes`.
- **Availability Overrides**
  - `Night`, `Main`, `Avail Pre?`, `Avail Post?`, `Reason`.

### Outputs (DB→Sheets; read‑only)

- **Night QA (Compact)** (manual override values fully replace auto-detected break times when both start and end are present; if only one is filled, service logs a warning and ignores override; logged in QA row)
- **Bench Night Totals**
- **Bench Week Totals**

### Removed from Sheets (DB‑only)

- `Fights`, `Participation (Mythic‑only)`, `Blocks`, and `Night Totals`.

### Idempotency & Crash‑Safety

* Export compares each row’s values to the current sheet using stable natural keys; **any difference triggers an update** (no `_rev` hash).
* **Scoped reconciliation = deletes included**: every export operates within a **scope key** and treats the DB as source of truth:
  * **Night QA** scope = `{night_id}` (single-night pages) or full table if not partitioned.
  * **Bench Night Totals** scope = `{night_id}`.
  * **Bench Week Totals** scope = `{game_week}`.
  * For each scope, the exporter **upserts changed rows and deletes rows whose keys are no longer present** in the recomputed set. This prevents stale rows lingering after upstream changes.
* **Sheets deletions:** exporter performs a **replace-in-scope** write: it clears the scoped region then writes the recomputed rows in one batch to avoid orphaned rows.
* **Mongo deletions:** per scope, after bulk upserts, a **set-difference delete** removes documents not in the recomputed key set (transactional when available; otherwise best-effort with idempotent retries).
* **Batched upserts** per sheet/collection minimize API calls.
* After every export, apply **canonical sort & formatting** to keep the UI consistent:
  * **Night QA**: sort by `Night Start (PT)` ascending.
  * **Bench Night Totals**: sort by `Night ID` asc, then `Bench Minutes (Total)` asc, then `Main` asc.
  * **Bench Week Totals**: sort by `Game Week` asc, then `Bench Minutes (Week)` asc, then `Main` asc.
* Stages can re‑run in any order; outputs are fully reproducible from DB.

---

## 5) Config

- `timezone`: `America/Los_Angeles`.
- **Break detection**: `break_window_start`, `break_window_end`, `min_break_min`, `max_break_min`.
- **Dedup tolerance**: `dedupe_tol_s`.
- **Sheets**: Named ranges/worksheet names.
- **Polling**: `poll_interval_sec`; optional faster cadence during break.
- **Feature flags**: e.g., enable manual break overrides.

### Configuration format
- **Primary file:** `config.yaml` (checked into repo). Defines non-secret runtime settings.
- **Environment file:** `.env` (not committed, DO NOT commit). Holds secrets and environment-specific overrides.
- **Precedence:** process env / `.env` values **override** `config.yaml` defaults.

**Example keys** (abridged):
```yaml
app:
  timezone: "America/Los_Angeles"
  sheet_id: "..."

  sheets:
    reports: "Reports"
    roster_map: "Roster Map"
    player_facts: "Player Facts"
    participation: "Participation"
    night_qa: "Night QA"
    team_roster: "Team Roster"
    availability_overrides: "Availability Overrides"
    bench_night_totals: "Bench Night Totals"
    bench_week_totals: "Bench Week Totals"
  
  raid_window:
    start_local: "19:00"
    end_local: "23:30"

  break_window:
    start_local: "20:50"
    end_local: "21:30"
    min_gap_minutes: 8
    max_gap_minutes: 30

  week_reset:
    day_of_week: "Tuesday"
    time_local: "07:00"

  dedupe:
    overlap_merge_tolerance_sec: 3

  inactivity_timeout_minutes: 35

  pacing:
    poll_seconds: 300
    max_reports_per_cycle: 5
    sleep_between_requests_ms: 1000

wcl:
  client_id: "${WCL_CLIENT_ID}"
  client_secret: "${WCL_CLIENT_SECRET}"
  api_url: "https://www.warcraftlogs.com/api/v2/client"
  oauth_url: "https://www.warcraftlogs.com/oauth/token"

google:
  service_account_json_path: "./service-account.json"
  
mongo:
  uri: "mongodb://localhost:27017"
  db: "pebble"
  write_batch_size: 500
  ```

**Required env vars** (via `.env` or deployment secrets):
- `WCL_CLIENT_ID` - WarcraftLogs Client ID
- `WCL_CLIENT_SECRET` - WarcraftLogs Client Secret
- `MONGO_URI` – overrides `mongo.uri` when set
- `GOOGLE_APPLICATION_CREDENTIALS` – path to service account JSON
- `SHEET_ID` – overrides `app.sheet_id` when set
- `LOG_LEVEL` – e.g., `INFO`, `DEBUG`

---

## 6) Implementation Notes&#x20;



---

## 7) Local Dev/Test

- **Run locally**: clone repo, create `.env` with required secrets, run `docker-compose up` or `python main.py`.
- **Test inputs**: paste a WCL report into the Reports sheet; verify Night QA and Bench outputs.
- **Fixtures**: sample reports and expected outputs stored under `tests/fixtures`.

## 8) Edge Cases & Policies

- **Overlapping logs** deduped by key.
- **Missing reports** → Mythic durations=0 + warning.
- **Heroic after break** → Mythic Pre=0; Post=envelope length.
- **Mid‑night join/leave**: availability inferred or overridden.
- **Mid‑season joins** gated by `Join Date`.

---

## 9) Officer UX

Minimal inputs: paste report, maintain rosters, overrides.\
Observability: logs, QA timelines.\
Idempotency: reprocessing preserves stable rows.

---

## 10) Validation Checklist

- Night QA: break in window; Mythic envelope plausible; Pre+Post=sum.
- Full‑time participant → Bench≈0.
- Last non‑Mythic only → Bench≈Mythic half duration.
- Week totals show all roster mains.
- Presence in only one Mythic half (Pre or Post) → Bench accrues in the other half (cross-half availability inference).

---

## 11) Dependencies (Python)

Runtime: Python 3.12

- **Core**: `requests`, `tenacity`, `pydantic`, `python-dotenv`, `zoneinfo` (stdlib), `python-dateutil`
- **Data**: `pandas`, `numpy`
- **Google Sheets**: `google-api-python-client`, `google-auth`, `google-auth-httplib2`, `google-auth-oauthlib`
- **MongoDB**: `pymongo`
- **Testing**: `pytest`, `freezegun`
- **Typing/quality**: `mypy`, `ruff`, `black`

## 12) Deployment & Ops

- **Runtime**: Python 3.12 container.
- **Storage**: MongoDB intermediate/working store; Sheets UI.
- **Secrets & config loading**:
  - Load `.env` (if present) then process env; merge over `config.yaml` defaults.
  - Validate required vars at startup; fail fast with clear errors.
- **Persistent error handling**: network failures use capped exponential backoff; if WCL API is down >1h, errors are logged until cleared.
- **Schedule**: poll ~5 min; faster near break.
- **Sheets bootstrap script**: `pebble bootstrap sheets` initializes Google Sheets resources idempotently.
- **Logging**: INFO network, WARN missing Mythic, ERR to service_log.
- **Backfill**: add past reports as `done`.
- **Config**: spreadsheet id, timezone, sheet names, DB URI, knobs.
- **Deployment note**: keep `.env` out of version control; mount via container secrets.

---

## 13) File & Module Sketch

**Top-level packages**
- `pebble/` (python package)
  - `config_loader.py` — load `config.yaml`, overlay `.env`/env; return typed `Settings` (pydantic).
  - `logging_setup.py` — structured logging config.
  - `wcl_client.py` — WarcraftLogs GraphQL v2 client (requests/tenacity); conditional GET emulation via ETag/Last-Modified cache.
  - `ingest.py` — fetch fights per report; write to Mongo (`fights_all`); maintain `reports` metadata.
  - `participation.py` — build per-main participation from Mythic fights (bridging trash time).
  - `breaks.py` — detect break from All-Fights; apply manual overrides.
  - `envelope.py` — compute Mythic envelope + pre/post split.
  - `blocks.py` — contiguous block construction per half.
  - `bench_calc.py` — bench minutes per night; availability inference; overrides.
  - `week_agg.py` — week aggregation and ranking materialization.
  - `export_sheets.py` — DB→Sheets export with scoped reconciliation (upsert + delete), canonical sort & formatting.
  - `mongo_client.py` — connection factory.
  - `bootstrap/`
    - `sheets_bootstrap.py` — create spreadsheet/worksheets, named ranges, header rows, formatting.
  - `utils/`
    - `time.py` — PT/UTC helpers (zoneinfo); week bucketing; ISO formatting.
    - `diff.py` — row diffing; key generation; reconciliation utilities.

**CLI entry points (Click)**
- `cli.py` — root CLI.
  - `pebble bootstrap sheets` — create sheets/ranges.
  - `pebble ingest` — pull WCL fights for new/changed reports.
  - `pebble compute` — run blocks → QA → bench night/week.
  - `pebble export` — export Night QA / Bench outputs to Sheets.
  - `pebble backfill --from YYYY-MM-DD --to YYYY-MM-DD` — historical runs.
  - `pebble verify` — run validations (invariants/consistency).

**Scripts / entry wrappers**
- `bin/pebble` — console script entry (setup.cfg/pyproject).
- `docker/` — Dockerfile and compose for local dev.

**Tests**
- `tests/` — pytest suite with fixtures under `tests/fixtures` (sample reports, expected outputs).

---

## 14) Quick Start for Contributors

1. Clone repo.
2. Copy `config.yaml.example` to `config.yaml`; create `.env` with secrets.
3. Run service locally; paste a report link into Sheets.
4. Verify Night QA, Bench Night/Week Totals appear.

## 15) Data Privacy

Sheets data includes character names only; no sensitive personal data. Officers should review sharing settings appropriately.

## 16) Future-proofing

The WCL client is pluggable; if WarcraftLogs releases v3 API, adapter can be swapped without schema changes.

## 17) Additional Considerations

- **Sheet schema contracts**: Provide a table with exact column order, names, and expected types/format for each worksheet (Reports, Roster Map, Team Roster, Availability Overrides, Night QA, Bench Night Totals, Bench Week Totals). This avoids drift and simplifies exporters.
- **Row key conventions**: Document natural keys per collection/table in one place for quick reference.
- **Validation invariants**: Define rules verified by `pebble verify`, e.g. Mythic Pre + Post = Envelope, no negative minutes, every Bench Week Totals main appears in Team Roster.
- **Rate-limit budget**: Specify target API call budgets for WCL and Sheets to ensure compliance with rate limits.
- **Dry-run mode**: Add `--dry-run` flag for `compute`/`export` to log diffs without writing.
- **Observability**: Each phase (ingest, participation, blocks, QA, bench, export) produces **detailed local logs** including counts, timings, warnings, and deletions. Exports must also log **rows written and rows deleted per scope** for reconciliation.
- **Time correctness**: Explicitly store both UTC ms and PT ISO; include regression tests for DST transitions and week/day boundaries.

## 17) Config ↔ Appendix Mapping

| Config key              | Worksheet name        | Schema (Appendix A)         |
|-------------------------|-----------------------|-----------------------------|
| `app.sheets.reports`    | Reports               | Reports                     |
| `app.sheets.roster_map` | Roster Map            | Roster Map                  |
| `app.sheets.team_roster`| Team Roster           | Team Roster                 |
| `app.sheets.overrides`  | Availability Overrides| Availability Overrides      |
| `app.sheets.night_qa`   | Night QA              | Night QA                    |
| `app.sheets.bench_night`| Bench Night Totals    | Bench Night Totals          |
| `app.sheets.bench_week` | Bench Week Totals     | Bench Week Totals           |

---

### Appendix A — Key Columns

**Reports**: `Report URL`, `Status`, `Last Checked (PT)`, `Notes`, `Break Override Start (PT)`, `Break Override End (PT)`, `Report Name`, `Report Start (PT)`, `Report End (PT)`, `Created By`.
**Roster Map**: `Alt`, `Main)`.
**Team Roster**: `Main`, `Join Date`, `Leave Date`, `Active?`, `Notes`.  
**Availability Overrides**: `Night`, `Main`, `Avail Pre?`, `Avail Post?`, `Reason`.

**Night QA**: `Night ID`, `Reports Involved`, `Mains Seen` (unique mains in any boss fight), `Night Start/End (PT)`, `Break Start/End (PT)`, `Break Duration (min)`, `Mythic Start/End (PT)`, `Mythic Pre/Post Duration (min)`, `Gap Window`, `Min/Max Break`, `Dedupe Tol`, `Largest Gap (min)`, `Candidate Gaps (JSON)`, `Override Used?`.
**Bench Night Totals**: `Night ID`, `Main`, `Bench Minutes Pre/Post/Total`, `Played Pre/Post/Total`, `Avail Pre?/Post?`, `Status Source`.
**Bench Week Totals**: `Game Week`, `Main`, `Bench Minutes (Week)`, `Played Minutes (Week)`, `Bench Pre/Post`.