
# EV-Backend Import Scripts

These scripts download, import, and validate data into the PostGIS database that powers
Empowered Vote — geofence boundaries for address-to-politician lookups and state legislative
data (committees, bills, votes) for Indiana and California.

---

## Prerequisites (shared)

### Python version

**Python 3.10 or newer is required.**

The system `python3` on macOS is Python 3.9.6 and will NOT work — geopandas 1.1.2 dropped
Python 3.9 support. Use Python 3.13 or 3.12 from Homebrew:

```bash
# Install Python 3.13 via Homebrew (if not already installed)
brew install python@3.13

# Verify version
/opt/homebrew/bin/python3.13 --version
# → Python 3.13.x
```

Python 3.10+ from any source works for the state legislative scripts (they do not use geopandas).

### Two requirements files

| File | Used by | Key packages |
|------|---------|-------------|
| `requirements.txt` | Geofence boundary imports | geopandas, SQLAlchemy, psycopg2-binary, shapely |
| `requirements-state.txt` | State legislative imports | psycopg2-binary, requests, python-dotenv |

Install both if you plan to run both workflows:

```bash
cd EV-Backend/scripts

# Geofence imports — requires Python 3.13 (geopandas 1.1.2 drops 3.9)
python3.13 -m pip install -r requirements.txt

# State legislative imports — any Python 3.10+
python3 -m pip install -r requirements-state.txt
```

### Database connection

**Important:** Use the Supabase **direct connection** (port 5432), NOT the pooler (port 6543).
The pooler breaks bulk imports with `prepared statement already exists` errors.

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

Or create `EV-Backend/.env.local` (never commit this file) with:

```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

All scripts read `DATABASE_URL` from the environment first, then fall back to `.env.local`
automatically.

---

## Section 1: Geofence Boundary Imports

These scripts download and import US Census TIGER/Line shapefiles into the PostGIS database
to enable local address → politician lookups without hitting external APIs.

### Install dependencies

```bash
cd EV-Backend/scripts
python3.13 -m pip install -r requirements.txt
```

This installs exact pinned versions of all import dependencies:
- `geopandas==1.1.2`
- `SQLAlchemy==2.0.46`
- `psycopg2-binary==2.9.11`
- `shapely==2.0.7`
- `requests==2.32.5`

### Bash script alternative

```bash
# Install GDAL (includes ogr2ogr)
brew install gdal  # macOS
# or
apt-get install gdal-bin  # Ubuntu/Debian
```

### Run import

**Python (recommended):**

```bash
cd EV-Backend/scripts
python3.13 import_missing_geofences.py
python3.13 import_school_board_districts.py
python3.13 import_ca_legislative_geofences.py
```

**Bash:**

```bash
cd EV-Backend/scripts
./import_shapefiles.sh
```

### Verify import

```bash
psql "$DATABASE_URL" -c "
  SELECT
    mtfcc,
    COUNT(*) as count,
    MIN(name) as example_name
  FROM essentials.geofence_boundaries
  GROUP BY mtfcc
  ORDER BY count DESC;
"
```

### Shared utilities

New import scripts should use the shared `utils.py` module rather than duplicating common functions:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_engine, load_env, next_ext_id
```

**Available functions:**
- `load_env()` — Reads `DATABASE_URL` from environment or falls back to `.env.local`
- `get_engine()` — Creates SQLAlchemy engine with proper password URL-encoding
- `next_ext_id()` — Returns next synthetic external ID in the v1.6 range (starts at -200001)

Do NOT copy these function bodies into new scripts.

### What gets imported

**Monroe County, Indiana (FIPS: 18105)**
- County boundaries
- Townships (county subdivisions)
- School districts
- Congressional District 9
- State legislative districts (Senate & House)
- Voting districts (precincts)

**Los Angeles County, California (FIPS: 06037)**
- County boundaries
- Cities and places
- School districts
- Congressional districts (multiple)
- State legislative districts (Senate & Assembly)

### Expected feature counts

| Feature Type | MTFCC | Monroe, IN | LA County, CA |
|--------------|-------|------------|---------------|
| County | G4020 | 1 | 1 |
| Township | G4040 | ~10 | - |
| City/Place | G4110 | ~5 | ~80 |
| School District | G5420 | ~3 | ~80 |
| Congressional District | G5200 | 1 | ~15 |
| State Senate | G5210 | ~2 | ~10 |
| State House/Assembly | G5220 | ~3 | ~20 |

### Troubleshooting geofence imports

**"No module named 'geopandas'"**

```bash
python3.13 -m pip install -r requirements.txt
```

**"Python version mismatch" or import errors with Python 3.9**

The macOS system `python3` is 3.9.6 — geopandas 1.1.2 requires Python 3.10+.
Always use `python3.13` (or `python3.12`) explicitly:

```bash
/opt/homebrew/bin/python3.13 import_missing_geofences.py
```

**"ogr2ogr: command not found"**

```bash
brew install gdal  # macOS
```

**"Unable to connect to database"**

Check that `DATABASE_URL` is set and PostGIS extension is enabled:

```sql
SELECT PostGIS_version();
```

**"prepared statement already exists"**

You are connecting via the Supabase pooler (port 6543). Switch to the direct connection (port 5432):

```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

**Slow imports**

The Python script is optimized for reliability over speed. First import may take 10–15 minutes.

### Data source

All shapefiles are from the US Census Bureau TIGER/Line program:
https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

Data is public domain and updated annually.

### Next steps after geofence import

1. **Verify geofence coverage:**

```sql
SELECT geo_id, name, mtfcc
FROM essentials.geofence_boundaries
WHERE state = 'IN'
LIMIT 10;
```

2. **Test point-in-polygon lookup:**

```sql
SELECT geo_id, name
FROM essentials.geofence_boundaries
WHERE ST_Contains(
    geometry,
    ST_SetSRID(ST_MakePoint(-86.5264, 39.1653), 4326)  -- Bloomington, IN
);
```

3. **Run the bulk ZIP import** to populate politician data for these areas.

---

## Section 2: State Legislative Imports

These scripts import committee memberships and legislative data (bills, votes) for Indiana
and California state legislators, then verify data quality via the Go API.

### Overview

| What | How | States |
|------|-----|--------|
| Committee memberships | IGA API (IN) or Open States API (CA) | IN, CA |
| Bills, votes, cosponsors | LegiScan Dataset API | IN, CA |
| Validation | 7-check audit + 80% committee coverage check | IN, CA |
| API verification | Hits the live Go API endpoints | IN, CA |

**Session config:** `state_legislative_config.json` is the single source of truth for session
years. Both import scripts read from this file and exit with a clear error if it is missing.

Current config (`EV-Backend/scripts/state_legislative_config.json`):

```json
{
  "states": {
    "IN": {
      "current_year_start": 2026,
      "previous_year_start": 2025,
      "committee_source": "iga",
      "legislative_source": "legiscan"
    },
    "CA": {
      "current_year_start": 2025,
      "previous_year_start": 2023,
      "committee_source": "openstates",
      "legislative_source": "legiscan"
    }
  }
}
```

### Install dependencies

```bash
cd EV-Backend/scripts
python3 -m pip install -r requirements-state.txt
```

This installs:
- `psycopg2-binary>=2.9` — PostgreSQL adapter
- `requests>=2.31` — HTTP client for API calls
- `python-dotenv>=1.0` — Loads `.env.local`

Python 3.10+ is sufficient. Python 3.13 from Homebrew works fine.

### Additional environment variables

Beyond `DATABASE_URL` (set up above), the state legislative scripts need:

**`LEGISCAN_API_KEY`** — Required for `import_state_legislative.py`.

Get a free key at https://legiscan.com/legiscan (account → API key).
Add to `EV-Backend/.env.local`:

```
LEGISCAN_API_KEY=your_key_here
```

**`OPENSTATES_API_KEY`** — Required only for `import_state_committees.py --state CA`.
Indiana uses the IGA API which requires no key.

Get a free key at https://openstates.org/accounts/signup/ (free tier: 10 requests/minute).
Add to `EV-Backend/.env.local`:

```
OPENSTATES_API_KEY=your_key_here
```

Your `.env.local` should look like this when fully configured:

```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
LEGISCAN_API_KEY=abc123...
OPENSTATES_API_KEY=xyz789...
```

---

### Scripts reference

#### 1. `import_state_legislative.py`

Downloads LegiScan dataset ZIPs and imports bills, votes, and cosponsors for IN and CA.

Uses LegiScan's Dataset API (download entire sessions as ZIP archives) rather than
individual getBill/getRollCall calls. This reduces API usage from ~2,000–15,000 queries
to about 5 per state per run.

**Usage:**

```bash
cd EV-Backend/scripts

# Dry run first — verify config without writing to DB
python3 import_state_legislative.py --state IN --sessions current --dry-run --verbose

# Full import for Indiana (current session only)
python3 import_state_legislative.py --state IN --sessions current

# Full import for California (current session only)
python3 import_state_legislative.py --state CA --sessions current

# Import both sessions (current + previous)
python3 import_state_legislative.py --state IN --sessions current,previous
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--state IN\|CA` | State to import (required) |
| `--sessions current,previous` | Which sessions to import (comma-separated) |
| `--dry-run` | Log operations without writing to DB |
| `--verbose` | Enable DEBUG logging |

**Expected output:**

```
2026-03-05 10:00:00 INFO     Loaded config: IN current_year_start=2026
2026-03-05 10:00:01 INFO     Fetching dataset list for IN...
2026-03-05 10:00:02 INFO     Dataset hash unchanged — skipping download
2026-03-05 10:00:03 INFO     Importing bills from extracted ZIP...
2026-03-05 10:00:15 INFO     ============================================================
2026-03-05 10:00:15 INFO     IMPORT COMPLETE: IN
2026-03-05 10:00:15 INFO       Bills imported:      935
2026-03-05 10:00:15 INFO       Votes imported:      6,069
2026-03-05 10:00:15 INFO       Cosponsors imported: 412
```

Exact counts are session-specific and will vary.

**Budget impact:** ~5 API queries per state per run (getDatasetList + getDataset × sessions).
The free tier allows 30,000 queries/month. Check current usage in
`~/.ev-backend/legiscan_counter.json`.

---

#### 2. `import_state_committees.py`

Imports committee memberships from IGA (Indiana) or Open States (California).

LegiScan's getSessionPeople returns `committee_id=0` for all state legislators,
so this script is the authoritative source for committee membership data.

**Usage:**

```bash
cd EV-Backend/scripts

# Indiana — dry run first (IGA API, no auth required)
python3 import_state_committees.py --state IN --dry-run --verbose

# Indiana — full import (~1 minute)
python3 import_state_committees.py --state IN

# California — dry run first (Open States API, OPENSTATES_API_KEY required)
python3 import_state_committees.py --state CA --dry-run --verbose

# California — full import (~40 minutes due to rate limiting)
python3 import_state_committees.py --state CA
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--state IN\|CA` | State to import (required) |
| `--dry-run` | Log operations without writing to DB |
| `--verbose` | Enable DEBUG logging |

**Expected output:**

```
2026-03-05 10:00:00 INFO     Starting committee import for IN (indiana)
2026-03-05 10:00:00 INFO     No previous import recorded for IN
2026-03-05 10:00:01 INFO     Fetching committees from IGA API (session=session_2026)...
2026-03-05 10:00:02 INFO     Fetched 46 standing committees with memberships from IGA
2026-03-05 10:00:15 INFO     ============================================================
2026-03-05 10:00:15 INFO     COMMITTEE IMPORT COMPLETE: IN
2026-03-05 10:00:15 INFO       Committees fetched:            46
2026-03-05 10:00:15 INFO       Committees matched (existing): 0
2026-03-05 10:00:15 INFO       Committees created (new):      46
2026-03-05 10:00:15 INFO       Memberships linked:            61
2026-03-05 10:00:15 INFO       Memberships skipped (no match):37
```

Indiana runtime is approximately 1 minute. California runtime is approximately 40 minutes
because the Open States API enforces a 10 requests/minute free-tier rate limit, and the
script enforces a 6-second delay between pages.

Exact counts are session-specific and will vary.

---

#### 3. `validate_state_legislative.py`

Runs a 7-check audit of bills, votes, and bridge records for IN and CA.

**Checks performed:**

| Check | What it verifies |
|-------|-----------------|
| A | DB bill/vote counts and active legislators |
| B | LegiScan session total comparison (optional, costs 1 API call per state) |
| C | Bridge record coverage (80% threshold) |
| D | Zero-activity legislator analysis (10% cap) |
| E | Orphaned record detection (bills with no sponsor, broken FK references) |
| F | Spot-check leadership legislators for bridge + activity |
| G | Overall PASS/FAIL determination |

**Usage:**

```bash
cd EV-Backend/scripts

# Standard run (both states)
python3 validate_state_legislative.py

# Verbose output (lists all missing bridges and zero-activity legislators)
python3 validate_state_legislative.py --verbose

# Single state
python3 validate_state_legislative.py --state IN
python3 validate_state_legislative.py --state CA

# Include LegiScan API comparison (costs 1 API call per state)
python3 validate_state_legislative.py --legiscan-check

# Dry run — print what would be checked, no DB queries
python3 validate_state_legislative.py --dry-run
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--state IN\|CA\|both` | State(s) to validate (default: both) |
| `--verbose` | Show full lists of missing bridges, zero-activity legislators |
| `--legiscan-check` | Compare DB against LegiScan totals (2 API calls) |
| `--dry-run` | Print what would be checked without running DB queries |

**Expected output (passing):**

```
State Legislative Data Validation
Threshold: bridge coverage >= 80% | established zero-activity <= 10%

============================================================
Validating IN (indiana) — 2026 Regular Session
============================================================

Sessions found: 1 current session(s)
  - 2026 Regular Session

[A] DB Bill/Vote Counts
  Bills in DB:                     935
  Vote records in DB:              6,069
  Legislators with sponsorship:    15
  Legislators with votes:          17

[C] Bridge Record Completeness
  Legislators in DB:      18
  With bridge records:    17 (94.4%)
  Missing bridge:         1  [PASS]

[D] Zero-Activity Legislator Analysis
  Total legislators:                 18
  Active (any bill or vote):         17
  Established with zero activity:    1 (5.6%) [PASS]

...

IN: PASS

============================================================
SUMMARY
============================================================
  IN: PASS
  CA: PASS

Result: All states pass all validation checks.
```

Exit code 0 on full pass, 1 on any failure.

---

#### 4. `validate_committee_coverage.py`

Verifies that 80%+ of state legislators in the DB have at least one committee assignment.

**Usage:**

```bash
cd EV-Backend/scripts

# Standard run (both states)
python3 validate_committee_coverage.py

# Verbose — show committee samples for each spot-check legislator
python3 validate_committee_coverage.py --verbose

# Dry run — print what would be checked
python3 validate_committee_coverage.py --dry-run
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--verbose` | Show committee samples for spot-check legislators |
| `--dry-run` | Print what would be checked without running DB queries |

**Expected output (passing):**

```
Committee Coverage Validation
Threshold: 80% of DB legislators must have committee assignments

============================================================
Validating IN (indiana) — 2026 session
============================================================

DB counts for indiana:
  Committees in DB:               46
  Membership rows:                61
  Politicians with membership:    16

Coverage: 16/18 IN legislators in DB have committee assignments (88.9%)
IN: PASS (>= 80% threshold)

============================================================
Validating CA (california) — 2025-2026 session
============================================================
...
CA: PASS (>= 80% threshold)

============================================================
SUMMARY
============================================================
  IN: PASS
  CA: PASS

Result: Both states meet the 80% coverage threshold.
```

Exit code 0 on full pass, 1 on any failure.

---

#### 5. `verify_state_api.py`

Confirms that all 4 legislative API endpoints return valid, non-empty data
for known legislators via the running Go API server. Tests the full HTTP stack:
HTTP request → Chi router → GORM → PostgreSQL.

**Requires the Go server to be running before executing this script.**

```bash
# In a separate terminal:
cd EV-Backend && go run .
```

**Usage:**

```bash
cd EV-Backend/scripts

# Default: hits http://localhost:5050
python3 verify_state_api.py

# Verbose: print first 200 chars of each JSON response
python3 verify_state_api.py --verbose

# Against production API
python3 verify_state_api.py --api-url https://api.empowered.vote
```

**Flags:**

| Flag | Description |
|------|-------------|
| `--api-url URL` | Base URL of the Go API server (default: http://localhost:5050) |
| `--verbose` | Print full JSON responses for each endpoint |

**Endpoints tested** (4 endpoints × 2 legislators = 8 checks):

| Endpoint | Response type | Key checked |
|----------|--------------|-------------|
| `/essentials/politician/{id}/committees` | JSON array | `name` key present |
| `/essentials/politician/{id}/bills` | JSON array | `title` key present |
| `/essentials/politician/{id}/votes` | JSON array | `result` key present |
| `/essentials/politician/{id}/legislative-summary` | JSON object | non-empty |

**Test legislators:**
- Rodric Bray (Indiana) — Senate President Pro Tempore
- Lisa Calderon (California) — Assembly Member

**Expected output (all passing):**

```
State Legislative API Verification
===================================

Indiana (Rodric Bray):
  /committees               PASS (5 items)
  /bills                    PASS (42 items)
  /votes                    PASS (931 items)
  /legislative-summary      PASS (summary present)

California (Lisa Calderon):
  /committees               PASS (3 items)
  /bills                    PASS (12 items)
  /votes                    PASS (187 items)
  /legislative-summary      PASS (summary present)

Result: 8/8 checks passed
```

Exit code 0 on full pass, 1 on any failure.

---

### New session playbook

When a new legislative session starts (or at the beginning of a new year), follow these
steps in order. **Always run `--dry-run` first** — this catches config errors before
writing to the database.

**Step 1: Update session config**

Edit `EV-Backend/scripts/state_legislative_config.json`. Update `current_year_start`
(and `previous_year_start` if you want to import the prior session too):

```json
{
  "states": {
    "IN": {
      "current_year_start": 2027,
      "previous_year_start": 2026,
      ...
    },
    "CA": {
      "current_year_start": 2027,
      "previous_year_start": 2025,
      ...
    }
  }
}
```

Both `import_state_committees.py` and `import_state_legislative.py` read from this file.
If it is missing, both scripts exit with a clear error message before touching the database.

**Step 2: Import Indiana committees (dry run first)**

```bash
cd EV-Backend/scripts
python3 import_state_committees.py --state IN --dry-run --verbose
```

Verify the output shows the correct session year and reasonable committee counts.
Then run without `--dry-run`:

```bash
python3 import_state_committees.py --state IN
```

Expected runtime: ~1 minute.

**Step 3: Import California committees (dry run first)**

```bash
python3 import_state_committees.py --state CA --dry-run --verbose
```

Verify OPENSTATES_API_KEY is loaded and the jurisdiction is `california`.
Then run without `--dry-run`:

```bash
python3 import_state_committees.py --state CA
```

Expected runtime: ~40 minutes. The script enforces a 6-second delay per Open States
API page (10 req/min free tier). Progress is cached to disk — if interrupted, re-running
will resume from the last completed page.

**Step 4: Import Indiana legislative data (dry run first)**

```bash
python3 import_state_legislative.py --state IN --sessions current --dry-run --verbose
```

Verify the correct session year appears in the output. Then run without `--dry-run`:

```bash
python3 import_state_legislative.py --state IN --sessions current
```

**Step 5: Import California legislative data (dry run first)**

```bash
python3 import_state_legislative.py --state CA --sessions current --dry-run --verbose
```

Then run without `--dry-run`:

```bash
python3 import_state_legislative.py --state CA --sessions current
```

**Step 6: Validate committee coverage**

```bash
python3 validate_committee_coverage.py --verbose
```

Both states should report PASS (>= 80% coverage). If either fails, check the import
logs for match failures and review the Troubleshooting section below.

**Step 7: Validate legislative data**

```bash
python3 validate_state_legislative.py --verbose
```

Both states should report PASS across all checks (bridge coverage >= 80%,
established zero-activity <= 10%). Review any flagged legislators.

**Step 8: Verify via Go API**

Start the Go server in a separate terminal:

```bash
cd EV-Backend && go run .
```

Then run:

```bash
cd EV-Backend/scripts
python3 verify_state_api.py --verbose
```

Expected: `8/8 checks passed`. If any endpoint fails, confirm the server started
successfully and that `DATABASE_URL` points to the same database the imports used.

---

### Tracking files

These files are stored in `~/.ev-backend/` and are created automatically on first run.
They persist across sessions and are never committed to git.

| File | Purpose |
|------|---------|
| `~/.ev-backend/legiscan_counter.json` | LegiScan monthly API query count. Resets at month boundary. Check before running imports to verify budget headroom. |
| `~/.ev-backend/legiscan_import_tracker.json` | Last import timestamp and counts per state (IN, CA). Logged at the start of each run so you can verify freshness. |
| `~/.ev-backend/committee_import_tracker.json` | Last committee import timestamp and counts per state. Same pattern as the LegiScan tracker. |
| `~/.ev-backend/dataset_hashes.json` | Cached dataset hashes from LegiScan. If the hash is unchanged since last import, the dataset download is skipped (no API call consumed). |

---

### Troubleshooting

#### 1. "server closed the connection unexpectedly" during CA committee import

**Symptom:** Error appears partway through the CA Open States import, typically after
10–15 minutes.

**Cause:** Supabase closes idle connections after approximately 15 minutes. The CA Open
States import takes ~40 minutes of API pagination — the initial database connection
opened before the API fetch can go idle while waiting for API responses.

**Fix:** Already handled in `import_state_committees.py`. After the Open States API fetch
completes, the script closes the stale connection and opens a fresh one before writing
to the database. If the error recurs, confirm that the reconnect block in
`import_committees()` is active (look for the `"Reconnecting to database after API fetch"`
log line in the output).

---

#### 2. LegiScan API budget exhaustion (HTTP 429 or empty responses)

**Symptom:** Import fails with HTTP 429, or `getDatasetList` returns empty results.

**Cause:** The free tier allows 30,000 queries/month. The dataset approach uses only ~5
queries per state per full run, but repeated re-runs or `--legiscan-check` validation
runs accumulate usage.

**Fix:** Check current usage:

```bash
cat ~/.ev-backend/legiscan_counter.json
# → { "month": "2026-03", "queries": 24 }
```

If near the limit, wait for the monthly reset (first of each month). Use `--dry-run` to
verify config without consuming API budget.

---

#### 3. Open States rate limiting (HTTP 429)

**Symptom:** `429 Too Many Requests` during CA committee import.

**Cause:** Free tier = 10 requests per minute.

**Fix:** No action needed. The script enforces a 6-second delay between pages and
includes automatic retry logic (3 retries, up to 60 seconds each). If a 429 occurs,
the retry handler waits for the `Retry-After` header duration and continues automatically.

---

#### 4. High "no match" count during CA committee import

**Symptom:** Import output shows thousands of `memberships_skipped_no_match` entries.

**Cause:** The Open States CA jurisdiction returns multi-state committees (interstate
legislative councils and national organizations) in addition to California-only committees.
These multi-state committees include legislators from other states who are not in our
database. 20,000+ no-match entries are expected and normal.

**Fix:** Not a bug. Ignore the raw no-match count. Check the actual coverage percentage
using `validate_committee_coverage.py`. As long as the result is >= 80%, the import
is complete.

---

#### 5. "No module named 'psycopg2'" or "No module named 'dotenv'"

**Fix:**

```bash
cd EV-Backend/scripts
pip install -r requirements-state.txt
```

If you are using a virtual environment:

```bash
source .venv/bin/activate
pip install -r requirements-state.txt
```

---

#### 6. `validate_state_legislative.py` reports zero-activity legislators

**Symptom:** Check D reports legislators with no bills or votes.

**Cause:** These legislators are missing bridge records in
`essentials.legislative_politician_id_map`. Without a bridge record linking them to their
LegiScan person ID, the import script cannot associate bills and votes to them.

Known missing bridges as of Phase 61:
- Robert Johnson (IN) — no LegiScan match found
- Blanca Pachecco (CA) — no LegiScan match found
- Suzette Valladares (CA) — no LegiScan match found

**Fix:** These require either a manual bridge record insertion (insert the correct
`id_type='legiscan'` + `id_value` row into `essentials.legislative_politician_id_map`)
or a reimport after the bridge is added. Full details are in
`.planning/phases/61-state-data-verification-gap-fill/61-STATE-LEGISLATIVE-AUDIT.md`.

The validation script treats these as PASS as long as the count stays at or below 10%
of the total roster.
