
# Geofence Boundary Import Scripts

These scripts download and import US Census TIGER/Line shapefiles into the PostGIS database to enable local address → politician lookups without hitting external APIs.

## Prerequisites

### Python Version

**Python 3.10 or newer is required.**

The system `python3` on macOS is Python 3.9.6 and will NOT work — geopandas 1.1.2 dropped Python 3.9 support. Use Python 3.13 or 3.12 from Homebrew:

```bash
# Install Python 3.13 via Homebrew (if not already installed)
brew install python@3.13

# Verify version
/opt/homebrew/bin/python3.13 --version
# → Python 3.13.x
```

### Install Dependencies

Use the pinned `requirements.txt` for a one-command setup:

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

### Bash Script (Alternative)

```bash
# Install GDAL (includes ogr2ogr)
brew install gdal  # macOS
# or
apt-get install gdal-bin  # Ubuntu/Debian
```

## Usage

### 1. Set Database Connection

**Important:** Use the Supabase **direct connection** (port 5432), NOT the pooler (port 6543).
The pooler breaks bulk imports with `prepared statement already exists` errors.

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

Or create `EV-Backend/.env.local` (never committed to git) with:
```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

The scripts automatically read from `.env.local` if `DATABASE_URL` is not set in environment.

### 2. Run Import

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

### 3. Verify Import

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

## Shared Utilities

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

## What Gets Imported

### Monroe County, Indiana (FIPS: 18105)
- County boundaries
- Townships (county subdivisions)
- School districts
- Congressional District 9
- State legislative districts (Senate & House)
- Voting districts (precincts)

### Los Angeles County, California (FIPS: 06037)
- County boundaries
- Cities and places
- School districts
- Congressional districts (multiple)
- State legislative districts (Senate & Assembly)

## Expected Feature Counts

| Feature Type | MTFCC | Monroe, IN | LA County, CA |
|--------------|-------|------------|---------------|
| County | G4020 | 1 | 1 |
| Township | G4040 | ~10 | - |
| City/Place | G4110 | ~5 | ~80 |
| School District | G5420 | ~3 | ~80 |
| Congressional District | G5200 | 1 | ~15 |
| State Senate | G5210 | ~2 | ~10 |
| State House/Assembly | G5220 | ~3 | ~20 |

## Troubleshooting

### "No module named 'geopandas'"
```bash
python3.13 -m pip install -r requirements.txt
```

### "Python version mismatch" or import errors with Python 3.9
The macOS system `python3` is 3.9.6 — geopandas 1.1.2 requires Python 3.10+.
Always use `python3.13` (or `python3.12`) explicitly:
```bash
/opt/homebrew/bin/python3.13 import_missing_geofences.py
```

### "ogr2ogr: command not found"
```bash
brew install gdal  # macOS
```

### "Unable to connect to database"
Check that `DATABASE_URL` is set and PostGIS extension is enabled:
```sql
SELECT PostGIS_version();
```

### "prepared statement already exists"
You are connecting via the Supabase pooler (port 6543). Switch to the direct connection (port 5432):
```
DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

### Slow imports
The Python script is optimized for reliability over speed. First import may take 10-15 minutes.

## Data Source

All shapefiles are from the US Census Bureau TIGER/Line program:
https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

Data is public domain and updated annually.

## Next Steps

After importing shapefiles:

1. **Verify geofence coverage**:
```sql
SELECT geo_id, name, mtfcc
FROM essentials.geofence_boundaries
WHERE state = 'IN'
LIMIT 10;
```

2. **Test point-in-polygon lookup**:
```sql
SELECT geo_id, name
FROM essentials.geofence_boundaries
WHERE ST_Contains(
    geometry,
    ST_SetSRID(ST_MakePoint(-86.5264, 39.1653), 4326)  -- Bloomington, IN
);
```

3. **Run the bulk ZIP import** to populate politician data for these areas.
