
# Geofence Boundary Import Scripts

These scripts download and import US Census TIGER/Line shapefiles into the PostGIS database to enable local address → politician lookups without hitting external APIs.

## Prerequisites

### Python Script (Recommended)

```bash
# Install Python dependencies
pip install geopandas psycopg2-binary requests
```

### Bash Script (Alternative)

```bash
# Install GDAL (includes ogr2ogr)
brew install gdal  # macOS
# or
apt-get install gdal-bin  # Ubuntu/Debian
```

## Usage

### 1. Set Database Connection

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

### 2. Run Import

**Python (recommended):**
```bash
cd EV-Backend/scripts
python3 import_shapefiles.py
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
pip install geopandas psycopg2-binary requests
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

