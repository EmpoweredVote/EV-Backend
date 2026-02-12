# Geofence-Based Address Lookup Setup

## Overview

This system enables **instant address → politician lookups** for pre-populated geographic areas (Monroe County, IN and LA County, CA) without calling external APIs. It uses:

1. **Google Maps** for address geocoding (address → lat/lng)
2. **PostGIS** for point-in-polygon queries (lat/lng → geo-IDs)
3. **Local database** for politician lookup (geo-IDs → officials)
4. **BallotReady fallback** for areas not yet imported

## How It Works

```
User Address: "123 Main St, Bloomington, IN"
    ↓
Google Maps Geocoding
    ↓
Lat/Lng: (39.1653, -86.5264)
    ↓
PostGIS Point-in-Polygon Query
    ↓
Geo-IDs: [18105, 180063000003, 1809, ...]
    (Monroe County, School District 3, Congressional District 9)
    ↓
Database JOIN (districts.geo_id → offices → politicians)
    ↓
Politicians: [All officials representing those districts]
```

### Key Data Linkage

**BallotReady** provides `district.geo_id` (e.g., `18105` for Monroe County)
↕
**Census TIGER** shapefiles have `GEOID` field with same values
↕
**PostGIS** stores polygon geometries indexed by `geo_id`

When a lat/lng point falls inside a polygon, we get the `geo_id`, which we use to look up politicians in the database.

## Setup Steps

### 1. Enable PostGIS

PostGIS is automatically enabled when the backend starts (`internal/essentials/setup.go`):

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

### 2. Import Shapefiles

**Prerequisites:**
```bash
pip install geopandas psycopg2-binary requests
export DATABASE_URL="postgresql://user:pass@host:5432/dbname"
```

**Run Import:**
```bash
cd EV-Backend/scripts
python3 import_shapefiles.py
```

This downloads and imports Census TIGER/Line shapefiles for:

**Monroe County, Indiana (FIPS: 18105):**
- County boundary
- Townships
- School districts
- Congressional District 9
- State legislative districts
- Voting districts

**LA County, California (FIPS: 06037):**
- County boundary
- Cities
- School districts
- Congressional districts
- State legislative districts

Expected import time: 10-15 minutes

### 3. Bulk Import Politicians

Use the existing bulk import endpoint to populate politician data for these areas:

```bash
# Log in as admin
# POST to /essentials/admin/import

curl -X POST http://localhost:5050/essentials/admin/import \
  -H 'Content-Type: application/json' \
  -H 'Cookie: session_id=your-session-cookie' \
  -d '{
    "zips": ["47401", "47403", "47404", "47405", "47406", "47407", "47408", ...],
    "delay_between_ms": 3000
  }'
```

**Monroe County ZIPs:** ~15 ZIPs (47401-47408, 47420, 47429, 47433, 47436, 47437, 47451, 47462)

**LA County ZIPs:** ~400 ZIPs (90001-90899, 91001-91899, etc.)

### 4. Verify Setup

**Check geofence coverage:**
```sql
SELECT COUNT(*), mtfcc
FROM essentials.geofence_boundaries
GROUP BY mtfcc;
```

**Test point-in-polygon:**
```sql
SELECT geo_id, name, mtfcc
FROM essentials.geofence_boundaries
WHERE ST_Contains(
    geometry,
    ST_SetSRID(ST_MakePoint(-86.5264, 39.1653), 4326)  -- Bloomington, IN
);
```

Expected result: Multiple geo-IDs including Monroe County (18105)

**Test address search:**
```bash
curl -X POST http://localhost:5050/essentials/politicians/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "107 S Indiana Ave, Bloomington, IN"}'
```

Check response headers:
- `X-Data-Status: fresh-local` = served from geofence data ✅
- `X-Geofence-Count: 8` = matched 8 districts
- `X-Data-Status: fresh` = fell back to BallotReady API

## Database Schema

### `essentials.geofence_boundaries`

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| geo_id | VARCHAR(50) | Census GEOID - **UNIQUE INDEX** |
| ocd_id | VARCHAR(255) | Open Civic Data ID |
| name | TEXT | District name |
| state | CHAR(2) | State abbreviation |
| mtfcc | VARCHAR(10) | Feature class code |
| geometry | GEOMETRY | Polygon/MultiPolygon in WGS84 |
| source | TEXT | Data source (e.g., "census_tiger_2024") |
| imported_at | TIMESTAMP | Import timestamp |

**Spatial Index:** GIST index on `geometry` column for fast point-in-polygon queries.

## Performance

**Cold lookup (area not imported):**
- Google geocoding: ~200ms
- PostGIS query: ~50ms (no results)
- BallotReady fallback: ~2-3 seconds
- **Total: ~2.5 seconds**

**Hot lookup (area pre-imported):**
- Google geocoding: ~200ms
- PostGIS query: ~50ms (returns 5-10 geo-IDs)
- Database JOIN: ~100ms
- **Total: ~350ms** (7x faster!)

## Maintenance

### Adding New Areas

To add coverage for a new county:

1. **Download TIGER shapefiles** for that state/county
2. **Run import script** with new filters
3. **Bulk import politicians** for all ZIPs in that county

### Updating Boundaries

Census releases new TIGER files annually. To update:

1. **Download new YEAR shapefiles**
2. **Delete old data**: `DELETE FROM essentials.geofence_boundaries WHERE source = 'census_tiger_2023'`
3. **Re-import** with new year

### Monitoring

Log messages to watch for:
- `✓ Served N officials from local geofence data` - Success!
- `no geofences found at (lat, lng)` - Area not imported yet
- `no politicians found for geo-IDs` - Geofences exist but politician data missing

## Troubleshooting

### "no geofences found" for known area

**Check if shapefiles were imported:**
```sql
SELECT COUNT(*) FROM essentials.geofence_boundaries WHERE state = 'IN';
```

If zero, run the import script.

### "no politicians found" despite geofences

**Check if politician data exists for those geo-IDs:**
```sql
SELECT d.geo_id, d.label, COUNT(p.id) as politician_count
FROM essentials.districts d
LEFT JOIN essentials.offices o ON o.district_id = d.id
LEFT JOIN essentials.politicians p ON p.office_id = o.id
WHERE d.geo_id = '18105'
GROUP BY d.geo_id, d.label;
```

If zero, run the bulk ZIP import for that area.

### PostGIS queries are slow

**Rebuild spatial index:**
```sql
REINDEX INDEX essentials.idx_geofence_boundaries_geometry;
ANALYZE essentials.geofence_boundaries;
```

## Future Enhancements

1. **Expand coverage** to all 50 states
2. **Cache geocoding results** (address → lat/lng) to reduce Google API costs
3. **Pre-compute ZIP → geofence mappings** for even faster lookups
4. **Add city/county autocomplete** using geofence_boundaries names

