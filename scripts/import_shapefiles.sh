#!/bin/bash
set -e

# Shapefile Import Script for Geofence Boundaries
# Imports Census TIGER/Line shapefiles for Monroe County, IN and LA County, CA
# Requires: ogr2ogr (GDAL), curl, unzip

YEAR=2024
WORK_DIR="./shapefile_data"
DB_CONNECTION="${DATABASE_URL}"

# Check dependencies
command -v ogr2ogr >/dev/null 2>&1 || { echo "Error: ogr2ogr (GDAL) not found. Install with: brew install gdal" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "Error: curl not found" >&2; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "Error: unzip not found" >&2; exit 1; }

if [ -z "$DB_CONNECTION" ]; then
    echo "Error: DATABASE_URL environment variable not set"
    exit 1
fi

mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

echo "========================================="
echo "Census TIGER/Line Shapefile Importer"
echo "Year: $YEAR"
echo "========================================="

# Function to download and import a shapefile
import_shapefile() {
    local url=$1
    local filename=$2
    local geo_type=$3
    local state_filter=$4
    local county_filter=$5

    echo ""
    echo "Processing: $filename ($geo_type)"
    echo "URL: $url"

    # Download
    if [ ! -f "$filename" ]; then
        echo "  Downloading..."
        curl -# -o "$filename" "$url"
    else
        echo "  Already downloaded"
    fi

    # Extract
    local extract_dir="${filename%.zip}"
    if [ ! -d "$extract_dir" ]; then
        echo "  Extracting..."
        unzip -q "$filename" -d "$extract_dir"
    else
        echo "  Already extracted"
    fi

    # Find .shp file
    local shp_file=$(find "$extract_dir" -name "*.shp" | head -1)
    if [ -z "$shp_file" ]; then
        echo "  Error: No .shp file found"
        return 1
    fi

    echo "  Importing to database..."

    # Build ogr2ogr command with filters
    local where_clause=""
    if [ -n "$state_filter" ] && [ -n "$county_filter" ]; then
        where_clause="-where \"STATEFP='$state_filter' AND COUNTYFP='$county_filter'\""
    elif [ -n "$state_filter" ]; then
        where_clause="-where \"STATEFP='$state_filter'\""
    fi

    # Import to PostGIS
    eval ogr2ogr -f PostgreSQL \
        "PG:$DB_CONNECTION" \
        "$shp_file" \
        -nln essentials.geofence_boundaries_staging \
        -append \
        -t_srs EPSG:4326 \
        -lco GEOMETRY_NAME=geometry \
        -lco SPATIAL_INDEX=GIST \
        -lco OVERWRITE=NO \
        $where_clause \
        -sql "SELECT GEOID as geo_id, NAME as name, MTFCC as mtfcc, geometry FROM $(basename ${shp_file%.shp})"

    echo "  ✓ Imported"
}

# Monroe County, Indiana (FIPS: 18105)
# State: 18 (Indiana), County: 105 (Monroe)
echo ""
echo "========================================="
echo "Monroe County, Indiana"
echo "========================================="

# Counties (for Monroe County itself)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/COUNTY/tl_${YEAR}_us_county.zip" \
    "tl_${YEAR}_us_county.zip" \
    "County" \
    "18" \
    "105"

# County Subdivisions (townships in Monroe County)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/COUSUB/tl_${YEAR}_18_cousub.zip" \
    "tl_${YEAR}_18_cousub.zip" \
    "County Subdivision" \
    "18" \
    "105"

# School Districts (unified) in Indiana
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/UNSD/tl_${YEAR}_18_unsd.zip" \
    "tl_${YEAR}_18_unsd.zip" \
    "School District (Unified)" \
    "" \
    ""

# Congressional Districts (Indiana CD-9)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/CD/tl_${YEAR}_us_cd118.zip" \
    "tl_${YEAR}_us_cd118.zip" \
    "Congressional District" \
    "18" \
    ""

# State Legislative Districts - Upper (Indiana State Senate)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/SLDU/tl_${YEAR}_18_sldu.zip" \
    "tl_${YEAR}_18_sldu.zip" \
    "State Senate District" \
    "" \
    ""

# State Legislative Districts - Lower (Indiana State House)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/SLDL/tl_${YEAR}_18_sldl.zip" \
    "tl_${YEAR}_18_sldl.zip" \
    "State House District" \
    "" \
    ""

# Voting Districts (precincts) in Monroe County
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/VTD/tl_${YEAR}_18_vtd.zip" \
    "tl_${YEAR}_18_vtd.zip" \
    "Voting District" \
    "" \
    ""

# Los Angeles County, California (FIPS: 06037)
# State: 06 (California), County: 037 (Los Angeles)
echo ""
echo "========================================="
echo "Los Angeles County, California"
echo "========================================="

# Counties (for LA County itself)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/COUNTY/tl_${YEAR}_us_county.zip" \
    "tl_${YEAR}_us_county.zip" \
    "County" \
    "06" \
    "037"

# County Subdivisions (cities/places in LA County)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/COUSUB/tl_${YEAR}_06_cousub.zip" \
    "tl_${YEAR}_06_cousub.zip" \
    "County Subdivision" \
    "06" \
    "037"

# Places (incorporated cities in California)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/PLACE/tl_${YEAR}_06_place.zip" \
    "tl_${YEAR}_06_place.zip" \
    "Place (City)" \
    "" \
    ""

# School Districts (unified) in California
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/UNSD/tl_${YEAR}_06_unsd.zip" \
    "tl_${YEAR}_06_unsd.zip" \
    "School District (Unified)" \
    "" \
    ""

# Congressional Districts (California - multiple districts in LA County)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/CD/tl_${YEAR}_us_cd118.zip" \
    "tl_${YEAR}_us_cd118.zip" \
    "Congressional District" \
    "06" \
    ""

# State Legislative Districts - Upper (California State Senate)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/SLDU/tl_${YEAR}_06_sldu.zip" \
    "tl_${YEAR}_06_sldu.zip" \
    "State Senate District" \
    "" \
    ""

# State Legislative Districts - Lower (California State Assembly)
import_shapefile \
    "https://www2.census.gov/geo/tiger/TIGER${YEAR}/SLDL/tl_${YEAR}_06_sldl.zip" \
    "tl_${YEAR}_06_sldl.zip" \
    "State Assembly District" \
    "" \
    ""

echo ""
echo "========================================="
echo "Post-processing"
echo "========================================="

# Normalize data from staging table to final table
echo "Normalizing imported data..."

psql "$DB_CONNECTION" <<SQL
-- Move data from staging to final table with metadata
INSERT INTO essentials.geofence_boundaries (geo_id, name, mtfcc, geometry, source, imported_at)
SELECT
    geo_id,
    name,
    mtfcc,
    geometry,
    'census_tiger_$YEAR' as source,
    NOW() as imported_at
FROM essentials.geofence_boundaries_staging
ON CONFLICT (geo_id) DO NOTHING;

-- Drop staging table
DROP TABLE IF EXISTS essentials.geofence_boundaries_staging;

-- Analyze table for query optimization
ANALYZE essentials.geofence_boundaries;

-- Show import summary
SELECT
    mtfcc,
    COUNT(*) as count
FROM essentials.geofence_boundaries
WHERE source = 'census_tiger_$YEAR'
GROUP BY mtfcc
ORDER BY mtfcc;
SQL

echo ""
echo "✓ Import complete!"
echo ""
echo "To verify the import, run:"
echo "  psql \"\$DATABASE_URL\" -c \"SELECT COUNT(*), mtfcc FROM essentials.geofence_boundaries GROUP BY mtfcc;\""
