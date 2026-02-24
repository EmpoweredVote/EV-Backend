#!/usr/bin/env python3
"""
Import California legislative district boundaries from Census TIGER/Line shapefiles.

Imports:
  - Congressional Districts (119th Congress) — MTFCC G5200
  - State Senate Districts (SLDU) — MTFCC G5210
  - State Assembly Districts (SLDL) — MTFCC G5220

Each boundary gets:
  - geo_id: Census GEOID (e.g., "0643" for CA CD-43)
  - mtfcc: Feature class code for district type disambiguation
  - ocd_id: Open Civic Data ID (e.g., "ocd-division/country:us/state:ca/cd:43")

These link to essentials.districts via geo_id for address-based politician lookups.

Requires: pip install geopandas psycopg2-binary requests sqlalchemy
Usage: DATABASE_URL="postgresql://..." python3 import_ca_legislative_geofences.py
  Or: automatically reads from ../.env.local
"""

import os
import sys
import requests
import zipfile
import geopandas as gpd
from sqlalchemy import text
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_engine, load_env

YEAR = 2024
CA_FIPS = "06"
WORK_DIR = Path("./shapefile_data")

# FIPS state code to abbreviation (for OCD-ID generation)
FIPS_TO_STATE = {
    "06": "ca",
}

# Shapefiles to import
SHAPEFILES = [
    {
        "key": "cd_ca",
        "url": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/CD/tl_{YEAR}_{CA_FIPS}_cd119.zip",
        "desc": "California Congressional Districts (119th Congress)",
        "mtfcc": "G5200",
        "ocd_type": "cd",
        # CD GEOID format: STATEFP(2) + CD119FP(2) = "0643"
        "district_num_slice": slice(2, 4),  # extract district number from GEOID
        "district_num_pad": 0,  # don't strip leading zeros beyond int conversion
    },
    {
        "key": "sldu_ca",
        "url": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDU/tl_{YEAR}_{CA_FIPS}_sldu.zip",
        "desc": "California State Senate Districts (SLDU)",
        "mtfcc": "G5210",
        "ocd_type": "sldu",
        # SLDU GEOID format: STATEFP(2) + SLDUST(3) = "06022"
        "district_num_slice": slice(2, 5),
    },
    {
        "key": "sldl_ca",
        "url": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDL/tl_{YEAR}_{CA_FIPS}_sldl.zip",
        "desc": "California State Assembly Districts (SLDL)",
        "mtfcc": "G5220",
        "ocd_type": "sldl",
        # SLDL GEOID format: STATEFP(2) + SLDLST(3) = "06048"
        "district_num_slice": slice(2, 5),
    },
]


def download_file(url, dest):
    if dest.exists():
        print(f"  Already downloaded: {dest.name}")
        return
    print(f"  Downloading: {dest.name}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    with open(dest, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size > 0:
                pct = (downloaded / total_size) * 100
                print(f"\r    Progress: {pct:.1f}%", end='', flush=True)
    print(f"\r  Downloaded: {dest.name}          ")


def extract_shapefile(zip_path, extract_dir):
    if extract_dir.exists():
        print(f"  Already extracted: {extract_dir.name}")
        return
    print(f"  Extracting: {zip_path.name}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)


def geoid_to_ocd_id(geoid, ocd_type):
    """Convert Census GEOID to OCD-ID.

    Examples:
        geoid="0643", ocd_type="cd"   -> "ocd-division/country:us/state:ca/cd:43"
        geoid="06022", ocd_type="sldu" -> "ocd-division/country:us/state:ca/sldu:22"
        geoid="06048", ocd_type="sldl" -> "ocd-division/country:us/state:ca/sldl:48"
    """
    state_fips = geoid[:2]
    state_abbr = FIPS_TO_STATE.get(state_fips, state_fips)

    if ocd_type == "cd":
        district_num = int(geoid[2:4])
    else:
        district_num = int(geoid[2:5])

    return f"ocd-division/country:us/state:{state_abbr}/{ocd_type}:{district_num}"


def import_to_postgis(shp_path, engine, config):
    """Import shapefile into essentials.geofence_boundaries with OCD-IDs"""
    print(f"  Loading shapefile: {shp_path.name}...")
    gdf = gpd.read_file(shp_path)

    # Filter to California
    if 'STATEFP' in gdf.columns:
        gdf = gdf[gdf['STATEFP'] == CA_FIPS]

    if len(gdf) == 0:
        print("  No California records found")
        return 0

    # Reproject to WGS84
    if gdf.crs != "EPSG:4326":
        print(f"  Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    # Build clean dataframe
    # Use NAMELSAD for name (e.g., "Congressional District 43") — TIGER legislative
    # files use NAMELSAD instead of NAME
    name_col = 'NAMELSAD' if 'NAMELSAD' in gdf.columns else 'NAME'

    records = []
    for _, row in gdf.iterrows():
        geoid = row['GEOID']
        ocd_id = geoid_to_ocd_id(geoid, config['ocd_type'])
        records.append({
            'geo_id': geoid,
            'ocd_id': ocd_id,
            'name': row.get(name_col, ''),
            'mtfcc': row.get('MTFCC', config['mtfcc']),
            'state': row.get('STATEFP', CA_FIPS),
            'geometry': row['geometry'],
            'source': f'census_tiger_{YEAR}',
            'imported_at': datetime.now().isoformat(),
        })

    gdf_clean = gpd.GeoDataFrame(records, geometry='geometry', crs="EPSG:4326")

    print(f"  Found {len(gdf_clean)} districts to import")
    print(f"  MTFCC: {gdf_clean['mtfcc'].unique().tolist()}")
    print(f"  Sample records:")
    for _, row in gdf_clean.head(3).iterrows():
        print(f"    geo_id={row['geo_id']}  ocd_id={row['ocd_id']}  name={row['name']}")

    # Import with duplicate handling
    try:
        gdf_clean.to_postgis(
            "geofence_boundaries",
            engine,
            schema='essentials',
            if_exists='append',
            index=False,
        )
        print(f"  Imported {len(gdf_clean)} records")
        return len(gdf_clean)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"  Some records exist, importing individually...")
            return import_individually(gdf_clean, engine)
        print(f"  Import failed: {e}")
        raise


def import_individually(gdf, engine):
    """Import records one by one, skipping duplicates"""
    imported = 0
    skipped = 0
    for idx in range(len(gdf)):
        single = gdf.iloc[[idx]]
        try:
            single.to_postgis(
                "geofence_boundaries",
                engine,
                schema='essentials',
                if_exists='append',
                index=False,
            )
            imported += 1
        except Exception:
            skipped += 1
    print(f"  Imported {imported}, skipped {skipped} duplicates")
    return imported


def verify_import(engine):
    """Run verification queries to confirm import worked"""
    print(f"\n{'=' * 60}")
    print("Verification")
    print(f"{'=' * 60}")

    with engine.connect() as conn:
        # Count by MTFCC for CA
        result = conn.execute(text("""
            SELECT mtfcc, COUNT(*) as count
            FROM essentials.geofence_boundaries
            WHERE state = '06'
            GROUP BY mtfcc
            ORDER BY mtfcc
        """))
        print("\nCA geofence boundaries by MTFCC:")
        for row in result:
            desc = {
                'G4020': 'County',
                'G4040': 'County Subdivision',
                'G4110': 'Incorporated Place',
                'G5200': 'Congressional District',
                'G5210': 'State Senate (SLDU)',
                'G5220': 'State Assembly (SLDL)',
                'G5420': 'School District',
            }.get(row[0], row[0])
            print(f"  {row[0]} ({desc}): {row[1]}")

        # Show all legislative districts with OCD-IDs
        result = conn.execute(text("""
            SELECT geo_id, ocd_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE state = '06' AND mtfcc IN ('G5200', 'G5210', 'G5220')
            ORDER BY mtfcc, geo_id
        """))
        print("\nCA legislative boundaries imported:")
        current_mtfcc = None
        for row in result:
            if row[3] != current_mtfcc:
                current_mtfcc = row[3]
                print(f"\n  {current_mtfcc}:")
            print(f"    {row[0]} | {row[1]} | {row[2]}")

        # Test point-in-polygon for downtown LA (City Hall: 34.0537, -118.2427)
        print("\nPoint-in-polygon test (LA City Hall: 34.0537, -118.2427):")
        result = conn.execute(text("""
            SELECT geo_id, mtfcc, name, ocd_id
            FROM essentials.geofence_boundaries
            WHERE ST_Contains(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
            ORDER BY mtfcc
        """))
        hits = list(result)
        if hits:
            for row in hits:
                print(f"  {row[1]} | geo_id={row[0]} | {row[2]} | {row[3]}")
        else:
            print("  No hits — check if boundaries imported correctly")


def main():
    print("=" * 60)
    print("Import California Legislative District Geofences")
    print(f"Source: Census TIGER/Line {YEAR}")
    print("=" * 60)

    load_env()
    engine = get_engine()

    # Show current state
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries WHERE state = '06'"
        ))
        print(f"\n  Current CA geofence_boundaries count: {result.scalar()}")

    WORK_DIR.mkdir(exist_ok=True)

    total_imported = 0

    for config in SHAPEFILES:
        print(f"\n{'=' * 60}")
        print(f"Processing: {config['desc']}")
        print(f"{'=' * 60}")

        filename = WORK_DIR / Path(config['url'].split('/')[-1])
        download_file(config['url'], filename)

        extract_dir = WORK_DIR / filename.stem
        extract_shapefile(filename, extract_dir)

        shp_files = list(extract_dir.glob("*.shp"))
        if not shp_files:
            print(f"  No .shp file found in {extract_dir}")
            continue

        count = import_to_postgis(shp_files[0], engine, config)
        total_imported += count

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Import complete! Total new records: {total_imported}")
    print(f"{'=' * 60}")

    verify_import(engine)


if __name__ == "__main__":
    main()
