#!/usr/bin/env python3
"""
Import missing geofence boundaries: State House (SLDL) and Congressional Districts (CD)
for Indiana. These are needed for address-based politician lookups.

Requires: pip install geopandas psycopg2-binary requests sqlalchemy
Usage: DATABASE_URL="postgresql://..." python3 import_missing_geofences.py
  Or: automatically reads from ../.env.local
"""

import os
import sys
import requests
import zipfile
import geopandas as gpd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

YEAR = 2024
WORK_DIR = Path("./shapefile_data")

# Shapefiles to import
SHAPEFILES = {
    "sldl_in": {
        "url": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDL/tl_{YEAR}_18_sldl.zip",
        "desc": "Indiana State House Districts (SLDL)",
        "state_filter": "18",
    },
    "cd_in": {
        "url": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/CD/tl_{YEAR}_18_cd119.zip",
        "desc": "Indiana Congressional Districts (119th Congress)",
        "state_filter": "18",
    },
}


def load_env():
    """Load DATABASE_URL from .env.local if not already set"""
    if os.getenv("DATABASE_URL"):
        return
    env_path = Path(__file__).parent.parent / ".env.local"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABASE_URL="):
                    os.environ["DATABASE_URL"] = line.split("=", 1)[1]
                    print(f"  Loaded DATABASE_URL from {env_path}")
                    return
    print("Error: DATABASE_URL not set and .env.local not found")
    sys.exit(1)


def get_engine():
    """Create SQLAlchemy engine, URL-encoding the password if needed"""
    from urllib.parse import urlparse, quote_plus, urlunparse
    raw_url = os.getenv("DATABASE_URL")
    parsed = urlparse(raw_url)
    # Re-encode the password to handle special chars like @
    if parsed.password:
        encoded_pw = quote_plus(parsed.password)
        netloc = f"{parsed.username}:{encoded_pw}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        safe_url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    else:
        safe_url = raw_url
    return create_engine(safe_url)


def download_file(url, dest):
    if dest.exists():
        print(f"  Already downloaded: {dest.name}")
        return
    print(f"  Downloading: {dest.name}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(dest, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"  Downloaded: {dest.name}")


def extract_shapefile(zip_path, extract_dir):
    if extract_dir.exists():
        print(f"  Already extracted: {extract_dir.name}")
        return
    print(f"  Extracting: {zip_path.name}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)


def import_to_postgis(shp_path, engine, state_filter=None):
    """Import shapefile records into essentials.geofence_boundaries using upsert logic"""
    print(f"  Loading shapefile: {shp_path.name}...")
    gdf = gpd.read_file(shp_path)

    if state_filter and 'STATEFP' in gdf.columns:
        gdf = gdf[gdf['STATEFP'] == state_filter]

    if len(gdf) == 0:
        print("  No records match filters")
        return 0

    if gdf.crs != "EPSG:4326":
        print(f"  Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    # Select and rename columns
    columns_map = {
        'GEOID': 'geo_id',
        'NAME': 'name',
        'MTFCC': 'mtfcc',
        'STATEFP': 'state',
        'geometry': 'geometry'
    }
    available_cols = {k: v for k, v in columns_map.items() if k in gdf.columns}
    gdf_clean = gdf[list(available_cols.keys())].rename(columns=available_cols)
    gdf_clean['source'] = f'census_tiger_{YEAR}'
    gdf_clean['imported_at'] = datetime.now()

    print(f"  Found {len(gdf_clean)} records to import")
    print(f"  MTFCC values: {gdf_clean['mtfcc'].unique().tolist()}")
    print(f"  Sample geo_ids: {gdf_clean['geo_id'].head(5).tolist()}")

    # Use to_postgis with append (unique constraint on geo_id+mtfcc handles dedup)
    try:
        gdf_clean.to_postgis(
            "geofence_boundaries",
            engine,
            schema='essentials',
            if_exists='append',
            index=False
        )
        print(f"  Imported {len(gdf_clean)} records")
        return len(gdf_clean)
    except Exception as e:
        # If unique constraint violation, records already exist
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            print(f"  Some records already exist, importing individually...")
            return import_individually(gdf_clean, engine)
        print(f"  Import failed: {e}")
        return 0


def import_individually(gdf, engine):
    """Import records one by one, skipping duplicates"""
    imported = 0
    skipped = 0
    for idx, row in gdf.iterrows():
        single = gdf.iloc[[gdf.index.get_loc(idx)]]
        try:
            single.to_postgis(
                "geofence_boundaries",
                engine,
                schema='essentials',
                if_exists='append',
                index=False
            )
            imported += 1
        except Exception:
            skipped += 1
    print(f"  Imported {imported}, skipped {skipped} duplicates")
    return imported


def main():
    print("=" * 60)
    print("Import Missing Geofence Boundaries")
    print("SLDL (State House) + CD (Congressional) for Indiana")
    print("=" * 60)

    load_env()

    engine = get_engine()

    # Verify connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM essentials.geofence_boundaries"))
        count = result.scalar()
        print(f"\n  Current geofence_boundaries count: {count}")

    WORK_DIR.mkdir(exist_ok=True)

    total_imported = 0

    for key, config in SHAPEFILES.items():
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

        count = import_to_postgis(shp_files[0], engine, config.get("state_filter"))
        total_imported += count

    # Show final summary
    print(f"\n{'=' * 60}")
    print(f"Import complete! Total new records: {total_imported}")
    print(f"{'=' * 60}")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT mtfcc, COUNT(*) as count
            FROM essentials.geofence_boundaries
            GROUP BY mtfcc
            ORDER BY mtfcc
        """))
        print("\nGeofence boundaries by MTFCC:")
        for row in result:
            print(f"  {row[0]}: {row[1]} records")


if __name__ == "__main__":
    main()
