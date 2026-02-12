#!/usr/bin/env python3
"""
Census TIGER/Line Shapefile Importer (Fixed URL parsing)

Downloads and imports geofence boundaries for Monroe County, IN and LA County, CA.
Requires: pip install geopandas psycopg2-binary requests sqlalchemy
"""

import os
import sys
import requests
import zipfile
import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path

YEAR = 2024
CD_YEAR = 2023  # Congressional District data for 2024 not yet available, use 2023
WORK_DIR = Path("./shapefile_data")

# Target counties
MONROE_IN = {"state": "18", "county": "105", "name": "Monroe County, IN"}
LA_CA = {"state": "06", "county": "037", "name": "Los Angeles County, CA"}

# Shapefile sources
SHAPEFILES = {
    "county": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/COUNTY/tl_{YEAR}_us_county.zip",
    "cousub_in": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/COUSUB/tl_{YEAR}_18_cousub.zip",
    "cousub_ca": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/COUSUB/tl_{YEAR}_06_cousub.zip",
    "unsd_in": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/UNSD/tl_{YEAR}_18_unsd.zip",
    "unsd_ca": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/UNSD/tl_{YEAR}_06_unsd.zip",
    # Skip CD - will fetch from BallotReady which has up-to-date district data
    # "cd": f"https://www2.census.gov/geo/tiger/TIGER{CD_YEAR}/CD/tl_{CD_YEAR}_us_cd118.zip",
    "sldu_in": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDU/tl_{YEAR}_18_sldu.zip",
    "sldu_ca": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDU/tl_{YEAR}_06_sldu.zip",
    "sldl_in": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDL/tl_{YEAR}_18_sldl.zip",
    "sldl_ca": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/SLDL/tl_{YEAR}_06_sldl.zip",
    "place_ca": f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/PLACE/tl_{YEAR}_06_place.zip",
}


def download_file(url, dest):
    """Download a file with progress indicator"""
    if dest.exists():
        print(f"  ✓ Already downloaded: {dest.name}")
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
                percent = (downloaded / total_size) * 100
                print(f"\r    Progress: {percent:.1f}%", end='', flush=True)

    print(f"\r  ✓ Downloaded: {dest.name}    ")


def extract_shapefile(zip_path, extract_dir):
    """Extract shapefile from ZIP"""
    if extract_dir.exists():
        print(f"  ✓ Already extracted: {extract_dir.name}")
        return

    print(f"  Extracting: {zip_path.name}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"  ✓ Extracted to: {extract_dir}")


def get_db_engine():
    """Get SQLAlchemy engine from DATABASE_URL"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("Error: DATABASE_URL environment variable not set")
        sys.exit(1)

    # SQLAlchemy handles URL parsing better than psycopg2
    return create_engine(db_url)


def import_shapefile_to_postgis(shp_path, table_name, state_filter=None, county_filter=None):
    """Import shapefile to PostGIS"""
    print(f"  Loading shapefile: {shp_path.name}...")
    gdf = gpd.read_file(shp_path)

    # Filter by state/county if specified
    if state_filter and 'STATEFP' in gdf.columns:
        gdf = gdf[gdf['STATEFP'] == state_filter]
    if county_filter and 'COUNTYFP' in gdf.columns:
        gdf = gdf[gdf['COUNTYFP'] == county_filter]

    if len(gdf) == 0:
        print(f"  ⚠ No records match filters")
        return 0

    # Reproject to WGS84 (EPSG:4326)
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

    # Only select columns that exist
    available_cols = {k: v for k, v in columns_map.items() if k in gdf.columns}
    gdf_clean = gdf[list(available_cols.keys())].rename(columns=available_cols)

    # Add metadata
    gdf_clean['source'] = f'census_tiger_{YEAR}'
    from datetime import datetime
    gdf_clean['imported_at'] = datetime.now()

    print(f"  Importing {len(gdf_clean)} records to {table_name}...")

    # Connect and import using SQLAlchemy
    engine = get_db_engine()
    try:
        # Use geopandas to_postgis for efficient import
        gdf_clean.to_postgis(
            table_name,
            engine,
            schema='essentials',
            if_exists='append',
            index=False
        )
        print(f"  ✓ Imported {len(gdf_clean)} records")
        return len(gdf_clean)
    except Exception as e:
        print(f"  ✗ Import failed: {e}")
        return 0


def main():
    print("=" * 60)
    print("Census TIGER/Line Shapefile Importer")
    print(f"Year: {YEAR}")
    print("=" * 60)

    # Check dependencies
    try:
        import geopandas
        import sqlalchemy
    except ImportError as e:
        print(f"Error: Missing dependency: {e}")
        print("Install with: pip install geopandas psycopg2-binary requests sqlalchemy")
        sys.exit(1)

    # Create work directory
    WORK_DIR.mkdir(exist_ok=True)
    os.chdir(WORK_DIR)

    total_imported = 0

    # Process each shapefile
    for key, url in SHAPEFILES.items():
        print(f"\n{'=' * 60}")
        print(f"Processing: {key}")
        print(f"{'=' * 60}")

        # Download
        filename = Path(url.split('/')[-1])
        download_file(url, filename)

        # Extract
        extract_dir = filename.stem
        extract_shapefile(filename, Path(extract_dir))

        # Find .shp file
        shp_files = list(Path(extract_dir).glob("*.shp"))
        if not shp_files:
            print(f"  ✗ No .shp file found in {extract_dir}")
            continue

        shp_path = shp_files[0]

        # Determine filters based on shapefile type
        state_filter = None
        county_filter = None

        if "county" in key.lower():
            # Import both Monroe and LA counties
            for county_info in [MONROE_IN, LA_CA]:
                print(f"\n  Importing {county_info['name']}...")
                count = import_shapefile_to_postgis(
                    shp_path,
                    "geofence_boundaries",
                    state_filter=county_info['state'],
                    county_filter=county_info['county']
                )
                total_imported += count

        elif "_in" in key:
            # Indiana-specific shapefiles
            count = import_shapefile_to_postgis(
                shp_path,
                "geofence_boundaries",
                state_filter=MONROE_IN['state']
            )
            total_imported += count

        elif "_ca" in key:
            # California-specific shapefiles
            count = import_shapefile_to_postgis(
                shp_path,
                "geofence_boundaries",
                state_filter=LA_CA['state']
            )
            total_imported += count

        elif "cd" in key:
            # Congressional districts (import for both states)
            for county_info in [MONROE_IN, LA_CA]:
                print(f"\n  Importing CDs for {county_info['name']}...")
                count = import_shapefile_to_postgis(
                    shp_path,
                    "geofence_boundaries",
                    state_filter=county_info['state']
                )
                total_imported += count

    print(f"\n{'=' * 60}")
    print(f"✓ Import complete!")
    print(f"Total records imported: {total_imported}")
    print(f"{'=' * 60}")

    # Show summary
    print("\nDatabase summary:")
    engine = get_db_engine()
    try:
        import pandas as pd
        query = f"""
            SELECT mtfcc, COUNT(*) as count
            FROM essentials.geofence_boundaries
            WHERE source = 'census_tiger_{YEAR}'
            GROUP BY mtfcc
            ORDER BY mtfcc
        """
        df = pd.read_sql(query, engine)

        print("\nImported by feature type:")
        for _, row in df.iterrows():
            print(f"  {row['mtfcc'] or 'NULL'}: {row['count']} features")

    except Exception as e:
        print(f"Could not fetch summary: {e}")


if __name__ == "__main__":
    main()
