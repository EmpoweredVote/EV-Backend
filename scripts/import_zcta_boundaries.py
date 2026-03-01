#!/usr/bin/env python3
"""
Import ZCTA (ZIP Code Tabulation Area) boundaries from Census TIGER/Line shapefiles.

Imports:
  - ZCTA boundaries for Indiana and California — MTFCC G6350

geo_id format: 5-digit ZIP code (e.g., "47403" for Bloomington, IN)

These enable area-intersection lookups for ZIP code searches. When a user searches
"47403", ResolveAreaBoundary finds the ZCTA boundary, then FindGeoIDsByAreaIntersection
returns only districts that overlap that ZIP polygon — instead of falling through to the
city boundary and returning all wards.

Note: ocd_id is intentionally NULL — ZCTAs are postal boundaries, not electoral districts.

Requires: python3.13 -m pip install -r requirements.txt
Usage: DATABASE_URL="postgresql://..." python3 import_zcta_boundaries.py
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
WORK_DIR = Path("./shapefile_data")
ZCTA_URL = f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/ZCTA520/tl_{YEAR}_us_zcta520.zip"

# States to import (FIPS codes)
TARGET_STATES = {"18", "06"}  # Indiana, California

# ZIP prefix ranges by state (fallback if STATEFP20 column missing)
# Source: USPS ZIP code prefix assignments
ZIP_PREFIXES_BY_STATE = {
    "18": range(460, 480),  # Indiana: 460-479
    "06": range(900, 962),  # California: 900-961
}


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


def filter_by_state(gdf):
    """Filter ZCTA records to target states using STATEFP20 or ZIP prefix fallback."""
    # Try STATEFP20 column first (present in TIGER ZCTA shapefiles)
    if 'STATEFP20' in gdf.columns:
        print(f"  Filtering by STATEFP20 column...")
        filtered = gdf[gdf['STATEFP20'].isin(TARGET_STATES)]
        if len(filtered) > 0:
            return filtered
        print(f"  WARNING: STATEFP20 filter returned 0 records, trying ZIP prefix fallback...")

    # Fallback: filter by ZIP code prefix ranges
    print(f"  Filtering by ZIP prefix ranges...")
    geo_col = 'GEOID20' if 'GEOID20' in gdf.columns else 'GEOID'
    mask = gdf[geo_col].apply(lambda z: any(
        int(z[:3]) in prefixes
        for prefixes in ZIP_PREFIXES_BY_STATE.values()
        if z[:3].isdigit()
    ))
    return gdf[mask]


def get_state_fips(row):
    """Get state FIPS for a ZCTA record."""
    if 'STATEFP20' in row.index and row['STATEFP20'] in TARGET_STATES:
        return row['STATEFP20']
    # Derive from ZIP prefix
    geo_col = 'GEOID20' if 'GEOID20' in row.index else 'GEOID'
    zip_code = row[geo_col]
    if len(zip_code) >= 3 and zip_code[:3].isdigit():
        prefix = int(zip_code[:3])
        for fips, prefixes in ZIP_PREFIXES_BY_STATE.items():
            if prefix in prefixes:
                return fips
    return None


def import_to_postgis(shp_path, engine):
    """Import ZCTA shapefile into essentials.geofence_boundaries"""
    print(f"  Loading shapefile: {shp_path.name}...")
    gdf = gpd.read_file(shp_path)
    print(f"  Total records in shapefile: {len(gdf)}")

    # Filter to target states
    gdf = filter_by_state(gdf)

    if len(gdf) == 0:
        print("  No records found after filtering")
        return 0

    # Reproject to WGS84 — TIGER ships in EPSG:4269 (NAD83)
    if gdf.crs != "EPSG:4326":
        print(f"  Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    # Validate geometry before import
    invalid_mask = ~gdf.geometry.is_valid
    if invalid_mask.any():
        print(f"  WARNING: {invalid_mask.sum()} invalid geometries — applying make_valid()")
        gdf.geometry = gdf.geometry.make_valid()
    else:
        print(f"  Geometry valid: all {len(gdf)} records pass is_valid check")

    # Determine column names (ZCTA 2020 uses *20 suffix)
    geo_col = 'GEOID20' if 'GEOID20' in gdf.columns else 'GEOID'
    name_col = 'NAMELSAD20' if 'NAMELSAD20' in gdf.columns else ('NAMELSAD' if 'NAMELSAD' in gdf.columns else geo_col)

    # Build clean dataframe
    records = []
    for _, row in gdf.iterrows():
        state_fips = get_state_fips(row)
        if state_fips is None:
            continue
        records.append({
            'geo_id': row[geo_col],              # 5-digit ZIP, e.g., "47403"
            'name': row.get(name_col, ''),       # e.g., "ZCTA5 47403"
            'mtfcc': 'G6350',                    # ZCTA feature class
            'state': state_fips,
            'geometry': row['geometry'],
            'source': f'census_tiger_{YEAR}',
            'imported_at': datetime.now().isoformat(),
            # ocd_id intentionally NULL — ZCTAs have no OCD-ID
        })

    gdf_clean = gpd.GeoDataFrame(records, geometry='geometry', crs="EPSG:4326")

    # Summary
    state_counts = gdf_clean['state'].value_counts()
    print(f"  Found {len(gdf_clean)} ZCTA boundaries (G6350)")
    for state, count in state_counts.items():
        label = "Indiana" if state == "18" else "California" if state == "06" else state
        print(f"    {label} (FIPS {state}): {count}")
    print(f"  Sample records:")
    for _, row in gdf_clean.head(5).iterrows():
        print(f"    geo_id={row['geo_id']}  name={row['name']}  state={row['state']}")

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
            print("  Some records exist, importing individually...")
            return import_individually(gdf_clean, engine)
        print(f"  Import failed: {e}")
        raise


def verify_import(engine):
    """Verify import: count, ST_IsValid check, point-in-polygon tests"""
    print(f"\n{'=' * 60}")
    print("Verification")
    print(f"{'=' * 60}")

    with engine.connect() as conn:
        # Count ZCTA records by state
        for fips, label in [("18", "Indiana"), ("06", "California")]:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM essentials.geofence_boundaries "
                "WHERE state = :state AND mtfcc = 'G6350'"
            ), {"state": fips})
            count = r.scalar()
            print(f"\n  [{label}] G6350 (ZCTA) records: {count}")

        # Total ZCTA count
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries WHERE mtfcc = 'G6350'"
        ))
        total = r.scalar()
        print(f"  [Total] G6350 (ZCTA) records: {total}")

        # ST_IsValid check
        r2 = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE mtfcc = 'G6350' AND NOT ST_IsValid(geometry)"
        ))
        invalid_count = r2.scalar()
        validity_status = "PASS" if invalid_count == 0 else "FAIL"
        print(f"  [{validity_status}] Invalid G6350 geometries: {invalid_count} (expected: 0)")

        # Point-in-polygon test: Bloomington, IN (39.1653, -86.5264)
        print(f"\n  Point-in-polygon test (Bloomington, IN: 39.1653, -86.5264):")
        r3 = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-86.5264, 39.1653), 4326)
            )
            AND mtfcc = 'G6350'
        """))
        bloom_hits = list(r3)
        if bloom_hits:
            for row in bloom_hits:
                geo_id_ok = row[0] == '47403'
                status = "PASS" if geo_id_ok else "INFO"
                print(f"    [{status}] {row[2]} | geo_id={row[0]} | {row[1]}")
            print("    SUCCESS: Bloomington resolves to ZCTA boundary")
        else:
            print("    [FAIL] No G6350 hit for Bloomington — check CRS or import")

        # Point-in-polygon test: Beverly Hills, CA (34.0736, -118.4004)
        print(f"\n  Point-in-polygon test (Beverly Hills, CA: 34.0736, -118.4004):")
        r4 = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.4004, 34.0736), 4326)
            )
            AND mtfcc = 'G6350'
        """))
        bh_hits = list(r4)
        if bh_hits:
            for row in bh_hits:
                geo_id_ok = row[0] == '90210'
                status = "PASS" if geo_id_ok else "INFO"
                print(f"    [{status}] {row[2]} | geo_id={row[0]} | {row[1]}")
            print("    SUCCESS: Beverly Hills resolves to ZCTA boundary")
        else:
            print("    [FAIL] No G6350 hit for Beverly Hills — check CRS or import")

        # Area intersection test: what districts overlap ZIP 47403?
        print(f"\n  Area intersection test (ZIP 47403 → overlapping districts):")
        r5 = conn.execute(text("""
            SELECT gb2.geo_id, gb2.name, gb2.mtfcc
            FROM essentials.geofence_boundaries gb1
            JOIN essentials.geofence_boundaries gb2
              ON ST_Contains(gb1.geometry, ST_PointOnSurface(gb2.geometry))
                 OR ST_Contains(gb2.geometry, ST_PointOnSurface(gb1.geometry))
            WHERE gb1.geo_id = '47403'
              AND gb1.mtfcc = 'G6350'
              AND gb2.geo_id != '47403'
            ORDER BY gb2.mtfcc, gb2.geo_id
        """))
        overlap_hits = list(r5)
        if overlap_hits:
            for row in overlap_hits:
                print(f"    {row[2]} | geo_id={row[0]} | {row[1]}")
            # Check if fewer than 6 X0001 (council ward) hits
            ward_hits = [r for r in overlap_hits if r[2] == 'X0001']
            if 0 < len(ward_hits) < 6:
                print(f"    [PASS] {len(ward_hits)} council ward(s) overlap ZIP 47403 (fewer than all 6)")
            elif len(ward_hits) == 6:
                print(f"    [WARN] All 6 wards overlap — ZIP boundary may be larger than expected")
            else:
                print(f"    [INFO] {len(ward_hits)} council ward(s) found")
        else:
            print("    [WARN] No overlapping districts found — check boundary data")

        # Summary count by MTFCC for ZCTA
        print(f"\n  geofence_boundaries ZCTA summary:")
        r6 = conn.execute(text("""
            SELECT state, COUNT(*) as count
            FROM essentials.geofence_boundaries
            WHERE mtfcc = 'G6350'
            GROUP BY state
            ORDER BY state
        """))
        state_labels = {"06": "California", "18": "Indiana"}
        for row in r6:
            label = state_labels.get(row[0], row[0])
            print(f"    FIPS {row[0]} ({label}): {row[1]}")


def main():
    print("=" * 60)
    print("Import ZCTA Boundaries (G6350)")
    print(f"Source: Census TIGER/Line {YEAR}")
    print(f"States: Indiana (18), California (06)")
    print("=" * 60)

    load_env()
    engine = get_engine()

    # Show current state
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries WHERE mtfcc = 'G6350'"
        ))
        print(f"\n  Current G6350 (ZCTA) count: {r.scalar()}")

    WORK_DIR.mkdir(exist_ok=True)

    # Download and extract
    filename = WORK_DIR / Path(ZCTA_URL.split('/')[-1])
    download_file(ZCTA_URL, filename)

    extract_dir = WORK_DIR / filename.stem
    extract_shapefile(filename, extract_dir)

    shp_files = list(extract_dir.glob("*.shp"))
    if not shp_files:
        print(f"  No .shp file found in {extract_dir}")
        return

    # Import
    count = import_to_postgis(shp_files[0], engine)

    print(f"\n{'=' * 60}")
    print(f"Import complete! Total new records: {count}")
    print(f"{'=' * 60}")

    verify_import(engine)


if __name__ == "__main__":
    main()
