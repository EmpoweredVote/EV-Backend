#!/usr/bin/env python3
"""
Import California incorporated place boundaries from Census TIGER/Line shapefiles.

Imports:
  - Incorporated Places (cities/towns) for California — MTFCC G4110

geo_id format: STATEFP(2) + PLACEFP(5) = 7 chars (e.g., "0644000" for Los Angeles city)

These link to essentials.districts via geo_id for address-based city council lookups.
The geofence_lookup.go FindPoliticiansByGeoMatches function joins on d.geo_id and
maps G4110 to LOCAL/LOCAL_EXEC district types.

Note: ocd_id is intentionally NULL — consistent with existing Indiana G4110 records.
The lookup code uses geo_id (not ocd_id) for matching, so ocd_id is not needed.

Requires: python3.13 -m pip install -r requirements.txt
Usage: DATABASE_URL="postgresql://..." python3 import_ca_place_boundaries.py
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
PLACE_URL = f"https://www2.census.gov/geo/tiger/TIGER{YEAR}/PLACE/tl_{YEAR}_{CA_FIPS}_place.zip"
MTFCC_FILTER = "G4110"  # Incorporated Place only — exclude G4150 (CDP) and G4210


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


def import_to_postgis(shp_path, engine):
    """Import CA G4110 shapefile into essentials.geofence_boundaries"""
    print(f"  Loading shapefile: {shp_path.name}...")
    gdf = gpd.read_file(shp_path)

    # Filter to California incorporated places only
    if 'STATEFP' in gdf.columns:
        gdf = gdf[gdf['STATEFP'] == CA_FIPS]
    gdf = gdf[gdf['MTFCC'] == MTFCC_FILTER]  # G4110 only — exclude CDPs (G4150) and other

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

    # Build clean dataframe
    # TIGER PLACE uses NAMELSAD (e.g., "Los Angeles city") — prefer over NAME
    name_col = 'NAMELSAD' if 'NAMELSAD' in gdf.columns else 'NAME'
    records = []
    for _, row in gdf.iterrows():
        records.append({
            'geo_id': row['GEOID'],         # STATEFP(2) + PLACEFP(5) = 7 chars
            'name': row.get(name_col, ''),  # e.g., "Los Angeles city"
            'mtfcc': row.get('MTFCC', MTFCC_FILTER),
            'state': row.get('STATEFP', CA_FIPS),
            'geometry': row['geometry'],
            'source': f'census_tiger_{YEAR}',
            'imported_at': datetime.now().isoformat(),
            # ocd_id intentionally NULL — consistent with Indiana G4110 pattern
        })

    gdf_clean = gpd.GeoDataFrame(records, geometry='geometry', crs="EPSG:4326")

    print(f"  Found {len(gdf_clean)} CA incorporated places (G4110)")
    print(f"  MTFCC: {gdf_clean['mtfcc'].unique().tolist()}")
    print(f"  Sample records:")
    for _, row in gdf_clean.head(5).iterrows():
        print(f"    geo_id={row['geo_id']}  name={row['name']}")

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
        # Count CA G4110 records
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE state = '06' AND mtfcc = 'G4110'"
        ))
        count = r.scalar()
        count_status = "PASS" if 450 <= count <= 520 else "WARN"
        print(f"\n  [{count_status}] CA G4110 records: {count} (expected 450-520)")

        # ST_IsValid check — success criterion #5
        r2 = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE state = '06' AND mtfcc = 'G4110' AND NOT ST_IsValid(geometry)"
        ))
        invalid_count = r2.scalar()
        validity_status = "PASS" if invalid_count == 0 else "FAIL"
        print(f"  [{validity_status}] Invalid CA G4110 geometries: {invalid_count} (expected: 0)")

        # All CA boundaries ST_IsValid check
        r3 = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE state = '06' AND NOT ST_IsValid(geometry)"
        ))
        all_invalid = r3.scalar()
        all_validity_status = "PASS" if all_invalid == 0 else "FAIL"
        print(f"  [{all_validity_status}] Invalid CA boundaries (all types): {all_invalid} (expected: 0)")

        # Point-in-polygon test: LA City Hall (34.0537, -118.2427)
        print("\n  Point-in-polygon test (LA City Hall: 34.0537, -118.2427):")
        r4 = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
            AND mtfcc = 'G4110'
        """))
        la_hits = list(r4)
        if la_hits:
            for row in la_hits:
                geo_id_ok = row[0] == '0644000'
                status = "PASS" if geo_id_ok else "WARN"
                print(f"    [{status}] {row[2]} | geo_id={row[0]} | {row[1]}")
            print("    SUCCESS: LA City Hall resolves to G4110 boundary")
        else:
            print("    [FAIL] No G4110 hit for LA City Hall — check CRS or import")

        # Point-in-polygon test: Pasadena City Hall (34.1478, -118.1445)
        print("\n  Point-in-polygon test (Pasadena City Hall: 34.1478, -118.1445):")
        r5 = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.1445, 34.1478), 4326)
            )
            AND mtfcc = 'G4110'
        """))
        pasadena_hits = list(r5)
        if pasadena_hits:
            for row in pasadena_hits:
                print(f"    [PASS] {row[2]} | geo_id={row[0]} | {row[1]}")
        else:
            print("    [WARN] No G4110 hit for Pasadena City Hall")

        # Full hierarchy test: all MTFCC types at LA City Hall
        print("\n  Full hierarchy test (LA City Hall — all boundary types):")
        r6 = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
            ORDER BY mtfcc
        """))
        all_hits = list(r6)
        hit_mtfccs = {row[2] for row in all_hits}
        for row in all_hits:
            print(f"    {row[2]} | geo_id={row[0]} | {row[1]}")
        required = {'G5200', 'G5210', 'G5220', 'G5420', 'G4110'}
        missing = required - hit_mtfccs
        if not missing:
            print("    [PASS] All five required MTFCC types present")
        else:
            print(f"    [FAIL] Missing MTFCC types: {missing}")

        # Summary count by MTFCC for CA
        print("\n  CA geofence_boundaries by MTFCC:")
        r7 = conn.execute(text("""
            SELECT mtfcc, COUNT(*) as count
            FROM essentials.geofence_boundaries
            WHERE state = '06'
            GROUP BY mtfcc
            ORDER BY mtfcc
        """))
        desc_map = {
            'G4020': 'County',
            'G4040': 'County Subdivision',
            'G4110': 'Incorporated Place',
            'G5200': 'Congressional District',
            'G5210': 'State Senate (SLDU)',
            'G5220': 'State Assembly (SLDL)',
            'G5420': 'Unified School District',
        }
        for row in r7:
            desc = desc_map.get(row[0], row[0])
            print(f"    {row[0]} ({desc}): {row[1]}")


def main():
    print("=" * 60)
    print("Import California Incorporated Place Boundaries (G4110)")
    print(f"Source: Census TIGER/Line {YEAR}")
    print("MTFCC filter: G4110 (Incorporated Place only — excludes CDPs)")
    print("=" * 60)

    load_env()
    engine = get_engine()

    # Show current state
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE state = '06' AND mtfcc = 'G4110'"
        ))
        print(f"\n  Current CA G4110 count: {r.scalar()}")

    WORK_DIR.mkdir(exist_ok=True)

    # Download and extract
    filename = WORK_DIR / Path(PLACE_URL.split('/')[-1])
    download_file(PLACE_URL, filename)

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
