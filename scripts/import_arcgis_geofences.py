#!/usr/bin/env python3
"""
Import geofence boundaries from ArcGIS FeatureServer endpoints.
Reads source configuration from arcgis_sources.json.

Imports:
  - LA County Supervisor Districts (5) — MTFCC X0001
  - LA City Council Districts (15) — MTFCC X0001
  - Other LA County city council districts (when source_url is confirmed)

geo_id format: OCD-ID string (e.g., ocd-division/country:us/state:ca/place:los_angeles/council_district:1)
These link to essentials.districts via geo_id for address-based council ward lookups.

Uses delete-before-insert for idempotent re-runs.
Applies shapely make_valid() to every geometry; sets quality_flag='geometry_repaired' when repair needed.

Requires: python3.13 -m pip install -r requirements.txt
Usage: DATABASE_URL="postgresql://..." python3 import_arcgis_geofences.py
  Or: automatically reads from ../.env.local
"""

import json
import sys
import time
import requests
import geopandas as gpd
from shapely.geometry import shape
from shapely.validation import make_valid
from sqlalchemy import text
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_engine, load_env

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "arcgis_sources.json"


def load_config():
    """Load ArcGIS source configuration from arcgis_sources.json."""
    with open(CONFIG_FILE) as f:
        return json.load(f)


def fetch_arcgis_geojson(url, retries=3):
    """Fetch GeoJSON from ArcGIS FeatureServer with exponential backoff.

    Args:
        url: Full ArcGIS query URL including ?f=geojson&outSR=4326
        retries: Number of attempts before giving up

    Returns:
        Parsed JSON dict with 'features' key, or None on failure
    """
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "features" not in data:
                raise ValueError(f"Response missing 'features' key: {list(data.keys())}")
            return data
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt  # 1s, 2s
                print(f"  Attempt {attempt + 1} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  FAILED after {retries} attempts: {e}")
                return None


def build_geodataframe(data, cfg, district_field, geo_id_template, source_label):
    """Convert ArcGIS GeoJSON response into a clean GeoDataFrame for import.

    Applies make_valid() to every geometry before building the GeoDataFrame.
    Sets quality_flag='geometry_repaired' when repair was needed.

    Args:
        data: Parsed GeoJSON dict with 'features' list
        cfg: Config dict for this source (has 'name_template', 'state', etc.)
        district_field: Property key containing the district number
        geo_id_template: OCD-ID template string with {n} placeholder
        source_label: Source identifier string for the 'source' column

    Returns:
        GeoDataFrame with columns: geo_id, ocd_id, name, mtfcc, state, geometry,
        source, imported_at, quality_flag
    """
    mtfcc = cfg.get("mtfcc", "X0001")
    state = cfg.get("state", "06")
    name_template = cfg.get("name_template", "District {n}")

    rows = []
    repaired = 0
    for feat in data["features"]:
        props = feat["properties"]
        geom_raw = shape(feat["geometry"])

        # Apply make_valid() before building GeoDataFrame
        qflag = None
        if not geom_raw.is_valid:
            geom_raw = make_valid(geom_raw)
            qflag = "geometry_repaired"
            repaired += 1
            district_val = props.get(district_field, "?")
            print(f"  WARNING: Invalid geometry repaired for district {district_val}")

        # District number — handle both string ("1"-"5") and integer (1-15) fields
        district_num = props.get(district_field)
        if district_num is None:
            print(f"  WARNING: district_field '{district_field}' not found in {list(props.keys())}")
            continue

        n = int(district_num)
        geo_id = geo_id_template.format(n=n)

        # Derive name from properties or template
        name = (
            props.get("name")
            or props.get("dist_name")
            or props.get("LABEL")
            or name_template.format(n=n)
        )

        rows.append({
            "geo_id": geo_id,
            "ocd_id": geo_id,  # For X0001 boundaries, geo_id IS the OCD-ID
            "name": name,
            "mtfcc": mtfcc,
            "state": state,
            "geometry": geom_raw,
            "source": source_label,
            "imported_at": datetime.now().isoformat(),
            "quality_flag": qflag,
        })

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    print(f"  Loaded {len(gdf)} features, {repaired} geometries repaired")
    return gdf


def delete_and_insert(gdf, engine):
    """Delete existing records for these geo_ids and insert fresh data.

    Uses delete-before-insert pattern for idempotent re-runs.
    """
    geo_ids = gdf["geo_id"].tolist()
    mtfcc = gdf["mtfcc"].iloc[0] if len(gdf) > 0 else "X0001"

    with engine.connect() as conn:
        result = conn.execute(
            text("DELETE FROM essentials.geofence_boundaries WHERE geo_id = ANY(:ids) AND mtfcc = :mtfcc"),
            {"ids": geo_ids, "mtfcc": mtfcc},
        )
        deleted = result.rowcount
        if deleted > 0:
            print(f"  Deleted {deleted} existing records (re-import)")
        conn.commit()

    gdf.to_postgis(
        "geofence_boundaries",
        engine,
        schema="essentials",
        if_exists="append",
        index=False,
    )
    print(f"  Inserted {len(gdf)} records")
    return len(gdf)


def import_supervisor_districts(engine, config):
    """Import LA County supervisor district boundaries.

    Fetches from LA County ArcGIS MapServer, builds GeoDataFrame with OCD-ID
    geo_ids, and uses delete-before-insert for idempotency.

    Args:
        engine: SQLAlchemy engine
        config: Full arcgis_sources.json config dict

    Returns:
        (count, error) tuple — count of imported records, error string or None
    """
    cfg = config["supervisor_districts"]
    url = cfg["source_url"]
    district_field = cfg["district_field"]
    geo_id_template = cfg["geo_id_template"]
    source_label = cfg["source_label"]

    print(f"\n  Fetching supervisor districts from LA County ArcGIS...")
    data = fetch_arcgis_geojson(url)
    if data is None:
        return 0, "Failed to fetch supervisor districts from LA County ArcGIS"

    feature_count = len(data.get("features", []))
    expected = cfg.get("count", 5)
    print(f"  Received {feature_count} features (expected {expected})")
    if feature_count != expected:
        print(f"  WARNING: Expected {expected} supervisor districts, got {feature_count}")

    gdf = build_geodataframe(data, cfg, district_field, geo_id_template, source_label)
    if len(gdf) == 0:
        return 0, "No valid features found in supervisor district response"

    count = delete_and_insert(gdf, engine)
    return count, None


def import_city_council(engine, entry):
    """Import council district boundaries for a single city.

    Skips entries with source_url of 'TBD', None, or election_type of 'at-large'.

    Args:
        engine: SQLAlchemy engine
        entry: Single entry from config['city_council'] list

    Returns:
        (city, count, error) tuple
    """
    city = entry.get("city", "Unknown")
    source_url = entry.get("source_url")
    election_type = entry.get("election_type")

    # Skip at-large cities — reuse G4110 place boundary, no X0001 import needed
    if election_type == "at-large":
        print(f"  Skipping {city}: at-large council (no X0001 import needed)")
        return city, 0, None

    # Skip TBD entries — URL not yet confirmed
    if not source_url or source_url == "TBD":
        print(f"  Skipping {city}: source_url is TBD")
        return city, 0, None

    district_field = entry.get("district_field")
    if not district_field or district_field == "TBD":
        print(f"  Skipping {city}: district_field is TBD")
        return city, 0, None

    geo_id_template = entry["geo_id_template"]
    source_label = entry["source_label"]
    expected = entry.get("districts")

    print(f"\n  Fetching {city} council districts from {source_url[:60]}...")
    data = fetch_arcgis_geojson(source_url)
    if data is None:
        return city, 0, f"Failed to fetch {city} council districts"

    feature_count = len(data.get("features", []))
    if expected:
        print(f"  Received {feature_count} features (expected {expected})")
        if feature_count != expected:
            print(f"  WARNING: Expected {expected} districts for {city}, got {feature_count}")
    else:
        print(f"  Received {feature_count} features")

    gdf = build_geodataframe(data, entry, district_field, geo_id_template, source_label)
    if len(gdf) == 0:
        return city, 0, f"No valid features found for {city}"

    count = delete_and_insert(gdf, engine)
    return city, count, None


def import_all(engine):
    """Orchestrate all imports: supervisors first, then each city council entry.

    Reports summary with counts and failures at the end.

    Returns:
        dict with 'supervisor_count', 'council_counts', 'failures', 'total'
    """
    config = load_config()

    print("=" * 60)
    print("LA County ArcGIS Geofence Import")
    print(f"Config: {CONFIG_FILE}")
    print("=" * 60)

    results = {
        "supervisor_count": 0,
        "council_counts": {},
        "failures": [],
        "total": 0,
    }

    # Import supervisor districts
    print("\n--- LA County Supervisor Districts ---")
    count, err = import_supervisor_districts(engine, config)
    if err:
        print(f"  ERROR: {err}")
        results["failures"].append(("LA County Supervisors", err))
    else:
        print(f"  SUCCESS: {count} supervisor district boundaries imported")
        results["supervisor_count"] = count
        results["total"] += count

    # Import each city council entry
    print("\n--- City Council Districts ---")
    for entry in config.get("city_council", []):
        city, count, err = import_city_council(engine, entry)
        if err:
            print(f"  ERROR ({city}): {err}")
            results["failures"].append((city, err))
        else:
            results["council_counts"][city] = count
            results["total"] += count

    # Summary report
    print(f"\n{'=' * 60}")
    print("Import Summary")
    print(f"{'=' * 60}")
    print(f"  Supervisor districts: {results['supervisor_count']}/5")
    la_count = results["council_counts"].get("Los Angeles", 0)
    print(f"  LA City council wards: {la_count}/15")
    print(f"  Total records imported: {results['total']}")
    if results["failures"]:
        print(f"  Failures ({len(results['failures'])}):")
        for source, err in results["failures"]:
            print(f"    - {source}: {err}")
    else:
        print("  No failures")

    return results


def verify_import(engine):
    """Run verification queries after import.

    Checks:
    - Supervisor district count (expected: 5)
    - LA City council ward count (expected: 15)
    - ST_IsValid for all new X0001 records
    - Point-in-polygon: LA City Hall (should hit supervisor + council ward)
    - Point-in-polygon: Compton area (unincorporated, should hit supervisor NOT council ward)
    - geo_id join test: geofence_boundaries.geo_id joins to essentials.districts.ocd_id
    """
    print(f"\n{'=' * 60}")
    print("Verification")
    print(f"{'=' * 60}")

    with engine.connect() as conn:

        # Count supervisor districts
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE geo_id LIKE 'ocd-division/country:us/state:ca/county:los_angeles/council_district:%' "
            "AND mtfcc = 'X0001'"
        ))
        sup_count = r.scalar()
        sup_status = "PASS" if sup_count == 5 else "FAIL"
        print(f"\n  [{sup_status}] Supervisor districts: {sup_count}/5")

        # Count LA City council wards
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE geo_id LIKE 'ocd-division/country:us/state:ca/place:los_angeles/council_district:%' "
            "AND mtfcc = 'X0001'"
        ))
        la_count = r.scalar()
        la_status = "PASS" if la_count == 15 else "FAIL"
        print(f"  [{la_status}] LA City council wards: {la_count}/15")

        # ST_IsValid check
        r = conn.execute(text(
            "SELECT COUNT(*) FROM essentials.geofence_boundaries "
            "WHERE mtfcc = 'X0001' AND state = '06' AND NOT ST_IsValid(geometry)"
        ))
        invalid_count = r.scalar()
        validity_status = "PASS" if invalid_count == 0 else "FAIL"
        print(f"  [{validity_status}] Invalid X0001 CA geometries: {invalid_count} (expected: 0)")

        # List all supervisor districts
        print("\n  Supervisor districts:")
        r = conn.execute(text(
            "SELECT geo_id, name, quality_flag FROM essentials.geofence_boundaries "
            "WHERE geo_id LIKE 'ocd-division/country:us/state:ca/county:los_angeles/council_district:%' "
            "AND mtfcc = 'X0001' ORDER BY geo_id"
        ))
        for row in r:
            qflag = row[2] if row[2] else "clean"
            print(f"    {row[1]} | {row[0]} | {qflag}")

        # List all LA City council wards
        print("\n  LA City council wards:")
        r = conn.execute(text(
            "SELECT geo_id, name, quality_flag FROM essentials.geofence_boundaries "
            "WHERE geo_id LIKE 'ocd-division/country:us/state:ca/place:los_angeles/council_district:%' "
            "AND mtfcc = 'X0001' ORDER BY geo_id"
        ))
        for row in r:
            qflag = row[2] if row[2] else "clean"
            print(f"    {row[1]} | {row[0]} | {qflag}")

        # Point-in-polygon: LA City Hall (34.0537, -118.2427)
        print("\n  Point-in-polygon test (LA City Hall: 34.0537, -118.2427):")
        r = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
            AND mtfcc = 'X0001'
            ORDER BY geo_id
        """))
        city_hall_hits = list(r)
        has_supervisor = any("county:los_angeles" in row[0] for row in city_hall_hits)
        has_council = any("place:los_angeles" in row[0] for row in city_hall_hits)
        for row in city_hall_hits:
            print(f"    {row[2]} | {row[0]} | {row[1]}")
        pip_status = "PASS" if (has_supervisor and has_council) else "FAIL"
        print(f"  [{pip_status}] LA City Hall: supervisor={has_supervisor}, council_ward={has_council} (both expected)")

        # Point-in-polygon: Compton/unincorporated area (33.8958, -118.2201)
        # Should hit supervisor district but NOT LA City council ward
        print("\n  Point-in-polygon test (Compton area: 33.8958, -118.2201):")
        r = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2201, 33.8958), 4326)
            )
            AND mtfcc = 'X0001'
            ORDER BY geo_id
        """))
        compton_hits = list(r)
        compton_sup = any("county:los_angeles" in row[0] for row in compton_hits)
        compton_council = any("place:los_angeles" in row[0] for row in compton_hits)
        for row in compton_hits:
            print(f"    {row[2]} | {row[0]} | {row[1]}")
        compton_status = "PASS" if (compton_sup and not compton_council) else "WARN"
        print(f"  [{compton_status}] Compton: supervisor={compton_sup}, la_council={compton_council} (supervisor=True, la_council=False expected)")

        # Full hierarchy at LA City Hall — show all MTFCC types
        print("\n  Full boundary hierarchy (LA City Hall — all MTFCC types):")
        r = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE ST_Covers(
                geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
            ORDER BY mtfcc, geo_id
        """))
        all_hits = list(r)
        hit_mtfccs = {row[2] for row in all_hits}
        for row in all_hits:
            print(f"    {row[2]} | {row[0]} | {row[1]}")
        required = {"G5200", "G5210", "G5220", "G5420", "G4110", "X0001"}
        missing = required - hit_mtfccs
        hierarchy_status = "PASS" if not missing else "FAIL"
        print(f"  [{hierarchy_status}] Required MTFCC types at LA City Hall: {sorted(hit_mtfccs)} (missing: {sorted(missing)})")

        # geo_id join test: geofence geo_ids must match essentials.districts.ocd_id
        print("\n  geo_id join test (geofence_boundaries.geo_id = districts.ocd_id):")
        r = conn.execute(text("""
            SELECT gb.geo_id, d.ocd_id, d.district_type, d.label
            FROM essentials.geofence_boundaries gb
            JOIN essentials.districts d ON gb.geo_id = d.ocd_id
            WHERE gb.mtfcc = 'X0001' AND gb.state = '06'
            ORDER BY gb.geo_id
        """))
        join_rows = list(r)
        print(f"  Matching rows: {len(join_rows)} (expected: {sup_count + la_count} for supervisor + LA council)")
        for row in join_rows[:10]:
            print(f"    {row[0]} | district_type={row[2]} | label={row[3]}")
        if len(join_rows) > 10:
            print(f"    ... ({len(join_rows) - 10} more)")

        join_status = "PASS" if len(join_rows) >= (sup_count + la_count) else "FAIL"
        print(f"  [{join_status}] geo_id join: {len(join_rows)}/{sup_count + la_count} boundaries join to districts")

    # Overall pass/fail
    print(f"\n{'=' * 60}")
    all_passed = (
        sup_status == "PASS"
        and la_status == "PASS"
        and validity_status == "PASS"
        and pip_status == "PASS"
        and join_status == "PASS"
    )
    if all_passed:
        print("ALL VERIFICATION CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED — review output above")
    print(f"{'=' * 60}")

    return all_passed


def main():
    load_env()
    engine = get_engine()

    results = import_all(engine)
    verify_import(engine)

    if results["failures"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
