#!/usr/bin/env python3
"""
Schema migration + geo_id fix for Phase 36 politician gap-fill.

This script does two things:
1. Schema migration: adds is_active and data_source columns to essentials.politicians
2. Geo_id fix: populates geo_id for all CA LOCAL and LOCAL_EXEC districts

The geo_id fix enables the join chain:
    geofence_boundaries -> districts -> offices -> politicians
for all Phase 35 geofences (supervisor districts + city council wards).

Usage:
    DATABASE_URL="postgresql://..." python3 gap_fill_geo_ids.py
    Or: automatically reads from ../.env.local

Idempotent: safe to run multiple times.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env

import psycopg2
import psycopg2.extras
from urllib.parse import urlparse, quote_plus

# Register UUID adapter (consistent with promote_scraped_officials.py)
psycopg2.extras.register_uuid()


def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Handles passwords containing special characters (e.g., '@', '/')
    by parsing the URL and connecting with keyword arguments.
    Consistent with the connect() pattern in promote_scraped_officials.py.
    """
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set")
        sys.exit(1)

    parsed = urlparse(raw_url)
    # Use keyword args to avoid URL-encoding issues with special chars in passwords
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }
    # Pass any query string options (e.g., sslmode=require)
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v

    return psycopg2.connect(**kwargs)


def migrate_schema(cur):
    """Add is_active and data_source columns to essentials.politicians (idempotent)."""
    print("\n" + "=" * 60)
    print("Part 1: Schema migration")
    print("=" * 60)

    print("  Adding is_active and data_source columns (IF NOT EXISTS)...")
    cur.execute("""
        ALTER TABLE essentials.politicians
            ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
            ADD COLUMN IF NOT EXISTS data_source text
    """)
    print("  Columns added (or already existed).")

    # Backfill: existing rows get is_active = true (already set by DEFAULT, but
    # rows inserted before this migration will have is_active=false if the default
    # wasn't applied — explicit backfill ensures consistency)
    cur.execute("""
        UPDATE essentials.politicians
        SET is_active = true
        WHERE data_source IS NULL
        RETURNING id
    """)
    backfilled = cur.rowcount
    print(f"  Backfilled is_active=true for {backfilled} rows with no data_source.")


def fix_local_geo_ids(cur):
    """Set geo_id = ocd_id for all CA LOCAL districts where geo_id is missing.

    This covers:
    - LA County supervisor districts (5 expected)
    - LA City council districts (15 expected)
    - Long Beach, Pasadena, Torrance, Inglewood, West Covina council districts
      imported in Phase 35 Plan 02 (31 expected)

    The WHERE clause (geo_id IS NULL OR geo_id = '') makes this idempotent.
    """
    print("\n" + "=" * 60)
    print("Part 2A: Fix geo_ids for CA LOCAL districts")
    print("=" * 60)

    cur.execute("""
        UPDATE essentials.districts
        SET geo_id = ocd_id
        WHERE state = 'CA'
          AND district_type = 'LOCAL'
          AND (geo_id IS NULL OR geo_id = '')
          AND ocd_id LIKE 'ocd-division/country:us/state:ca/%%'
        RETURNING id, ocd_id, district_type, label
    """)
    rows = cur.fetchall()
    print(f"  Updated {len(rows)} CA LOCAL district geo_ids.")
    if rows:
        print("  First 10 rows updated:")
        for row in rows[:10]:
            print(f"    [{row['district_type']}] {row['label']} -> {row['ocd_id']}")
        if len(rows) > 10:
            print(f"    ... and {len(rows) - 10} more")


def fix_mayor_geo_id(cur):
    """Set geo_id = '0644000' for the LA City mayor (LOCAL_EXEC) district.

    The mayor district uses Census GEOID (G4110) not OCD-ID because:
    - Phase 34 imported the LA city boundary with geo_id='0644000'
    - geofence_lookup.go joins on geo_id, not ocd_id
    - Using OCD-ID format would not match the Phase 34 geofence record
    """
    print("\n" + "=" * 60)
    print("Part 2B: Fix geo_id for LA City mayor (LOCAL_EXEC)")
    print("=" * 60)

    cur.execute("""
        UPDATE essentials.districts
        SET geo_id = '0644000'
        WHERE state = 'CA'
          AND district_type = 'LOCAL_EXEC'
          AND ocd_id = 'ocd-division/country:us/state:ca/place:los_angeles'
          AND (geo_id IS NULL OR geo_id = '')
        RETURNING id, ocd_id, geo_id
    """)
    rows = cur.fetchall()
    if rows:
        row = rows[0]
        print(f"  Updated LA City mayor: ocd_id={row['ocd_id']} -> geo_id={row['geo_id']}")
    else:
        print("  LA City mayor geo_id already set (or district not found).")


def verify_geo_ids(cur):
    """Run verification queries to confirm geo_id population is correct."""
    print("\n" + "=" * 60)
    print("Part 3: Verification")
    print("=" * 60)

    # Supervisor districts
    cur.execute("""
        SELECT COUNT(*) as count
        FROM essentials.districts
        WHERE ocd_id LIKE '%%county:los_angeles/council_district:%%'
          AND geo_id IS NOT NULL AND geo_id != ''
    """)
    supervisor_count = cur.fetchone()["count"]
    print(f"  LA County supervisor districts with geo_id: {supervisor_count} (expect 5)")

    # LA City council districts
    cur.execute("""
        SELECT COUNT(*) as count
        FROM essentials.districts
        WHERE ocd_id LIKE '%%place:los_angeles/council_district:%%'
          AND geo_id IS NOT NULL AND geo_id != ''
    """)
    city_council_count = cur.fetchone()["count"]
    print(f"  LA City council districts with geo_id: {city_council_count} (expect 15)")

    # LA City mayor
    cur.execute("""
        SELECT COUNT(*) as count
        FROM essentials.districts
        WHERE ocd_id = 'ocd-division/country:us/state:ca/place:los_angeles'
          AND district_type = 'LOCAL_EXEC'
          AND geo_id = '0644000'
    """)
    mayor_count = cur.fetchone()["count"]
    print(f"  LA City mayor district with geo_id='0644000': {mayor_count} (expect 1)")

    # All CA LOCAL districts with geo_id set
    cur.execute("""
        SELECT district_type, COUNT(*) as count
        FROM essentials.districts
        WHERE state = 'CA'
          AND geo_id IS NOT NULL AND geo_id != ''
          AND district_type IN ('LOCAL', 'LOCAL_EXEC')
        GROUP BY district_type
        ORDER BY district_type
    """)
    print("\n  All CA LOCAL/LOCAL_EXEC districts with geo_id set:")
    for row in cur.fetchall():
        print(f"    {row['district_type']}: {row['count']}")

    # Warn if counts are unexpected
    if supervisor_count < 5:
        print(f"\n  WARNING: Expected 5 supervisor districts, got {supervisor_count}")
        print("           Run this script after Phase 35 Plan 01 geofences are imported.")
    if city_council_count < 15:
        print(f"\n  WARNING: Expected 15 LA City council districts, got {city_council_count}")
        print("           Run this script after Phase 35 Plan 02 geofences are imported.")
    if mayor_count < 1:
        print("\n  WARNING: LA City mayor district not found or geo_id not set.")
        print("           Check that ocd_id='ocd-division/country:us/state:ca/place:los_angeles'")
        print("           and district_type='LOCAL_EXEC' exists in essentials.districts.")


def main():
    print("=" * 60)
    print("Phase 36 — Schema Migration + Geo-ID Fix")
    print("=" * 60)

    load_env()
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        migrate_schema(cur)
        fix_local_geo_ids(cur)
        fix_mayor_geo_id(cur)
        verify_geo_ids(cur)
        conn.commit()
        print("\nAll changes committed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
