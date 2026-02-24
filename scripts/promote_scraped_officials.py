#!/usr/bin/env python3
"""
Promote scraped officials from essentials.scraped_officials into the main
essentials schema (politicians, offices, districts, chambers).

This script:
1. Fixes 7 incorrect "possible" matches
2. Updates geo_id/mtfcc on existing matched districts for geofence lookups
3. Creates chambers/governments as needed
4. Creates districts with Census GEOIDs for geofence matching
5. Creates politician + office records for unmatched officials
6. Creates contact/address records from scraped data
7. Marks scraped officials as promoted

Requires: pip install psycopg2-binary
Usage: DATABASE_URL="postgresql://..." python3 promote_scraped_officials.py
  Or: automatically reads from ../.env.local
"""

import os
import sys
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from urllib.parse import urlparse, quote_plus

# Register UUID adapter
psycopg2.extras.register_uuid()

# === Config ===

# Known government IDs (from existing data)
US_GOV_ID = "0a6b51aa-00bb-4c15-b0f9-7f9da9150f47"
CA_GOV_ID = "e0f33bda-bfb5-4dd0-9816-576e6ce35fac"
LA_COUNTY_GOV_ID = "841f214e-7a8d-48fe-b67e-f6e2c6f8e1c4"  # Will verify at runtime

# Existing CA Senate chamber
CA_SENATE_EXT_ID = 11290

# Synthetic external_id counter (negative to avoid BallotReady collision)
EXT_ID_COUNTER = -100001

# Geo-ID generation helpers
CA_FIPS = "06"

# OCD-ID prefix
OCD_PREFIX = "ocd-division/country:us"

# District type to MTFCC mapping
DISTRICT_MTFCC = {
    "NATIONAL_LOWER": "G5200",
    "STATE_UPPER": "G5210",
    "STATE_LOWER": "G5220",
    "COUNTY": "G4020",
}

# Correct "possible" match: Lindsey Horvath
CORRECT_POSSIBLE_NAMES = ["Lindsey Horvath"]


def load_env():
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


def next_ext_id():
    global EXT_ID_COUNTER
    val = EXT_ID_COUNTER
    EXT_ID_COUNTER -= 1
    return val


def make_geo_id(district_type, district_number):
    """Convert district_type + number to Census GEOID."""
    if not district_number:
        return ""
    num = int(district_number)
    if district_type == "NATIONAL_LOWER":
        return f"{CA_FIPS}{num:02d}"
    elif district_type in ("STATE_UPPER", "STATE_LOWER"):
        return f"{CA_FIPS}{num:03d}"
    elif district_type == "COUNTY":
        return f"{CA_FIPS}037"  # LA County
    return ""


def make_ocd_id(district_type, district_number):
    """Generate OCD-ID from district type and number."""
    if district_type == "NATIONAL_EXEC":
        return f"{OCD_PREFIX}"
    elif district_type == "NATIONAL_UPPER":
        return f"{OCD_PREFIX}/state:ca"
    elif district_type == "NATIONAL_LOWER" and district_number:
        return f"{OCD_PREFIX}/state:ca/cd:{int(district_number)}"
    elif district_type == "STATE_EXEC":
        return f"{OCD_PREFIX}/state:ca"
    elif district_type == "STATE_UPPER" and district_number:
        return f"{OCD_PREFIX}/state:ca/sldu:{int(district_number)}"
    elif district_type == "STATE_LOWER" and district_number:
        return f"{OCD_PREFIX}/state:ca/sldl:{int(district_number)}"
    elif district_type == "COUNTY":
        return f"{OCD_PREFIX}/state:ca/county:los_angeles"
    return ""


def make_district_label(district_type, district_number, title=""):
    """Generate a human-readable district label."""
    if district_type == "NATIONAL_LOWER" and district_number:
        return f"Congressional District {int(district_number)}"
    elif district_type == "NATIONAL_UPPER":
        return "California"
    elif district_type == "STATE_UPPER" and district_number:
        return f"State Senate District {int(district_number)}"
    elif district_type == "STATE_LOWER" and district_number:
        return f"Assembly District {int(district_number)}"
    elif district_type == "STATE_EXEC":
        return title or "California"
    elif district_type == "COUNTY":
        return "Los Angeles County"
    return title or ""


def connect():
    """Connect with proper URL-encoding for passwords with special chars."""
    raw_url = os.getenv("DATABASE_URL")
    parsed = urlparse(raw_url)
    if parsed.password and '@' in parsed.password:
        # Re-encode password to handle special chars
        encoded_pw = quote_plus(parsed.password)
        port = parsed.port or 5432
        return psycopg2.connect(
            host=parsed.hostname,
            port=port,
            dbname=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password,  # raw password, not URL-encoded
            **dict(x.split('=', 1) for x in parsed.query.split('&') if '=' in x) if parsed.query else {}
        )
    return psycopg2.connect(raw_url)


def main():
    print("=" * 60)
    print("Promote Scraped Officials to Essentials Schema")
    print("=" * 60)

    load_env()
    conn = connect()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        run_migration(cur, conn)
        conn.commit()
        print("\nAll changes committed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        print("All changes rolled back.")
        raise
    finally:
        cur.close()
        conn.close()


def run_migration(cur, conn):
    # ================================================================
    # Phase 1: Fix incorrect "possible" matches
    # ================================================================
    print(f"\n{'=' * 60}")
    print("Phase 1: Fix incorrect 'possible' matches")
    print(f"{'=' * 60}")

    cur.execute("""
        UPDATE essentials.scraped_officials
        SET match_candidate_id = NULL, match_confidence = 'none'
        WHERE match_confidence = 'possible'
          AND full_name NOT IN %s
        RETURNING full_name, title
    """, (tuple(CORRECT_POSSIBLE_NAMES),))

    fixed = cur.fetchall()
    print(f"  Cleared {len(fixed)} wrong matches:")
    for row in fixed:
        print(f"    - {row['full_name']} ({row['title']})")

    # ================================================================
    # Phase 2: Ensure chambers and governments exist
    # ================================================================
    print(f"\n{'=' * 60}")
    print("Phase 2: Create/find chambers and governments")
    print(f"{'=' * 60}")

    # Verify LA County government ID
    cur.execute("""
        SELECT id FROM essentials.governments
        WHERE name ILIKE '%Los Angeles County%' AND state = 'CA'
        LIMIT 1
    """)
    row = cur.fetchone()
    la_county_gov_id = str(row['id']) if row else None

    if not la_county_gov_id:
        la_county_gov_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO essentials.governments (id, name, type, state)
            VALUES (%s, 'Los Angeles County, California, US', 'LOCAL', 'CA')
        """, (la_county_gov_id,))
        print(f"  Created LA County government: {la_county_gov_id}")
    else:
        print(f"  Found LA County government: {la_county_gov_id}")

    # Create chambers as needed
    chambers = {}

    # US Senate
    chambers['NATIONAL_UPPER'] = find_or_create_chamber(
        cur, "U.S. Senate", "United States Senate",
        US_GOV_ID, term_length="6 years", election_freq="6 years"
    )

    # US House
    chambers['NATIONAL_LOWER'] = find_or_create_chamber(
        cur, "U.S. House of Representatives", "United States House of Representatives",
        US_GOV_ID, term_length="2 years", election_freq="2 years"
    )

    # CA Senate (already exists)
    cur.execute("""
        SELECT id FROM essentials.chambers WHERE external_id = %s
    """, (CA_SENATE_EXT_ID,))
    row = cur.fetchone()
    if row:
        chambers['STATE_UPPER'] = str(row['id'])
        print(f"  Found CA Senate chamber: {chambers['STATE_UPPER']}")
    else:
        chambers['STATE_UPPER'] = find_or_create_chamber(
            cur, "Senate", "California Senate",
            CA_GOV_ID, term_length="4 years", election_freq="4 years"
        )

    # CA Assembly
    chambers['STATE_LOWER'] = find_or_create_chamber(
        cur, "Assembly", "California State Assembly",
        CA_GOV_ID, term_length="2 years", election_freq="2 years"
    )

    # CA State Executive (for BOE member)
    chambers['STATE_EXEC'] = find_or_create_chamber(
        cur, "Board of Equalization", "California Board of Equalization",
        CA_GOV_ID, term_length="4 years", election_freq="4 years"
    )

    # LA County officers
    chambers['COUNTY'] = find_or_create_chamber(
        cur, "County Officers", "Los Angeles County Officers",
        la_county_gov_id, term_length="4 years", election_freq="4 years"
    )

    print(f"\n  Chamber IDs:")
    for dtype, cid in chambers.items():
        print(f"    {dtype}: {cid}")

    # ================================================================
    # Phase 3: Update geo_ids on existing matched districts
    # ================================================================
    print(f"\n{'=' * 60}")
    print("Phase 3: Update geo_ids on existing matched districts")
    print(f"{'=' * 60}")

    # Get correctly matched scraped officials and their linked districts
    cur.execute("""
        SELECT so.id as scraped_id, so.full_name, so.district_type as scraped_dt,
               so.district_number, so.match_candidate_id,
               p.id as pol_id, d.id as dist_id, d.geo_id, d.mtfcc,
               d.district_type as db_dt, d.ocd_id
        FROM essentials.scraped_officials so
        JOIN essentials.politicians p ON so.match_candidate_id = p.id
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE so.match_confidence IN ('exact', 'likely')
           OR (so.match_confidence = 'possible' AND so.full_name IN %s)
    """, (tuple(CORRECT_POSSIBLE_NAMES),))

    matched = cur.fetchall()
    updated_districts = 0

    for row in matched:
        scraped_dt = row['scraped_dt']
        dist_num = row['district_number']
        dist_id = row['dist_id']
        current_geo_id = row['geo_id'] or ""
        current_mtfcc = row['mtfcc'] or ""
        db_dt = row['db_dt']

        # Only update geo_id for district-specific types where it's empty
        if scraped_dt in DISTRICT_MTFCC and not current_geo_id:
            new_geo_id = make_geo_id(scraped_dt, dist_num)
            new_mtfcc = DISTRICT_MTFCC.get(scraped_dt, "")
            new_ocd_id = make_ocd_id(scraped_dt, dist_num)

            if new_geo_id:
                # Only update if the DB district type matches what we expect
                if db_dt == scraped_dt:
                    cur.execute("""
                        UPDATE essentials.districts
                        SET geo_id = %s, mtfcc = %s, ocd_id = COALESCE(NULLIF(ocd_id, ''), %s)
                        WHERE id = %s AND (geo_id IS NULL OR geo_id = '')
                    """, (new_geo_id, new_mtfcc, new_ocd_id, dist_id))
                    if cur.rowcount > 0:
                        print(f"  Updated district for {row['full_name']}: geo_id={new_geo_id}, mtfcc={new_mtfcc}")
                        updated_districts += 1
                else:
                    print(f"  Skipped {row['full_name']}: scraped_dt={scraped_dt} != db_dt={db_dt}")

        # Mark as promoted
        cur.execute("""
            UPDATE essentials.scraped_officials
            SET status = 'promoted', promoted_to_id = %s
            WHERE id = %s
        """, (row['pol_id'], row['scraped_id']))

    print(f"  Updated {updated_districts} district geo_ids")
    print(f"  Marked {len(matched)} scraped officials as promoted (linked to existing)")

    # ================================================================
    # Phase 4: Create new officials for unmatched scraped records
    # ================================================================
    print(f"\n{'=' * 60}")
    print("Phase 4: Create new politicians for unmatched officials")
    print(f"{'=' * 60}")

    cur.execute("""
        SELECT id, full_name, first_name, last_name, title, body,
               district_label, district_number, district_type, level,
               party, phone, office_address, year_elected, term_length,
               state, county, source_url
        FROM essentials.scraped_officials
        WHERE match_confidence = 'none' AND status = 'pending'
        ORDER BY level, district_type, NULLIF(district_number, '')::int NULLS LAST, full_name
    """)

    unmatched = cur.fetchall()
    print(f"  Found {len(unmatched)} unmatched officials to create")

    created = 0
    for row in unmatched:
        scraped_id = row['id']
        dt = row['district_type'] or ""
        dist_num = row['district_number']
        name = row['full_name']

        # Determine chamber
        chamber_id = chambers.get(dt)
        if not chamber_id:
            print(f"  SKIP (no chamber): {name} — dt={dt}")
            continue

        # Create district
        district_id = str(uuid.uuid4())
        geo_id = make_geo_id(dt, dist_num)
        ocd_id = make_ocd_id(dt, dist_num)
        mtfcc = DISTRICT_MTFCC.get(dt, "")
        label = make_district_label(dt, dist_num, row['title'])
        dist_ext_id = next_ext_id()

        cur.execute("""
            INSERT INTO essentials.districts
                (id, external_id, ocd_id, label, district_type, district_id,
                 state, mtfcc, geo_id, num_officials, is_judicial,
                 has_unknown_boundaries, retention)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, false, false, false)
        """, (
            district_id, dist_ext_id, ocd_id, label, dt,
            str(int(dist_num)) if dist_num else "",
            "CA", mtfcc, geo_id,
        ))

        # Create politician
        politician_id = str(uuid.uuid4())
        pol_ext_id = next_ext_id()

        # Parse party short name
        party = row['party'] or ""
        party_short = ""
        if party.lower().startswith("democrat"):
            party_short = "D"
        elif party.lower().startswith("republican"):
            party_short = "R"

        cur.execute("""
            INSERT INTO essentials.politicians
                (id, external_id, first_name, last_name, full_name,
                 party, party_short_name, source, last_synced,
                 is_appointed, is_vacant, is_off_cycle)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'scraped', NOW(),
                    false, false, false)
        """, (
            politician_id, pol_ext_id,
            row['first_name'] or "", row['last_name'] or "",
            name, party, party_short,
        ))

        # Create office
        office_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO essentials.offices
                (id, politician_id, chamber_id, district_id, title,
                 representing_state, seats, is_appointed_position)
            VALUES (%s, %s, %s, %s, %s, 'CA', 1, false)
        """, (
            office_id, politician_id, chamber_id, district_id,
            row['title'] or "",
        ))

        # Create contact if we have phone or address
        if row['office_address'] or row['phone']:
            contact_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO essentials.politician_contacts
                    (id, politician_id, source, phone, contact_type)
                VALUES (%s, %s, 'scraped', %s, 'office')
            """, (contact_id, politician_id, row['phone']))

        # Mark scraped official as promoted
        cur.execute("""
            UPDATE essentials.scraped_officials
            SET status = 'promoted', promoted_to_id = %s
            WHERE id = %s
        """, (politician_id, scraped_id))

        created += 1
        print(f"  Created: {name} — {dt} {dist_num or ''} (geo_id={geo_id})")

    print(f"\n  Total created: {created}")

    # ================================================================
    # Phase 5: Verification
    # ================================================================
    print(f"\n{'=' * 60}")
    print("Phase 5: Verification")
    print(f"{'=' * 60}")

    # Count by status
    cur.execute("""
        SELECT status, COUNT(*) as count
        FROM essentials.scraped_officials
        GROUP BY status
        ORDER BY status
    """)
    print("\n  Scraped officials by status:")
    for row in cur.fetchall():
        print(f"    {row['status']}: {row['count']}")

    # Count new politicians by district type
    cur.execute("""
        SELECT d.district_type, COUNT(*) as count
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE p.source = 'scraped'
        GROUP BY d.district_type
        ORDER BY d.district_type
    """)
    print("\n  New scraped politicians by district type:")
    for row in cur.fetchall():
        print(f"    {row['district_type']}: {row['count']}")

    # Test geofence lookup for LA City Hall
    cur.execute("""
        SELECT p.full_name, d.district_type, d.geo_id, d.mtfcc, d.ocd_id
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE p.source = 'scraped'
          AND d.geo_id IN (
            SELECT gb.geo_id FROM essentials.geofence_boundaries gb
            WHERE ST_Contains(
                gb.geometry,
                ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
            )
          )
          AND d.district_type = ANY(
            CASE d.mtfcc
                WHEN 'G5200' THEN ARRAY['NATIONAL_LOWER']
                WHEN 'G5210' THEN ARRAY['STATE_UPPER']
                WHEN 'G5220' THEN ARRAY['STATE_LOWER']
                WHEN 'G4020' THEN ARRAY['COUNTY', 'JUDICIAL']
                ELSE ARRAY[d.district_type]
            END
          )
        ORDER BY d.district_type, p.full_name
    """)

    results = cur.fetchall()
    print(f"\n  Geofence test (LA City Hall — 34.0537, -118.2427):")
    print(f"  Officials found via geofence matching: {len(results)}")
    for row in results:
        print(f"    {row['district_type']:20s} | {row['full_name']:30s} | geo={row['geo_id']} | {row['ocd_id']}")

    # Check statewide supplemental would also return
    cur.execute("""
        SELECT p.full_name, d.district_type
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        JOIN essentials.chambers c ON o.chamber_id = c.id
        JOIN essentials.governments g ON c.government_id = g.id
        WHERE p.source = 'scraped'
          AND d.district_type IN ('NATIONAL_UPPER', 'STATE_EXEC')
          AND (o.representing_state = 'CA' OR d.state = 'CA')
        ORDER BY d.district_type, p.full_name
    """)
    statewide = cur.fetchall()
    print(f"\n  Statewide officials (supplemental fetch): {len(statewide)}")
    for row in statewide:
        print(f"    {row['district_type']:20s} | {row['full_name']}")


def find_or_create_chamber(cur, name, name_formal, gov_id, term_length="", election_freq=""):
    """Find existing chamber by name+government or create new one."""
    cur.execute("""
        SELECT id FROM essentials.chambers
        WHERE name = %s AND government_id = %s
        LIMIT 1
    """, (name, gov_id))
    row = cur.fetchone()
    if row:
        print(f"  Found chamber '{name}': {row['id']}")
        return str(row['id'])

    # Also check name_formal
    cur.execute("""
        SELECT id FROM essentials.chambers
        WHERE name_formal = %s AND government_id = %s
        LIMIT 1
    """, (name_formal, gov_id))
    row = cur.fetchone()
    if row:
        print(f"  Found chamber '{name_formal}': {row['id']}")
        return str(row['id'])

    chamber_id = str(uuid.uuid4())
    ext_id = next_ext_id()
    cur.execute("""
        INSERT INTO essentials.chambers
            (id, external_id, government_id, name, name_formal,
             term_length, election_frequency)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (chamber_id, ext_id, gov_id, name, name_formal, term_length, election_freq))
    print(f"  Created chamber '{name}': {chamber_id}")
    return chamber_id


if __name__ == "__main__":
    main()
