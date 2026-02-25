#!/usr/bin/env python3
"""
Import city website URLs and supervisor phone numbers into politician_contacts.

Section A: City website URLs for all 89 LA County cities.
  - Reads city_sources.json for city roster names.
  - Matches politicians by name (the scraped city council members have office_id=null
    so OCD-ID joining doesn't work; name-based matching is the correct approach).
  - Upserts a contact row with contact_type='city_website', source='scraped'.

Section B: Supervisor phone numbers for 5 LA County Board of Supervisors.
  - Hardcoded phone numbers from bos.lacounty.gov (verified 2026-02-25).
  - Upserts phone contact and office website contact for each supervisor.

Schema safety: Adds website_url column via ALTER TABLE IF NOT EXISTS before any writes.

Usage:
    cd EV-Backend/scripts
    python3 import_city_contacts.py           # Live run
    python3 import_city_contacts.py --dry-run  # Preview without DB writes

Idempotency: Re-running this script updates existing contacts instead of creating
duplicates. Uses SELECT + INSERT/UPDATE pattern.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, quote_plus, urlunparse

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env

import psycopg2
import psycopg2.extras

psycopg2.extras.register_uuid()

# ============================================================
# Hardcoded supervisor phone numbers (verified from bos.lacounty.gov)
# ============================================================

SUPERVISOR_PHONES = {
    "Hilda L. Solis":     "213-974-4111",
    "Holly J. Mitchell":  "213-974-2222",
    "Lindsey P. Horvath": "213-974-3333",
    "Janice Hahn":        "213-974-4444",
    "Kathryn Barger":     "213-974-5555",
}

BOS_WEBSITE = "https://bos.lacounty.gov"


# ============================================================
# Connection helper
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Parses the URL by component to handle passwords with special characters
    (e.g., '@') that would break raw URL passing.
    """
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    parsed = urlparse(raw_url)
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v
    return psycopg2.connect(**kwargs)


# ============================================================
# Schema safety: ensure website_url column exists
# ============================================================

def ensure_website_url_column(cur):
    """Add website_url column to politician_contacts if it doesn't exist.

    The GORM AutoMigrate adds this column on server start, but the Python
    import script runs independently so we ensure it exists here.
    """
    cur.execute("""
        ALTER TABLE essentials.politician_contacts
        ADD COLUMN IF NOT EXISTS website_url TEXT DEFAULT ''
    """)
    print("  Schema: website_url column ensured on politician_contacts")


# ============================================================
# Idempotent upsert helpers
# ============================================================

def upsert_website_contact(cur, politician_id, website_url, dry_run=False):
    """Upsert a city_website contact for a politician.

    Returns 'created', 'updated', or 'skipped' (dry_run).
    """
    if dry_run:
        return "skipped"

    cur.execute("""
        SELECT id, website_url FROM essentials.politician_contacts
        WHERE politician_id = %s
          AND source = 'scraped'
          AND contact_type = 'city_website'
        LIMIT 1
    """, (politician_id,))
    existing = cur.fetchone()

    if existing:
        if existing["website_url"] == website_url:
            return "unchanged"
        cur.execute("""
            UPDATE essentials.politician_contacts
            SET website_url = %s
            WHERE id = %s
        """, (website_url, existing["id"]))
        return "updated"
    else:
        cur.execute("""
            INSERT INTO essentials.politician_contacts
                (politician_id, source, contact_type, website_url, email, phone, fax)
            VALUES (%s, 'scraped', 'city_website', %s, '', '', '')
        """, (politician_id, website_url))
        return "created"


def upsert_phone_contact(cur, politician_id, phone, dry_run=False):
    """Upsert a district phone contact for a supervisor.

    Returns 'created', 'updated', or 'skipped' (dry_run).
    """
    if dry_run:
        return "skipped"

    cur.execute("""
        SELECT id, phone FROM essentials.politician_contacts
        WHERE politician_id = %s
          AND source = 'scraped'
          AND contact_type = 'district'
        LIMIT 1
    """, (politician_id,))
    existing = cur.fetchone()

    if existing:
        if existing["phone"] == phone:
            return "unchanged"
        cur.execute("""
            UPDATE essentials.politician_contacts
            SET phone = %s
            WHERE id = %s
        """, (phone, existing["id"]))
        return "updated"
    else:
        cur.execute("""
            INSERT INTO essentials.politician_contacts
                (politician_id, source, contact_type, phone, website_url, email, fax)
            VALUES (%s, 'scraped', 'district', %s, '', '', '')
        """, (politician_id, phone))
        return "created"


def upsert_office_website_contact(cur, politician_id, website_url, dry_run=False):
    """Upsert an office_website contact (used for supervisors' BOS website).

    Returns 'created', 'updated', or 'skipped' (dry_run).
    """
    if dry_run:
        return "skipped"

    cur.execute("""
        SELECT id, website_url FROM essentials.politician_contacts
        WHERE politician_id = %s
          AND source = 'scraped'
          AND contact_type = 'office_website'
        LIMIT 1
    """, (politician_id,))
    existing = cur.fetchone()

    if existing:
        if existing["website_url"] == website_url:
            return "unchanged"
        cur.execute("""
            UPDATE essentials.politician_contacts
            SET website_url = %s
            WHERE id = %s
        """, (website_url, existing["id"]))
        return "updated"
    else:
        cur.execute("""
            INSERT INTO essentials.politician_contacts
                (politician_id, source, contact_type, website_url, email, phone, fax)
            VALUES (%s, 'scraped', 'office_website', %s, '', '', '')
        """, (politician_id, website_url))
        return "created"


# ============================================================
# Section A: City website URLs
# ============================================================

def get_city_website(city_config):
    """Extract base website URL from city council URL in city_sources.json."""
    url = city_config.get("url", "")
    if not url:
        return None
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def find_politician_by_name(cur, name):
    """Find a politician ID by exact name match (case-insensitive).

    The city council members from the CA state roster PDF have office_id=null,
    so we cannot use OCD-ID joins. Name-based matching is the correct approach
    for these scraped politicians.

    Returns politician_id (uuid) or None if not found.
    """
    cur.execute("""
        SELECT id FROM essentials.politicians
        WHERE full_name ILIKE %s
          AND is_active = true
        LIMIT 1
    """, (name,))
    row = cur.fetchone()
    return row["id"] if row else None


def import_city_websites(cur, city_sources_path, dry_run=False):
    """Import city website URLs for all cities in city_sources.json.

    Uses name-based matching to find politicians since scraped city council
    members have office_id=null (cannot use OCD-ID district joins).

    Returns summary dict: {cities_processed, politicians_found, contacts_created,
                           contacts_updated, contacts_unchanged, politicians_not_found}
    """
    with open(city_sources_path) as f:
        data = json.load(f)

    cities = data["cities"]
    print(f"\nSection A: City website URLs for {len(cities)} cities")

    stats = {
        "cities_processed": 0,
        "politicians_found": 0,
        "politicians_not_found": 0,
        "contacts_created": 0,
        "contacts_updated": 0,
        "contacts_unchanged": 0,
    }
    not_found_names = []

    for city in cities:
        city_name = city["name"]
        website_url = get_city_website(city)
        if not website_url:
            print(f"  SKIP {city_name}: no URL in city_sources.json")
            continue

        roster = city.get("roster", [])
        if not roster:
            print(f"  SKIP {city_name}: no roster members")
            continue

        city_found = 0
        city_not_found = 0

        for member in roster:
            member_name = member["name"]
            politician_id = find_politician_by_name(cur, member_name)

            if politician_id is None:
                city_not_found += 1
                stats["politicians_not_found"] += 1
                not_found_names.append(f"{city_name}: {member_name}")
                continue

            stats["politicians_found"] += 1
            city_found += 1

            result = upsert_website_contact(cur, politician_id, website_url, dry_run)
            if result == "created":
                stats["contacts_created"] += 1
            elif result == "updated":
                stats["contacts_updated"] += 1
            elif result == "unchanged":
                stats["contacts_unchanged"] += 1

        stats["cities_processed"] += 1
        if dry_run:
            print(f"  [DRY RUN] {city_name}: {city_found} found, {city_not_found} not found | {website_url}")
        else:
            print(f"  {city_name}: {city_found} politicians matched | {website_url}")

    # Also add LA City (not in city_sources.json)
    la_city_website = "https://lacity.gov"
    print(f"\n  LA City (hardcoded): {la_city_website}")
    la_politicians = import_la_city_website(cur, la_city_website, dry_run)
    stats["politicians_found"] += la_politicians["found"]
    stats["politicians_not_found"] += la_politicians["not_found"]
    stats["contacts_created"] += la_politicians["created"]
    stats["contacts_updated"] += la_politicians["updated"]
    stats["contacts_unchanged"] += la_politicians["unchanged"]

    if not_found_names and not dry_run:
        print(f"\n  Politicians not found ({len(not_found_names)}):")
        for name in not_found_names[:20]:
            print(f"    - {name}")
        if len(not_found_names) > 20:
            print(f"    ... and {len(not_found_names) - 20} more")

    return stats


def import_la_city_website(cur, website_url, dry_run=False):
    """Add city_website contact for LA City council members.

    LA City is not in city_sources.json (it was added separately via scrape_la_officials.py).
    Find LA City politicians via OCD-ID prefix (they DO have office_id linked).
    """
    cur.execute("""
        SELECT p.id, p.full_name
        FROM essentials.politicians p
        JOIN essentials.offices o ON p.office_id = o.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:los_angeles%'
          AND p.is_active = true
    """)
    rows = cur.fetchall()

    stats = {"found": 0, "not_found": 0, "created": 0, "updated": 0, "unchanged": 0}

    for row in rows:
        stats["found"] += 1
        result = upsert_website_contact(cur, row["id"], website_url, dry_run)
        if result == "created":
            stats["created"] += 1
        elif result == "updated":
            stats["updated"] += 1
        elif result == "unchanged":
            stats["unchanged"] += 1

    if dry_run:
        print(f"    [DRY RUN] LA City: {stats['found']} politicians found")
    else:
        print(f"    LA City: {stats['found']} politicians matched")

    return stats


# ============================================================
# Section B: Supervisor phone numbers
# ============================================================

def import_supervisor_contacts(cur, dry_run=False):
    """Import phone numbers and office website for LA County supervisors.

    Hardcoded from bos.lacounty.gov (verified 2026-02-25).
    Each supervisor gets:
    - contact_type='district', phone={district_number}, source='scraped'
    - contact_type='office_website', website_url='https://bos.lacounty.gov', source='scraped'

    Returns summary dict.
    """
    print(f"\nSection B: Supervisor phone numbers and office websites")

    stats = {
        "supervisors_found": 0,
        "supervisors_not_found": 0,
        "phone_contacts_created": 0,
        "phone_contacts_updated": 0,
        "website_contacts_created": 0,
        "website_contacts_updated": 0,
    }
    not_found = []

    for name, phone in SUPERVISOR_PHONES.items():
        cur.execute("""
            SELECT id FROM essentials.politicians
            WHERE full_name ILIKE %s
              AND is_active = true
            LIMIT 1
        """, (name,))
        row = cur.fetchone()

        if row is None:
            stats["supervisors_not_found"] += 1
            not_found.append(name)
            print(f"  WARN: Supervisor not found: {name}")
            continue

        politician_id = row["id"]
        stats["supervisors_found"] += 1

        # Phone contact
        phone_result = upsert_phone_contact(cur, politician_id, phone, dry_run)
        if phone_result == "created":
            stats["phone_contacts_created"] += 1
        elif phone_result == "updated":
            stats["phone_contacts_updated"] += 1

        # Office website contact
        web_result = upsert_office_website_contact(cur, politician_id, BOS_WEBSITE, dry_run)
        if web_result == "created":
            stats["website_contacts_created"] += 1
        elif web_result == "updated":
            stats["website_contacts_updated"] += 1

        if dry_run:
            print(f"  [DRY RUN] {name}: phone={phone} | website={BOS_WEBSITE}")
        else:
            print(f"  {name}: phone={phone} | website={BOS_WEBSITE}")

    return stats


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Import city website URLs and supervisor phones into politician_contacts"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be written without making DB changes",
    )
    args = parser.parse_args()

    load_env()

    if args.dry_run:
        print("=== DRY RUN MODE (no DB writes) ===\n")

    city_sources_path = Path(__file__).parent / "city_sources.json"
    if not city_sources_path.exists():
        print(f"Error: city_sources.json not found at {city_sources_path}")
        sys.exit(1)

    print("Connecting to database...")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Schema safety: ensure website_url column exists
        if not args.dry_run:
            ensure_website_url_column(cur)

        # Section A: City website URLs
        city_stats = import_city_websites(cur, city_sources_path, args.dry_run)

        # Section B: Supervisor phones
        sup_stats = import_supervisor_contacts(cur, args.dry_run)

        # Commit all changes in one transaction
        if not args.dry_run:
            conn.commit()
            print("\n  Transaction committed successfully.")

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Section A — City website contacts:")
        print(f"  Cities processed:      {city_stats['cities_processed']}")
        print(f"  Politicians matched:   {city_stats['politicians_found']}")
        print(f"  Politicians not found: {city_stats['politicians_not_found']}")
        print(f"  Contacts created:      {city_stats['contacts_created']}")
        print(f"  Contacts updated:      {city_stats['contacts_updated']}")
        print(f"  Contacts unchanged:    {city_stats['contacts_unchanged']}")
        print()
        print(f"Section B — Supervisor contacts:")
        print(f"  Supervisors found:         {sup_stats['supervisors_found']}")
        print(f"  Supervisors not found:     {sup_stats['supervisors_not_found']}")
        print(f"  Phone contacts created:    {sup_stats['phone_contacts_created']}")
        print(f"  Phone contacts updated:    {sup_stats['phone_contacts_updated']}")
        print(f"  Website contacts created:  {sup_stats['website_contacts_created']}")
        print(f"  Website contacts updated:  {sup_stats['website_contacts_updated']}")

        if args.dry_run:
            print("\nNo DB changes made (dry run).")
        else:
            total_created = (
                city_stats["contacts_created"]
                + sup_stats["phone_contacts_created"]
                + sup_stats["website_contacts_created"]
            )
            total_updated = (
                city_stats["contacts_updated"]
                + sup_stats["phone_contacts_updated"]
                + sup_stats["website_contacts_updated"]
            )
            print(f"\nTotal contacts created: {total_created}")
            print(f"Total contacts updated: {total_updated}")

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
