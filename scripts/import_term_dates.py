#!/usr/bin/env python3
"""
Import researched term dates for LA County supervisors.

Sets valid_from, valid_to, and term_date_precision='year' for all 5
LA County Board of Supervisors members. Term dates are year-precision
only (no month or day) — the term_date_precision field signals this
so the frontend displays "2024" not "Jan 2024".

NOTE on TERM-02 scope:
City council election years are NOT available in city_sources.json
(no election_year field). Election years are known only for supervisors
(not city council members without per-city research). TERM-02 is satisfied
by supervisor data in this phase. City council term dates require per-city
election year research — deferred to future phase.

Usage:
    cd EV-Backend/scripts
    python3 import_term_dates.py            # import all 5 supervisors
    python3 import_term_dates.py --dry-run  # print SQL but don't commit

Requirements:
    DATABASE_URL in EV-Backend/.env.local
"""

import os
import sys
import argparse
from pathlib import Path
from urllib.parse import urlparse

import psycopg2

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env


def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Handles passwords with special characters by parsing the URL and
    connecting with keyword arguments. Consistent with scrape_headshots.py.
    """
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set. Call load_env() first.")
        sys.exit(1)

    parsed = urlparse(raw_url)
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }
    # Pass through extra query params (e.g., sslmode=require for Supabase)
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v

    return psycopg2.connect(**kwargs)

# ============================================================
# Researched supervisor term dates (verified 2026-02-25)
# Source: LA County Board of Supervisors official records
# All supervisors have 4-year elected terms.
# ============================================================
SUPERVISOR_TERMS = [
    {"name": "Hilda L. Solis",     "valid_from": "2022", "valid_to": "2026"},
    {"name": "Holly J. Mitchell",  "valid_from": "2020", "valid_to": "2028"},
    {"name": "Lindsey P. Horvath", "valid_from": "2022", "valid_to": "2026"},
    {"name": "Janice Hahn",        "valid_from": "2024", "valid_to": "2028"},
    {"name": "Kathryn Barger",     "valid_from": "2024", "valid_to": "2028"},
]

UPDATE_SQL = """
UPDATE essentials.politicians
SET valid_from = %s, valid_to = %s, term_date_precision = 'year'
WHERE full_name ILIKE %s AND is_active = true
"""


def main():
    parser = argparse.ArgumentParser(
        description="Import LA County supervisor term dates into essentials.politicians"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL but do not commit changes to the database",
    )
    args = parser.parse_args()

    load_env()

    conn = get_connection()
    cur = conn.cursor()

    updated_count = 0

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Importing supervisor term dates...")
    print()

    for sup in SUPERVISOR_TERMS:
        name_pattern = f"%{sup['name']}%"
        if args.dry_run:
            print(f"  [DRY RUN] Would UPDATE: {sup['name']}")
            print(f"    SET valid_from='{sup['valid_from']}', valid_to='{sup['valid_to']}', term_date_precision='year'")
            print(f"    WHERE full_name ILIKE '{name_pattern}' AND is_active = true")
            print()
            updated_count += 1
        else:
            cur.execute(UPDATE_SQL, (sup["valid_from"], sup["valid_to"], name_pattern))
            rowcount = cur.rowcount
            if rowcount == 0:
                print(f"  WARNING: No matching politician found for '{sup['name']}' (is_active=true)")
            elif rowcount > 1:
                print(f"  WARNING: {rowcount} rows updated for '{sup['name']}' — expected 1")
                updated_count += 1
            else:
                print(f"  OK: {sup['name']} -> {sup['valid_from']}–{sup['valid_to']} (precision=year)")
                updated_count += 1

    if not args.dry_run:
        conn.commit()
        print()
        print(f"Updated {updated_count}/{len(SUPERVISOR_TERMS)} supervisors with term dates")
        print()
        print("Note: City council term dates require per-city election year research — deferred to future phase.")
    else:
        conn.rollback()
        print(f"[DRY RUN] Would update {updated_count}/{len(SUPERVISOR_TERMS)} supervisors")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
