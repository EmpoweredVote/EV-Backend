#!/usr/bin/env python3
"""
Generate a research manifest CSV of all LA County city council politicians
who are still missing headshots, grouped by city with council page URLs.

This script supports the manual headshot curation sprint (Plan 06).
It converts the abstract "N politicians missing headshots" gap into a
concrete, city-by-city research checklist.

Usage:
    cd EV-Backend/scripts
    python3 generate_headshot_manifest.py
    python3 generate_headshot_manifest.py --output my_manifest.csv
    python3 generate_headshot_manifest.py --include-blocked

Output:
    headshot_research_manifest.csv — politicians missing headshots, one row per person
    stdout — summary statistics (total missing, breakdown by status, top cities)

Columns in CSV:
    city_name          — human-readable city name from city_sources.json
    city_id            — city identifier (e.g., "burbank_city_council")
    council_url        — city's council page URL for manual browser research
    member_name        — politician's full_name from the database
    role               — Mayor or Council Member
    has_existing_override — True if city_sources.json already has a headshot_url
    headshot_status    — city's current headshot_status (scraped/blocked/failed/null)

Requirements:
    - DATABASE_URL in EV-Backend/.env.local
    - city_sources.json in same directory
    - essentials.politician_images table with photo_license column (Phase 39)
    - essentials schema: politicians, offices, districts tables
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env


# ============================================================
# DB connection (same pattern as scrape_city_headshots.py)
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Uses direct connection (port 5432), NOT the pooler (port 6543).
    Handles passwords with special characters via urlparse keyword arguments.

    Returns:
        psycopg2.connection: Open database connection.
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
# Query: politicians missing Supabase headshots
# ============================================================

MISSING_HEADSHOTS_SQL = """
SELECT DISTINCT ON (p.id)
    p.id,
    p.full_name,
    d.district_type,
    o.title AS office_title
FROM essentials.politicians p
JOIN essentials.offices o ON o.politician_id = p.id
JOIN essentials.districts d ON o.district_id = d.id
LEFT JOIN essentials.politician_images pi
    ON pi.politician_id = p.id
    AND pi.type = 'default'
    AND pi.url LIKE '%%supabase%%'
WHERE d.district_type IN ('LOCAL', 'LOCAL_EXEC')
  AND d.state = 'CA'
  AND p.is_active = true
  AND pi.id IS NULL
ORDER BY p.id, p.full_name
"""


def fetch_missing_politicians(conn):
    """Query politicians missing Supabase headshots.

    Finds LOCAL/LOCAL_EXEC CA politicians who do NOT have a 'default' type
    row in politician_images with a Supabase CDN URL.

    Args:
        conn: psycopg2 connection

    Returns:
        list of dicts: {id, full_name, district_type, office_title}
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(MISSING_HEADSHOTS_SQL)
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


# ============================================================
# City metadata from city_sources.json
# ============================================================

def load_city_sources():
    """Load city_sources.json from scripts directory.

    Returns:
        dict: {city_id -> city_config} mapping
    """
    config_path = Path(__file__).parent / "city_sources.json"
    if not config_path.exists():
        print(f"Error: city_sources.json not found at {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    return {city["id"]: city for city in config.get("cities", [])}


# ============================================================
# Name matching: politician -> city
# ============================================================

def build_name_to_city_map(city_map):
    """Build a lookup from lowercase member name to city config.

    Cross-references roster entries from city_sources.json so that each
    missing politician can be assigned to the correct city.

    Args:
        city_map: {city_id -> city_config} from load_city_sources()

    Returns:
        dict: {lowercase_name -> city_config}

    Note:
        If the same name appears in multiple cities, only the first city
        is kept (rare edge case). The 'has_existing_override' and 'role'
        are also captured per name.
    """
    name_map = {}
    for city_id, city_config in city_map.items():
        roster = city_config.get("roster", [])
        for member in roster:
            name = member.get("name", "").strip()
            if not name:
                continue
            key = name.lower()
            if key not in name_map:
                name_map[key] = {
                    "city_config": city_config,
                    "role": member.get("role", "Council Member"),
                    "has_existing_override": bool(member.get("headshot_url", "").strip()),
                }
    return name_map


# ============================================================
# Main manifest generation
# ============================================================

def generate_manifest(include_blocked=False, output_path="headshot_research_manifest.csv"):
    """Generate the headshot research manifest CSV.

    Steps:
    1. Load city_sources.json for city metadata and roster entries
    2. Query DB for politicians without Supabase headshots
    3. Match each politician to a city via roster name lookup
    4. Group by city, sort by gap size descending (most missing first)
    5. Write CSV and print summary

    Args:
        include_blocked: If True, include cities with headshot_status="blocked"
                         (Cloudflare-protected cities are harder to research manually)
        output_path: Path to write CSV output

    Returns:
        int: Total number of politicians in the manifest
    """
    # Step 1: Load city metadata
    print("Loading city_sources.json...")
    city_map = load_city_sources()
    name_to_city = build_name_to_city_map(city_map)
    print(f"  Loaded {len(city_map)} cities, {len(name_to_city)} unique roster names")

    # Step 2: Fetch missing politicians from DB
    print("\nConnecting to database...")
    load_env()
    conn = get_connection()
    print("  Connected. Querying missing politicians...")

    missing = fetch_missing_politicians(conn)
    conn.close()
    print(f"  Found {len(missing)} LOCAL/LOCAL_EXEC CA politicians without Supabase headshots")

    # Step 3: Match politicians to cities
    matched = []
    unmatched = []

    for pol in missing:
        name_key = pol["full_name"].lower()
        city_info = name_to_city.get(name_key)

        if city_info is None:
            # Try partial match: check if any roster name is a substring match
            for roster_name_key, info in name_to_city.items():
                # Split roster name and check if last name matches
                roster_parts = roster_name_key.split()
                pol_parts = name_key.split()
                if roster_parts and pol_parts and roster_parts[-1] == pol_parts[-1]:
                    city_info = info
                    break

        if city_info is not None:
            city_config = city_info["city_config"]
            headshot_status = city_config.get("headshot_status")

            # Filter blocked cities if requested
            if not include_blocked and headshot_status == "blocked":
                continue

            matched.append({
                "city_name": city_config["name"],
                "city_id": city_config["id"],
                "council_url": city_config.get("url", ""),
                "member_name": pol["full_name"],
                "role": city_info["role"],
                "has_existing_override": city_info["has_existing_override"],
                "headshot_status": headshot_status or "pending",
            })
        else:
            unmatched.append(pol["full_name"])

    # Step 4: Group by city and sort by gap size descending
    from collections import defaultdict
    city_groups = defaultdict(list)
    for row in matched:
        city_groups[row["city_id"]].append(row)

    # Sort cities by number of missing members (largest gap first)
    sorted_cities = sorted(
        city_groups.items(),
        key=lambda kv: len(kv[1]),
        reverse=True,
    )

    # Flatten into ordered list
    ordered_rows = []
    for city_id, rows in sorted_cities:
        # Sort within city: Mayor first, then alphabetically
        rows.sort(key=lambda r: (0 if r["role"] == "Mayor" else 1, r["member_name"]))
        ordered_rows.extend(rows)

    # Step 5: Write CSV
    fieldnames = [
        "city_name",
        "city_id",
        "council_url",
        "member_name",
        "role",
        "has_existing_override",
        "headshot_status",
    ]

    output_file = Path(__file__).parent / output_path
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ordered_rows)

    # ============================================================
    # Summary statistics
    # ============================================================
    print("\n" + "=" * 60)
    print("HEADSHOT RESEARCH MANIFEST SUMMARY")
    print("=" * 60)

    total_missing = len(missing)
    total_matched = len(ordered_rows)
    total_unmatched = len(unmatched)

    print(f"\nTotal politicians missing headshots (LOCAL/LOCAL_EXEC CA): {total_missing}")
    print(f"  Matched to city roster: {total_matched}")
    print(f"  Unmatched (not in any roster): {total_unmatched}")
    if not include_blocked:
        print(f"  Excluded (blocked cities): {total_missing - total_matched - total_unmatched}")

    # Breakdown by headshot_status
    from collections import Counter
    status_counts = Counter(row["headshot_status"] for row in ordered_rows)
    print("\nBreakdown by city status:")
    for status, count in sorted(status_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {status:20s} {count:4d} politicians")

    # Count cities with overrides available
    override_count = sum(1 for r in ordered_rows if r["has_existing_override"])
    print(f"\nPoliticians with existing headshot_url override: {override_count}")
    print(f"Politicians needing new URL research:           {total_matched - override_count}")

    # Top 20 cities by gap size
    print("\nTop 20 cities by gap size (most missing headshots):")
    print(f"  {'City':<35} {'Missing':>7}  {'Status':<10}")
    print(f"  {'-'*35} {'-'*7}  {'-'*10}")
    for i, (city_id, rows) in enumerate(sorted_cities[:20]):
        city_name = rows[0]["city_name"]
        status = rows[0]["headshot_status"]
        print(f"  {city_name:<35} {len(rows):>7}  {status:<10}")

    # Effort estimate
    research_cities = len([c for c, rows in sorted_cities
                           if any(not r["has_existing_override"] for r in rows)])
    override_cities = len([c for c, rows in sorted_cities
                           if all(r["has_existing_override"] for r in rows)])
    print(f"\nEstimated effort:")
    print(f"  {research_cities} cities with members needing new URL research")
    print(f"  {override_cities} cities where all members have existing override URLs")
    print(f"  Total: {total_matched} roster entries to process across {len(sorted_cities)} cities")

    # Coverage context
    total_local_sql = """
        SELECT COUNT(DISTINCT p.id)
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE d.district_type IN ('LOCAL', 'LOCAL_EXEC')
          AND d.state = 'CA'
          AND p.is_active = true
    """

    print(f"\nManifest written to: {output_file}")
    print(f"  Rows: {len(ordered_rows)}")
    print(f"  Columns: {', '.join(fieldnames)}")

    if unmatched:
        print(f"\nUnmatched politicians (not in city_sources.json roster):")
        for name in unmatched[:20]:
            print(f"  {name}")
        if len(unmatched) > 20:
            print(f"  ... and {len(unmatched) - 20} more")

    return len(ordered_rows)


# ============================================================
# CLI entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate headshot research manifest for manual curation sprint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 generate_headshot_manifest.py
  python3 generate_headshot_manifest.py --output custom_output.csv
  python3 generate_headshot_manifest.py --include-blocked

Output CSV columns:
  city_name          City name from city_sources.json
  city_id            City identifier (e.g., burbank_city_council)
  council_url        City council page URL for manual research
  member_name        Politician full name from database
  role               Mayor or Council Member
  has_existing_override  True if headshot_url already set in roster
  headshot_status    City scrape status (scraped/blocked/failed/pending)
        """,
    )
    parser.add_argument(
        "--output",
        default="headshot_research_manifest.csv",
        metavar="FILE",
        help="Output CSV file path (default: headshot_research_manifest.csv)",
    )
    parser.add_argument(
        "--include-blocked",
        action="store_true",
        help="Include Cloudflare-blocked cities (excluded by default — harder to research)",
    )
    args = parser.parse_args()

    total = generate_manifest(
        include_blocked=args.include_blocked,
        output_path=args.output,
    )

    print(f"\nDone. {total} politicians listed in manifest.")


if __name__ == "__main__":
    main()
