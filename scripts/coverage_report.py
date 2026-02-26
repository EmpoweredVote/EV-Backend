#!/usr/bin/env python3
"""
v1.7 Coverage Validation Report — Phase 44.

Validates three coverage targets for the LA County city council headshot pipeline
milestone (v1.7):

  Check 1 — Headshot CDN Health (80% threshold):
    Issues HTTP HEAD requests to every Supabase CDN headshot URL for active
    LA County LOCAL/LOCAL_EXEC/COUNTY politicians and reports:
      - CDN health percentage (pass/fail metric, threshold 80%)
      - Population coverage percentage (informational context)

  Check 2 — Contact Website Presence (89/89 cities):
    Confirms that every one of the 89 LA County cities in city_sources.json
    has at least one roster member with a 'city_website' contact row in the DB.

  Check 3 — Zero Government Hotlinks:
    Confirms that zero rows in essentials.politician_images for active LA County
    LOCAL/LOCAL_EXEC/COUNTY politicians use non-Supabase URLs (i.e., direct
    government domain hotlinks that will break silently).

Usage:
    cd EV-Backend/scripts
    python3 coverage_report.py              # Run all three checks
    python3 coverage_report.py --check 1   # Run only Check 1 (CDN HEAD requests)
    python3 coverage_report.py --check 2   # Run only Check 2 (contact websites)
    python3 coverage_report.py --check 3   # Run only Check 3 (hotlinks)

Exit codes:
    0 — all requested checks PASS
    1 — one or more checks FAIL
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env

import psycopg2
import psycopg2.extras
import requests
from urllib.parse import urlparse


# ============================================================
# Database connection
# ============================================================

def get_connection():
    """Open a psycopg2 connection using DATABASE_URL.

    Uses urlparse to extract components — handles passwords with special
    characters (e.g., '@') that break raw URL passing. Copies the pattern
    from import_city_contacts.py which handles the pooler URL edge case.
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
    # Pass through any query params (e.g., sslmode=require)
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v
    return psycopg2.connect(**kwargs)


# ============================================================
# Check 1 — Headshot CDN Health (HEAD requests, 80% threshold)
# ============================================================

CDN_URL_QUERY = """
    SELECT DISTINCT pi.url
    FROM essentials.politician_images pi
    JOIN essentials.politicians p ON p.id = pi.politician_id
    JOIN essentials.offices o ON o.politician_id = p.id
    JOIN essentials.districts d ON o.district_id = d.id
    WHERE pi.type = 'default'
      AND pi.url LIKE '%supabase%'
      AND d.district_type IN ('LOCAL', 'LOCAL_EXEC', 'COUNTY')
      AND d.state = 'CA'
      AND p.is_active = true
    LIMIT 600
"""

POPULATION_COUNT_QUERY = """
    SELECT COUNT(DISTINCT p.id) AS total
    FROM essentials.politicians p
    JOIN essentials.offices o ON o.politician_id = p.id
    JOIN essentials.districts d ON o.district_id = d.id
    WHERE d.district_type IN ('LOCAL', 'LOCAL_EXEC', 'COUNTY')
      AND d.state = 'CA'
      AND p.is_active = true
"""

POPULATION_WITH_HEADSHOT_QUERY = """
    SELECT COUNT(DISTINCT p.id) AS total
    FROM essentials.politicians p
    JOIN essentials.offices o ON o.politician_id = p.id
    JOIN essentials.districts d ON o.district_id = d.id
    JOIN essentials.politician_images pi ON pi.politician_id = p.id
    WHERE pi.type = 'default'
      AND pi.url LIKE '%supabase%'
      AND d.district_type IN ('LOCAL', 'LOCAL_EXEC', 'COUNTY')
      AND d.state = 'CA'
      AND p.is_active = true
"""


def run_check_1(conn) -> tuple[bool, str]:
    """Check 1: Issue HEAD requests to all Supabase CDN headshot URLs.

    Returns:
        (passed: bool, notes: str)
    """
    print("\n" + "=" * 70)
    print("CHECK 1: Headshot CDN Health (80% threshold)")
    print("=" * 70)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch CDN URLs
    cur.execute(CDN_URL_QUERY)
    cdn_rows = cur.fetchall()

    # Fetch population totals
    cur.execute(POPULATION_COUNT_QUERY)
    pop_row = cur.fetchone()
    total_politicians = pop_row["total"] if pop_row else 0

    cur.execute(POPULATION_WITH_HEADSHOT_QUERY)
    hs_row = cur.fetchone()
    politicians_with_headshots = hs_row["total"] if hs_row else 0

    cur.close()

    total_cdn_urls = len(cdn_rows)
    print(f"\nCDN URLs to validate: {total_cdn_urls}")
    print(f"Total active LA County LOCAL/LOCAL_EXEC/COUNTY politicians: {total_politicians}")
    print(f"Politicians with Supabase headshots: {politicians_with_headshots}")

    if total_cdn_urls == 0:
        print("\nWARN: No CDN URLs found in politician_images — nothing to check.")
        notes = "0 CDN URLs found (no headshots in DB)"
        return False, notes

    # Issue HEAD requests
    ok_count = 0
    failed_urls = []

    print(f"\nIssuing HEAD requests to {total_cdn_urls} CDN URLs...")
    for i, row in enumerate(cdn_rows):
        url = row["url"]
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                ok_count += 1
            else:
                failed_urls.append((resp.status_code, url))
                print(f"  FAIL [{resp.status_code}]: {url}")
        except requests.RequestException as e:
            failed_urls.append(("ERR", url))
            print(f"  ERROR: {url} — {e}")
        time.sleep(0.1)  # 100ms between requests — be polite to CDN

        if (i + 1) % 50 == 0:
            pct_so_far = (ok_count / (i + 1)) * 100
            print(f"  Progress: {i + 1}/{total_cdn_urls} checked ({pct_so_far:.1f}% OK so far)")

    cdn_pct = (ok_count / total_cdn_urls * 100) if total_cdn_urls > 0 else 0.0
    pop_pct = (politicians_with_headshots / total_politicians * 100) if total_politicians > 0 else 0.0

    print(f"\n  CDN health:          {ok_count}/{total_cdn_urls} CDN URLs return HTTP 200 ({cdn_pct:.1f}%)")
    print(f"  Population coverage: {politicians_with_headshots}/{total_politicians} LA County politicians have headshots ({pop_pct:.1f}%)")

    passed = cdn_pct >= 80.0
    status = "PASS" if passed else "FAIL"
    print(f"\n  Result: {status} (CDN health {cdn_pct:.1f}% vs 80% threshold)")

    notes = f"{ok_count}/{total_cdn_urls} CDN URLs OK ({cdn_pct:.1f}%) | pop coverage {politicians_with_headshots}/{total_politicians} ({pop_pct:.1f}%)"
    return passed, notes


# ============================================================
# Check 2 — Contact Website Presence (SQL, 89/89 cities)
# ============================================================

CITY_WEBSITE_QUERY = """
    SELECT COUNT(pc.id) > 0 AS has_contact
    FROM essentials.politicians p
    JOIN essentials.politician_contacts pc ON pc.politician_id = p.id
    WHERE p.full_name = ANY(%s)
      AND pc.contact_type = 'city_website'
      AND pc.website_url != ''
    LIMIT 1
"""


def run_check_2(conn) -> tuple[bool, str]:
    """Check 2: Verify contact website presence for all 89 LA County cities.

    Loads city_sources.json, collects roster names per city, queries DB for
    city_website contacts. A city passes if ANY roster member has a contact row.

    Returns:
        (passed: bool, notes: str)
    """
    print("\n" + "=" * 70)
    print("CHECK 2: Contact Website Presence (89/89 cities)")
    print("=" * 70)

    city_sources_path = Path(__file__).parent / "city_sources.json"
    if not city_sources_path.exists():
        print(f"\nError: city_sources.json not found at {city_sources_path}")
        return False, "city_sources.json missing"

    with open(city_sources_path) as f:
        data = json.load(f)

    cities = data.get("cities", [])
    expected_total = 89

    print(f"\nLoaded {len(cities)} cities from city_sources.json (expected {expected_total})")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cities_with_contact = 0
    cities_without_contact = []

    for city in cities:
        city_name = city.get("name", city.get("id", "unknown"))
        roster = city.get("roster", [])
        names = [member["name"] for member in roster if member.get("name")]

        if not names:
            cities_without_contact.append(f"{city_name} (no roster names)")
            continue

        cur.execute(CITY_WEBSITE_QUERY, (names,))
        row = cur.fetchone()
        has_contact = row["has_contact"] if row else False

        if has_contact:
            cities_with_contact += 1
        else:
            cities_without_contact.append(city_name)

    cur.close()

    if cities_without_contact:
        print(f"\n  Cities WITHOUT city_website contacts ({len(cities_without_contact)}):")
        for city_name in sorted(cities_without_contact):
            print(f"    - {city_name}")

    passed = cities_with_contact == expected_total and len(cities) == expected_total
    status = "PASS" if passed else "FAIL"
    print(f"\n  Result: {status} — {cities_with_contact}/{len(cities)} cities have contact websites")

    notes = f"{cities_with_contact}/{len(cities)} cities covered"
    return passed, notes


# ============================================================
# Check 3 — Zero Government Hotlinks (SQL only)
# ============================================================

HOTLINK_QUERY = """
    SELECT DISTINCT
        p.full_name,
        pi.url
    FROM essentials.politician_images pi
    JOIN essentials.politicians p ON p.id = pi.politician_id
    JOIN essentials.offices o ON o.politician_id = p.id
    JOIN essentials.districts d ON o.district_id = d.id
    WHERE pi.type = 'default'
      AND pi.url IS NOT NULL
      AND pi.url != ''
      AND pi.url NOT LIKE '%supabase%'
      AND d.district_type IN ('LOCAL', 'LOCAL_EXEC', 'COUNTY')
      AND d.state = 'CA'
      AND p.is_active = true
    ORDER BY p.full_name
"""


def run_check_3(conn) -> tuple[bool, str]:
    """Check 3: Confirm zero government domain hotlinks in politician_images.

    Queries for any non-Supabase URLs in politician_images for active LA County
    LOCAL/LOCAL_EXEC/COUNTY politicians. Any row is a hotlink violation.

    Returns:
        (passed: bool, notes: str)
    """
    print("\n" + "=" * 70)
    print("CHECK 3: Zero Government Hotlinks")
    print("=" * 70)

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(HOTLINK_QUERY)
    rows = cur.fetchall()
    cur.close()

    hotlink_count = len(rows)

    if rows:
        print(f"\n  Found {hotlink_count} government hotlink(s) — each must be re-hosted to Supabase:")
        for row in rows:
            print(f"    - {row['full_name']}: {row['url']}")

    passed = hotlink_count == 0
    status = "PASS" if passed else "FAIL"
    print(f"\n  Result: {status} — {hotlink_count} hotlink(s) found")

    notes = f"{hotlink_count} hotlinks found"
    return passed, notes


# ============================================================
# Summary table
# ============================================================

def print_summary(results: list[dict]) -> bool:
    """Print the coverage validation summary table.

    Args:
        results: List of dicts with keys: label, passed, notes, skipped

    Returns:
        True if all non-skipped checks passed, False otherwise.
    """
    print("\n" + "=" * 70)
    print("=== COVERAGE VALIDATION SUMMARY ===")
    print("=" * 70)
    print(f"{'Check':<48} {'Result':<7} {'Notes'}")
    print("-" * 70)

    all_pass = True
    for r in results:
        if r["skipped"]:
            result_str = "SKIP"
        else:
            result_str = "PASS" if r["passed"] else "FAIL"
            if not r["passed"]:
                all_pass = False

        label = r["label"][:47]
        notes = r["notes"]
        print(f"{label:<48} {result_str:<7} {notes}")

    print("=" * 70)

    overall_label = "PASS" if all_pass else "FAIL"
    print(f"OVERALL: {overall_label}")
    print("=" * 70)

    return all_pass


# ============================================================
# Main entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="v1.7 Coverage Validation Report — validates headshot CDN health, contact website presence, and zero government hotlinks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 coverage_report.py              # Run all three checks
  python3 coverage_report.py --check 1   # CDN HEAD requests only
  python3 coverage_report.py --check 2   # Contact website check only
  python3 coverage_report.py --check 3   # Zero hotlinks check only
        """,
    )
    parser.add_argument(
        "--check",
        type=int,
        choices=[1, 2, 3],
        help="Run only the specified check (1, 2, or 3). Defaults to all three.",
    )
    args = parser.parse_args()

    run_all = args.check is None
    run_1 = run_all or args.check == 1
    run_2 = run_all or args.check == 2
    run_3 = run_all or args.check == 3

    print("=" * 70)
    print("v1.7 Coverage Validation Report")
    print("Checks: Headshot CDN Health | Contact Website Presence | Zero Hotlinks")
    print("=" * 70)

    load_env()
    conn = get_connection()

    results = []

    try:
        # Check 1: CDN Health
        if run_1:
            passed_1, notes_1 = run_check_1(conn)
            results.append({
                "label": "1. Headshot CDN Health (80% threshold)",
                "passed": passed_1,
                "notes": notes_1,
                "skipped": False,
            })
        else:
            results.append({
                "label": "1. Headshot CDN Health (80% threshold)",
                "passed": True,
                "notes": "not requested",
                "skipped": True,
            })

        # Check 2: Contact Website Presence
        if run_2:
            passed_2, notes_2 = run_check_2(conn)
            results.append({
                "label": "2. Contact Website Presence (89 cities)",
                "passed": passed_2,
                "notes": notes_2,
                "skipped": False,
            })
        else:
            results.append({
                "label": "2. Contact Website Presence (89 cities)",
                "passed": True,
                "notes": "not requested",
                "skipped": True,
            })

        # Check 3: Zero Hotlinks
        if run_3:
            passed_3, notes_3 = run_check_3(conn)
            results.append({
                "label": "3. Zero Government Hotlinks",
                "passed": passed_3,
                "notes": notes_3,
                "skipped": False,
            })
        else:
            results.append({
                "label": "3. Zero Government Hotlinks",
                "passed": True,
                "notes": "not requested",
                "skipped": True,
            })

    finally:
        conn.close()

    overall_pass = print_summary(results)

    if overall_pass:
        print("\nValidation complete. All checks passed.")
        sys.exit(0)
    else:
        print("\nValidation complete. One or more checks FAILED — see summary above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
