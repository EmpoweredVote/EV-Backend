#!/usr/bin/env python3
"""
Profile Completeness Report — audits active politicians for data gaps.

Generates a completeness matrix showing which politicians are missing which
fields, grouped by region/body. Report-only — no modifications to data.

Checks:
  1. Photo Coverage — has CDN image or photo_origin_url
  2. Contact Info — has at least one phone, email, or website
  3. Party Affiliation — party field is populated
  4. Term Dates — valid_from is populated
  5. Bio / Background — bio_text, degrees, or experiences exist
  6. Office Metadata — office_title, district_type, chamber_name populated

Usage:
    cd EV-Backend/scripts
    python3 completeness_report.py                     # all regions, summary
    python3 completeness_report.py --region monroe     # Monroe County only
    python3 completeness_report.py --region la         # LA County only
    python3 completeness_report.py --detailed          # list every gap
    python3 completeness_report.py --export csv        # export to CSV

Requires: pip install psycopg2-binary
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env

import psycopg2
import psycopg2.extras


# ============================================================
# Database connection
# ============================================================

def get_connection():
    """Open a psycopg2 connection using DATABASE_URL (direct port 5432)."""
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set. Call load_env() first.")
        sys.exit(1)
    parsed = urlparse(raw_url)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )


# ============================================================
# Region filter
# ============================================================

REGION_FILTERS = {
    "monroe": {"state": "IN", "gov_name_contains": "Monroe"},
    "la": {"state": "CA", "gov_name_contains": "Los Angeles"},
}


def matches_region(row, region):
    """Check if a row matches the region filter."""
    if region is None:
        return True
    filt = REGION_FILTERS.get(region)
    if not filt:
        return True
    if filt.get("state") and row["state"] != filt["state"]:
        return False
    if filt.get("gov_name_contains"):
        if filt["gov_name_contains"].lower() not in (row["gov_name"] or "").lower():
            # Include state-level officials for that state
            if row["state"] == filt["state"]:
                return True
            return False
    return True


def region_label(region):
    """Human-readable region label."""
    if region is None:
        return "All Regions"
    labels = {"monroe": "Monroe County, IN", "la": "LA County, CA"}
    return labels.get(region, region)


# ============================================================
# Main query
# ============================================================

COMPLETENESS_QUERY = """
SELECT
    p.id AS politician_id,
    p.full_name,
    p.party,
    p.valid_from,
    p.bio_text,
    p.photo_origin_url,
    o.title AS office_title,
    c.name AS chamber_name,
    d.district_type,
    g.name AS gov_name,
    g.state,
    -- Photo: has Supabase CDN image
    EXISTS(
        SELECT 1 FROM essentials.politician_images pi
        WHERE pi.politician_id = p.id AND pi.type = 'default'
    ) AS has_image,
    -- Contact: has any non-empty contact record
    EXISTS(
        SELECT 1 FROM essentials.politician_contacts pc
        WHERE pc.politician_id = p.id
          AND (COALESCE(pc.email, '') != ''
               OR COALESCE(pc.phone, '') != ''
               OR COALESCE(pc.website_url, '') != '')
    ) AS has_contact,
    -- Address: has any non-empty address
    EXISTS(
        SELECT 1 FROM essentials.addresses a
        WHERE a.politician_id = p.id
          AND (COALESCE(a.phone1, '') != '' OR COALESCE(a.address1, '') != '')
    ) AS has_address,
    -- Degrees
    EXISTS(
        SELECT 1 FROM essentials.degrees deg
        WHERE deg.politician_id = p.id
    ) AS has_degrees,
    -- Experiences
    EXISTS(
        SELECT 1 FROM essentials.experiences exp
        WHERE exp.politician_id = p.id
    ) AS has_experiences
FROM essentials.politicians p
JOIN essentials.offices o ON o.politician_id = p.id
JOIN essentials.chambers c ON o.chamber_id = c.id
JOIN essentials.districts d ON o.district_id = d.id
JOIN essentials.governments g ON c.government_id = g.id
WHERE p.is_active = true
ORDER BY g.state, g.name, d.district_type, p.last_name
"""


# ============================================================
# Check functions
# ============================================================

def check_photo(row):
    """Returns True if politician has a photo."""
    return (
        row["has_image"]
        or bool((row.get("photo_origin_url") or "").strip())
    )


def check_contact(row):
    """Returns True if politician has any contact info."""
    return row["has_contact"] or row["has_address"]


def check_party(row):
    """Returns True if politician has a party affiliation."""
    return bool((row.get("party") or "").strip())


def check_term_dates(row):
    """Returns True if politician has term start date."""
    return bool((row.get("valid_from") or "").strip())


def check_bio(row):
    """Returns True if politician has bio or background data."""
    has_bio = bool((row.get("bio_text") or "").strip())
    return has_bio or row["has_degrees"] or row["has_experiences"]


def check_office_metadata(row):
    """Returns True if critical office metadata is populated."""
    has_title = bool((row.get("office_title") or "").strip())
    has_dt = bool((row.get("district_type") or "").strip())
    has_chamber = bool((row.get("chamber_name") or "").strip())
    return has_title and has_dt and has_chamber


CHECKS = [
    {"name": "Photo", "fn": check_photo, "label": "1. Photo"},
    {"name": "Contact Info", "fn": check_contact, "label": "2. Contact Info"},
    {"name": "Party Affiliation", "fn": check_party, "label": "3. Party Affiliation"},
    {"name": "Term Dates", "fn": check_term_dates, "label": "4. Term Dates"},
    {"name": "Bio / Background", "fn": check_bio, "label": "5. Bio / Background"},
    {"name": "Office Metadata", "fn": check_office_metadata, "label": "6. Office Metadata"},
]


# ============================================================
# Report rendering
# ============================================================

def format_pol_context(row):
    """Format politician context for detailed listing."""
    title = row.get("office_title") or "Unknown Title"
    gov = (row.get("gov_name") or "").split(",")[0].strip()
    return f"{row['full_name']} ({title}, {gov})"


def print_summary(results, total, region):
    """Print the completeness summary table."""
    print()
    print("=" * 74)
    print(f"PROFILE COMPLETENESS REPORT ({region_label(region)})")
    print("=" * 74)
    print(f"{'Category':<40} {'Complete':>10} {'Missing':>10} {'Coverage':>10}")
    print("-" * 74)

    coverage_sum = 0.0
    for check_name, complete, missing in results:
        pct = (complete / total * 100) if total > 0 else 0.0
        coverage_sum += pct
        print(f"  {check_name:<38} {complete:>10} {missing:>10} {pct:>9.1f}%")

    print("-" * 74)
    avg_coverage = (coverage_sum / len(results)) if results else 0.0
    print(f"  {'OVERALL (avg)':<38} {'':>10} {'':>10} {avg_coverage:>9.1f}%")
    print("=" * 74)
    print(f"\n  Total active politicians: {total}")


def print_detailed(gaps, check_name, rows):
    """Print detailed gap listing for one check."""
    missing = gaps.get(check_name, [])
    if not missing:
        return
    print(f"\nMISSING {check_name.upper()} ({len(missing)}):")
    for row in missing:
        print(f"  - {format_pol_context(row)}")


def export_csv(all_rows, gaps, region):
    """Export completeness gaps to CSV."""
    today = date.today().isoformat()
    region_slug = region or "all"
    filename = f"completeness_gaps_{region_slug}_{today}.csv"
    filepath = Path(__file__).parent / filename

    # Build gap lookup by politician_id
    gap_map = {}
    for row in all_rows:
        pid = str(row["politician_id"])
        gap_map[pid] = {
            "politician_id": pid,
            "full_name": row["full_name"],
            "office_title": row.get("office_title") or "",
            "government_name": (row.get("gov_name") or "").split(",")[0].strip(),
            "district_type": row.get("district_type") or "",
            "state": row.get("state") or "",
        }
        for check in CHECKS:
            gap_map[pid][f"missing_{check['name'].lower().replace(' / ', '_').replace(' ', '_')}"] = (
                not check["fn"](row)
            )

    with open(filepath, "w", newline="") as f:
        fieldnames = [
            "politician_id", "full_name", "office_title", "government_name",
            "district_type", "state",
            "missing_photo", "missing_contact_info", "missing_party_affiliation",
            "missing_term_dates", "missing_bio_background", "missing_office_metadata",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Only write rows that have at least one gap
        for pid, data in gap_map.items():
            has_gap = any(
                data.get(f"missing_{check['name'].lower().replace(' / ', '_').replace(' ', '_')}", False)
                for check in CHECKS
            )
            if has_gap:
                writer.writerow(data)

    print(f"\nExported to: {filepath}")
    return filepath


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Profile Completeness Report — audit active politicians for data gaps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--region", choices=list(REGION_FILTERS.keys()),
                        help="Filter to a specific region")
    parser.add_argument("--detailed", action="store_true",
                        help="List every gap per politician")
    parser.add_argument("--export", choices=["csv"],
                        help="Export gaps to file format")
    args = parser.parse_args()

    load_env()

    print("=" * 60)
    print("Profile Completeness Report")
    print("=" * 60)
    print(f"  Region: {region_label(args.region)}")
    print()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute(COMPLETENESS_QUERY)
        all_rows = cur.fetchall()
        print(f"  Loaded {len(all_rows)} active politician records")

        # Apply region filter
        rows = [r for r in all_rows if matches_region(r, args.region)]
        if args.region:
            print(f"  After region filter: {len(rows)} records")

        total = len(rows)

        # Run all checks
        results = []  # (label, complete_count, missing_count)
        gaps = {}     # check_name -> [rows missing this field]

        for check in CHECKS:
            missing_rows = [r for r in rows if not check["fn"](r)]
            complete = total - len(missing_rows)
            results.append((check["label"], complete, len(missing_rows)))
            gaps[check["name"]] = missing_rows

        # Print summary
        print_summary(results, total, args.region)

        # Print detailed gaps
        if args.detailed:
            print()
            print("-" * 74)
            print("DETAILED GAP LISTING")
            print("-" * 74)
            for check in CHECKS:
                print_detailed(gaps, check["name"], rows)

        # Export if requested
        if args.export == "csv":
            export_csv(rows, gaps, args.region)

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
