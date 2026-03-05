#!/usr/bin/env python3
"""
validate_committee_coverage.py

Validates that committee membership data is populated for Indiana and California
state legislators and that coverage meets the 80% threshold required by Phase 60.

Usage:
    python validate_committee_coverage.py [--dry-run] [--verbose]

Flags:
    --dry-run   Print what would be checked, without running DB queries
    --verbose   Enable DEBUG logging

Environment variables (loaded from EV-Backend/.env.local):
    DATABASE_URL   PostgreSQL connection string

Exit codes:
    0  Both IN and CA meet the 80% coverage threshold
    1  One or both states fail the 80% coverage threshold (or DB error)

Coverage threshold: 80% of state legislators in the DB must have at least
one committee assignment in essentials.legislative_committee_memberships.

Spot-check legislators (known IDs from v2026.3):
    Indiana:
        - 97c61094 (Rodric Bray) — Senate President Pro Tempore, expect 5+ committees
    California:
        - 0afa998d (Lisa Calderon) — Assembly member, expect 2+ committees
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env.local from EV-Backend root (parent of scripts/)
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env.local")
load_dotenv(ENV_PATH)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COVERAGE_THRESHOLD = 0.80  # 80% of legislators in DB must have committee data

# State configuration for coverage checks
STATE_CONFIGS = {
    "IN": {
        "jurisdiction": "indiana",
        "state_abbr": "IN",
        "session_label": "2026",
        # Known test politicians: (partial_uuid, expected_min_committees, description)
        "spot_checks": [
            ("97c61094", 1, "Rodric Bray — Senate President Pro Tempore"),
            # Indiana rank-and-file legislators (first/last-name based lookup below)
            ("LOOKUP:Eric Bassler", 1, "Eric Bassler — State Senator"),
            ("LOOKUP:Greg Taylor", 1, "Greg Taylor — State Senator"),
        ],
        "api_test_id": "97c61094-b345-40e3-94a9-0bb4c252757b",  # Will be resolved from partial
    },
    "CA": {
        "jurisdiction": "california",
        "state_abbr": "CA",
        "session_label": "2025-2026",
        "spot_checks": [
            ("0afa998d", 1, "Lisa Calderon — Assembly Member"),
            ("LOOKUP:Scott Wiener", 1, "Scott Wiener — State Senator"),
            ("LOOKUP:Robert Rivas", 1, "Robert Rivas — Assembly Speaker"),
        ],
        "api_test_id": "0afa998d-6018-4e61-9147-23a0a184fa67",  # Will be resolved from partial
    },
}


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

def count_legislators(conn, state_abbr: str) -> int:
    """Count total state legislators for this state in the DB."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(DISTINCT p.id)
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
              AND d.state = %s
            """,
            (state_abbr,),
        )
        row = cur.fetchone()
        return row[0] if row else 0


def count_legislators_with_committees(conn, state_abbr: str, jurisdiction: str) -> int:
    """Count state legislators who have at least one committee assignment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(DISTINCT p.id)
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            JOIN essentials.legislative_committee_memberships m ON m.politician_id = p.id
            JOIN essentials.legislative_committees c ON c.id = m.committee_id
              AND c.jurisdiction = %s
            WHERE d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
              AND d.state = %s
            """,
            (jurisdiction, state_abbr),
        )
        row = cur.fetchone()
        return row[0] if row else 0


def resolve_partial_uuid(conn, partial_id: str) -> Optional[str]:
    """Resolve a partial UUID prefix to a full politician ID."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM essentials.politicians
            WHERE id::text ILIKE %s || '%%'
            LIMIT 1
            """,
            (partial_id,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None


def lookup_politician_by_name(conn, full_name: str, state_abbr: str) -> Optional[str]:
    """Look up a politician by full name and state."""
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None
    first_name = parts[0]
    last_name = parts[-1]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.id
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE LOWER(p.last_name) = LOWER(%s)
              AND LOWER(p.first_name) ILIKE LOWER(%s) || '%%'
              AND d.state = %s
              AND d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
            """,
            (last_name, first_name, state_abbr),
        )
        rows = cur.fetchall()
    if len(rows) == 1:
        return str(rows[0][0])
    return None


def get_politician_committees(conn, politician_id: str, jurisdiction: str) -> list:
    """
    Get committee assignments for a politician.
    Returns list of (committee_name, role) tuples.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.name, m.role
            FROM essentials.legislative_committee_memberships m
            JOIN essentials.legislative_committees c ON c.id = m.committee_id
            WHERE m.politician_id = %s
              AND c.jurisdiction = %s
            ORDER BY m.role, c.name
            """,
            (politician_id, jurisdiction),
        )
        return cur.fetchall()


def get_politician_name(conn, politician_id: str) -> str:
    """Get a politician's display name by ID."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT first_name, last_name FROM essentials.politicians WHERE id = %s",
            (politician_id,),
        )
        row = cur.fetchone()
        if row:
            return f"{row[0]} {row[1]}"
        return f"(unknown: {politician_id[:8]})"


def get_db_committee_counts(conn, jurisdiction: str) -> dict:
    """Get overall DB counts for a jurisdiction."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT c.id) AS committees,
                COUNT(DISTINCT m.id) AS memberships,
                COUNT(DISTINCT m.politician_id) AS politicians_with_membership
            FROM essentials.legislative_committees c
            LEFT JOIN essentials.legislative_committee_memberships m ON m.committee_id = c.id
            WHERE c.jurisdiction = %s
            """,
            (jurisdiction,),
        )
        row = cur.fetchone()
        if row:
            return {
                "committees": row[0],
                "memberships": row[1],
                "politicians_with_membership": row[2],
            }
        return {}


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------

def validate_state(conn, state: str, verbose: bool) -> bool:
    """
    Validate committee coverage for a single state.
    Returns True if the state meets the 80% threshold, False otherwise.
    """
    config = STATE_CONFIGS[state]
    jurisdiction = config["jurisdiction"]
    state_abbr = config["state_abbr"]
    session_label = config["session_label"]

    print(f"\n{'=' * 60}")
    print(f"Validating {state} ({jurisdiction}) — {session_label} session")
    print(f"{'=' * 60}")

    # --- Overall DB counts ---
    db_counts = get_db_committee_counts(conn, jurisdiction)
    print(f"\nDB counts for {jurisdiction}:")
    print(f"  Committees in DB:               {db_counts.get('committees', 0)}")
    print(f"  Membership rows:                {db_counts.get('memberships', 0)}")
    print(f"  Politicians with membership:    {db_counts.get('politicians_with_membership', 0)}")

    # --- Coverage check ---
    total = count_legislators(conn, state_abbr)
    covered = count_legislators_with_committees(conn, state_abbr, jurisdiction)

    if total == 0:
        print(f"\nERROR: No {state} state legislators found in DB (district_type STATE_UPPER/STATE_LOWER, state='{state_abbr}')")
        print(f"{state}: FAIL — no legislators in DB to validate coverage")
        return False

    pct = covered / total
    print(f"\nCoverage: {covered}/{total} {state} legislators in DB have committee assignments ({pct:.1%})")

    passed = pct >= COVERAGE_THRESHOLD
    threshold_pct = int(COVERAGE_THRESHOLD * 100)
    if passed:
        print(f"{state}: PASS (>= {threshold_pct}% threshold)")
    else:
        print(f"{state}: FAIL (below {threshold_pct}% threshold — {covered}/{total} = {pct:.1%})")

    # --- Spot checks ---
    print(f"\nSpot checks for {state}:")
    for spec, expected_min, description in config["spot_checks"]:
        if spec.startswith("LOOKUP:"):
            name = spec[len("LOOKUP:"):]
            pol_id = lookup_politician_by_name(conn, name, state_abbr)
            if not pol_id:
                print(f"  [{description}] — NOT FOUND in DB (skipped)")
                continue
        else:
            pol_id = resolve_partial_uuid(conn, spec)
            if not pol_id:
                print(f"  [{description}] — UUID prefix '{spec}' not found in DB (skipped)")
                continue

        pol_name = get_politician_name(conn, pol_id)
        committees = get_politician_committees(conn, pol_id, jurisdiction)
        count = len(committees)

        if count >= expected_min:
            status = "OK"
        else:
            status = f"LOW (expected >= {expected_min})"

        print(f"  [{pol_name}] — {count} committee(s) [{status}]")
        if committees and verbose:
            sample = committees[:3]
            for committee_name, role in sample:
                print(f"      - {committee_name} (role: {role})")
            if len(committees) > 3:
                print(f"      ... and {len(committees) - 3} more")
        elif committees:
            # Always show at least one sample committee
            committee_name, role = committees[0]
            print(f"      Sample: {committee_name} (role: {role})")

    # --- API verification hint ---
    api_test_id_partial = config["spot_checks"][0][0]  # First spot check UUID
    pol_id = resolve_partial_uuid(conn, api_test_id_partial)
    if pol_id:
        print(f"\nAPI verification (run manually if backend is running):")
        print(f"  curl http://localhost:5050/essentials/politician/{pol_id}/committees")
        print(f"  Expected: JSON array with committee_name, role, chamber fields")

    return passed


def run_dry_run(args):
    """Print what would be checked without connecting to DB."""
    print("\nDRY RUN — would check the following:")
    for state, config in STATE_CONFIGS.items():
        print(f"\n{state} ({config['jurisdiction']}):")
        print(f"  1. Count legislators with district_type STATE_UPPER/STATE_LOWER, state='{config['state_abbr']}'")
        print(f"  2. Count those with >= 1 committee in legislative_committee_memberships (jurisdiction='{config['jurisdiction']}')")
        print(f"  3. Calculate coverage %, flag PASS/FAIL against {int(COVERAGE_THRESHOLD * 100)}% threshold")
        print(f"  4. Spot-check {len(config['spot_checks'])} legislators:")
        for spec, expected_min, description in config["spot_checks"]:
            print(f"      - {description} (expect >= {expected_min} committee(s))")
        print(f"  5. Print curl command for API endpoint check")
    print("\nExit code: 0 if both states PASS, 1 otherwise")
    sys.exit(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Validate committee coverage for Indiana and California state legislators. "
            "Exits 0 if both states meet the 80% threshold, 1 otherwise."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be checked, without running DB queries",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output (show multiple committee samples per legislator)",
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.dry_run:
        run_dry_run(args)
        return  # unreachable, but for clarity

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error(
            "DATABASE_URL environment variable required. "
            "Set it in EV-Backend/.env.local or the shell environment."
        )
        sys.exit(1)

    print("Committee Coverage Validation — Phase 60")
    print(f"Threshold: {int(COVERAGE_THRESHOLD * 100)}% of DB legislators must have committee assignments")

    results = {}
    conn = psycopg2.connect(db_url)
    try:
        for state in ["IN", "CA"]:
            results[state] = validate_state(conn, state, verbose=args.verbose)
    finally:
        conn.close()

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    all_passed = True
    for state, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {state}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\nResult: Both states meet the 80% coverage threshold.")
        sys.exit(0)
    else:
        print("\nResult: One or more states do NOT meet the 80% coverage threshold.")
        sys.exit(1)


if __name__ == "__main__":
    main()
