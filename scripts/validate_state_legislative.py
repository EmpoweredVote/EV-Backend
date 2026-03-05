#!/usr/bin/env python3
"""
validate_state_legislative.py

Validates Indiana and California state legislative data (bills, votes, bridge records)
for completeness. Cross-references against the known legislator roster, spot-checks
leadership attributions, and flags orphaned records.

Purpose: Verify that the LegiScan imports from v2026.3 (Phase 57) produced complete,
correctly-linked data for both states before moving to documentation.

Usage:
    python validate_state_legislative.py [--dry-run] [--verbose] [--state IN|CA|both]
    python validate_state_legislative.py --legiscan-check  # costs 1 API call per state

Flags:
    --dry-run         Print what would be checked, without running DB queries
    --verbose         Enable DEBUG logging
    --state IN|CA|both  Validate a single state (default: both)
    --legiscan-check  Compare DB counts against LegiScan session totals via API (2 calls)

Environment variables (loaded from EV-Backend/.env.local):
    DATABASE_URL      PostgreSQL connection string
    LEGISCAN_API_KEY  LegiScan API key (only required with --legiscan-check)

Exit codes:
    0  All specified states pass all checks
    1  One or more states fail (or DB error)

Checks performed:
    A. DB bill/vote counts
    B. LegiScan session total comparison (only with --legiscan-check)
    C. Bridge record completeness (essentials.legislative_politician_id_map)
    D. Zero-activity legislator analysis
    E. Orphaned record detection (unlinked bills/votes)
    F. Spot-check verification (leadership figures)
    G. Overall pass/fail determination

Spot-check legislators (known IDs):
    Indiana:
        - 97c61094 (Rodric Bray) — Senate President Pro Tempore
        - LOOKUP:Todd Huston — Speaker of the House (or current successor)
        - LOOKUP:Matt Lehman — rank-and-file Senate
        - LOOKUP:Sharon Negele — rank-and-file House
    California:
        - LOOKUP:Robert Rivas — Assembly Speaker
        - LOOKUP:Mike McGuire — Senate President Pro Tem
        - LOOKUP:Lena Gonzalez — rank-and-file Senate
        - LOOKUP:Buffy Wicks — rank-and-file Assembly
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
import requests
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
BILL_VOTE_THRESHOLD = 0.90      # 90% bill count match against LegiScan acceptable
BRIDGE_COVERAGE_THRESHOLD = 0.80  # 80% of legislators must have bridge records
MAX_ESTABLISHED_ZERO_ACTIVITY_PCT = 0.10  # 10% cap on established zero-activity legislators

LEGISCAN_BASE = "https://api.legiscan.com/"
BUDGET_LIMIT = 30000
COUNTER_PATH = Path.home() / ".ev-backend" / "legiscan_counter.json"

# ---------------------------------------------------------------------------
# State configuration
# ---------------------------------------------------------------------------
STATE_CONFIGS = {
    "IN": {
        "state_abbr": "IN",
        "jurisdiction": "indiana",
        "current_session_year_start": 2026,
        "session_label": "2026 Regular Session",
        "spot_check_leaders": [
            # (spec, description) — spec is partial UUID or "LOOKUP:Name"
            ("97c61094", "Rodric Bray — Senate President Pro Tem"),
            ("LOOKUP:Todd Huston", "Todd Huston — Speaker of the House"),
            ("LOOKUP:Matt Lehman", "Matt Lehman — Senate rank-and-file"),
            ("LOOKUP:Sharon Negele", "Sharon Negele — House rank-and-file"),
        ],
    },
    "CA": {
        "state_abbr": "CA",
        "jurisdiction": "california",
        "current_session_year_start": 2025,
        "session_label": "2025-2026 Session",
        "spot_check_leaders": [
            ("LOOKUP:Robert Rivas", "Robert Rivas — Assembly Speaker"),
            ("LOOKUP:Mike McGuire", "Mike McGuire — Senate President Pro Tem"),
            ("LOOKUP:Lena Gonzalez", "Lena Gonzalez — Senate rank-and-file"),
            ("LOOKUP:Buffy Wicks", "Buffy Wicks — Assembly rank-and-file"),
        ],
    },
}


# ---------------------------------------------------------------------------
# LegiScan budget helpers (copied from import_state_legislative.py — standalone)
# ---------------------------------------------------------------------------

def read_budget() -> int:
    """Read LegiScan monthly budget counter. Returns queries used this month."""
    if not COUNTER_PATH.exists():
        return 0
    with open(COUNTER_PATH) as f:
        data = json.load(f)
    if data.get("month") != datetime.now().strftime("%Y-%m"):
        return 0
    return data.get("queries", 0)


def increment_budget(count: int = 1) -> int:
    """Atomically increment the LegiScan budget counter."""
    current_month = datetime.now().strftime("%Y-%m")
    if COUNTER_PATH.exists():
        with open(COUNTER_PATH) as f:
            data = json.load(f)
        if data.get("month") != current_month:
            data = {"month": current_month, "queries": 0}
    else:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {"month": current_month, "queries": 0}

    data["queries"] += count

    fd, tmp = tempfile.mkstemp(dir=COUNTER_PATH.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.rename(tmp, COUNTER_PATH)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

    return data["queries"]


def legiscan_query(api_key: str, op: str, params: dict = None) -> dict:
    """Call LegiScan API. Checks budget, increments counter. Returns parsed JSON."""
    budget_used = read_budget()
    if budget_used >= BUDGET_LIMIT - 100:
        raise RuntimeError(
            f"LegiScan budget nearly exhausted: {budget_used}/{BUDGET_LIMIT}"
        )

    req_params = {"key": api_key, "op": op}
    if params:
        req_params.update(params)

    resp = requests.get(LEGISCAN_BASE, params=req_params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    increment_budget(1)

    if data.get("status") != "OK":
        raise RuntimeError(f"LegiScan {op} error: {data}")

    return data


# ---------------------------------------------------------------------------
# DB helper functions (following validate_committee_coverage.py patterns)
# ---------------------------------------------------------------------------

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


def get_session_ids(conn, jurisdiction: str, year_start: int) -> list:
    """Get session IDs matching jurisdiction and year_start."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM essentials.legislative_sessions
            WHERE jurisdiction = %s
              AND year_start = %s
            """,
            (jurisdiction, year_start),
        )
        return [str(row[0]) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Check A: DB bill/vote counts
# ---------------------------------------------------------------------------

def check_db_counts(conn, jurisdiction: str, session_ids: list, state_abbr: str) -> dict:
    """Count bills, votes, and active legislators for the given sessions."""
    if not session_ids:
        return {
            "session_ids": [],
            "bill_count": 0,
            "vote_count": 0,
            "legislators_with_sponsorship": 0,
            "legislators_with_votes": 0,
        }

    id_placeholders = ",".join(["%s"] * len(session_ids))

    with conn.cursor() as cur:
        # Count bills in these sessions
        cur.execute(
            f"SELECT COUNT(*) FROM essentials.bills WHERE session_id = ANY(%s::uuid[])",
            (session_ids,),
        )
        bill_count = cur.fetchone()[0]

        # Count vote records in these sessions
        cur.execute(
            f"SELECT COUNT(*) FROM essentials.votes WHERE session_id = ANY(%s::uuid[])",
            (session_ids,),
        )
        vote_count = cur.fetchone()[0]

        # Count distinct legislators with at least one sponsorship (primary or co)
        cur.execute(
            """
            SELECT COUNT(DISTINCT politician_id) FROM (
                SELECT sponsor_id AS politician_id
                FROM essentials.bills
                WHERE session_id = ANY(%s::uuid[])
                  AND sponsor_id IS NOT NULL
                UNION
                SELECT politician_id
                FROM essentials.bill_cosponsors bc
                JOIN essentials.bills b ON b.id = bc.bill_id
                WHERE b.session_id = ANY(%s::uuid[])
            ) sponsors
            """,
            (session_ids, session_ids),
        )
        legislators_with_sponsorship = cur.fetchone()[0]

        # Count distinct legislators with at least one vote
        cur.execute(
            """
            SELECT COUNT(DISTINCT politician_id)
            FROM essentials.votes
            WHERE session_id = ANY(%s::uuid[])
              AND politician_id IS NOT NULL
            """,
            (session_ids,),
        )
        legislators_with_votes = cur.fetchone()[0]

    return {
        "session_ids": session_ids,
        "bill_count": bill_count,
        "vote_count": vote_count,
        "legislators_with_sponsorship": legislators_with_sponsorship,
        "legislators_with_votes": legislators_with_votes,
    }


# ---------------------------------------------------------------------------
# Check B: LegiScan session total comparison
# ---------------------------------------------------------------------------

def check_legiscan_totals(api_key: str, state_abbr: str, year_start: int, db_bill_count: int) -> dict:
    """Compare DB bill count against LegiScan getDatasetList bill_count."""
    data = legiscan_query(api_key, "getDatasetList", {"state": state_abbr})
    datasets = data.get("datasetlist", [])

    # Find dataset matching the current session year_start
    matched = None
    for ds in datasets:
        if ds.get("year_start") == year_start:
            matched = ds
            break

    if not matched:
        return {
            "found": False,
            "legiscan_bill_count": None,
            "match_pct": None,
            "passed": False,
            "error": f"No dataset found for year_start={year_start}",
        }

    legiscan_count = matched.get("bill_count", 0)
    if legiscan_count == 0:
        return {
            "found": True,
            "session_name": matched.get("session_name", ""),
            "legiscan_bill_count": 0,
            "match_pct": None,
            "passed": False,
            "error": "LegiScan bill_count is 0 — cannot compute match percentage",
        }

    # Match percentage: min/max ratio to handle over- or under-import
    match_pct = min(db_bill_count, legiscan_count) / max(db_bill_count, legiscan_count)
    passed = match_pct >= BILL_VOTE_THRESHOLD

    return {
        "found": True,
        "session_name": matched.get("session_name", ""),
        "legiscan_bill_count": legiscan_count,
        "match_pct": match_pct,
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# Check C: Bridge record completeness
# ---------------------------------------------------------------------------

def check_bridge_records(conn, state_abbr: str) -> dict:
    """Check essentials.legislative_politician_id_map coverage for state legislators."""
    with conn.cursor() as cur:
        # Get all state legislators
        cur.execute(
            """
            SELECT DISTINCT p.id, p.first_name, p.last_name
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
              AND d.state = %s
            ORDER BY p.last_name, p.first_name
            """,
            (state_abbr,),
        )
        legislators = cur.fetchall()
        total = len(legislators)

        if total == 0:
            return {
                "total": 0,
                "with_bridge": 0,
                "without_bridge": [],
                "coverage_pct": 0.0,
                "passed": False,
            }

        # Get those with bridge records
        pol_ids = [str(row[0]) for row in legislators]
        cur.execute(
            """
            SELECT DISTINCT politician_id
            FROM essentials.legislative_politician_id_map
            WHERE id_type = 'legiscan'
              AND politician_id = ANY(%s::uuid[])
            """,
            (pol_ids,),
        )
        with_bridge_ids = {str(row[0]) for row in cur.fetchall()}

    without_bridge = [
        (str(row[0]), f"{row[1]} {row[2]}")
        for row in legislators
        if str(row[0]) not in with_bridge_ids
    ]

    coverage_pct = len(with_bridge_ids) / total
    passed = coverage_pct >= BRIDGE_COVERAGE_THRESHOLD

    return {
        "total": total,
        "with_bridge": len(with_bridge_ids),
        "without_bridge": without_bridge,
        "coverage_pct": coverage_pct,
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# Check D: Zero-activity legislator analysis
# ---------------------------------------------------------------------------

def check_zero_activity(conn, state_abbr: str, session_ids: list) -> dict:
    """Identify legislators with no bills or votes, classify new vs established."""
    with conn.cursor() as cur:
        # Get all state legislators with their office start dates
        cur.execute(
            """
            SELECT DISTINCT p.id, p.first_name, p.last_name,
                   MIN(o.start_date) AS earliest_office_start
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
              AND d.state = %s
            GROUP BY p.id, p.first_name, p.last_name
            """,
            (state_abbr,),
        )
        legislators = cur.fetchall()
        total = len(legislators)

        if total == 0 or not session_ids:
            return {
                "total": total,
                "new_zero_activity": [],
                "established_zero_activity": [],
                "active": 0,
                "passed": True,
            }

        pol_ids = [str(row[0]) for row in legislators]

        # Get legislators with any sponsorship activity
        cur.execute(
            """
            SELECT DISTINCT politician_id FROM (
                SELECT sponsor_id AS politician_id
                FROM essentials.bills
                WHERE session_id = ANY(%s::uuid[])
                  AND sponsor_id = ANY(%s::uuid[])
                UNION
                SELECT bc.politician_id
                FROM essentials.bill_cosponsors bc
                JOIN essentials.bills b ON b.id = bc.bill_id
                WHERE b.session_id = ANY(%s::uuid[])
                  AND bc.politician_id = ANY(%s::uuid[])
            ) active_sponsors
            """,
            (session_ids, pol_ids, session_ids, pol_ids),
        )
        active_ids = {str(row[0]) for row in cur.fetchall()}

        # Add legislators with vote activity
        cur.execute(
            """
            SELECT DISTINCT politician_id
            FROM essentials.votes
            WHERE session_id = ANY(%s::uuid[])
              AND politician_id = ANY(%s::uuid[])
            """,
            (session_ids, pol_ids),
        )
        active_ids.update(str(row[0]) for row in cur.fetchall())

    # Legislators with zero activity
    zero_activity = [row for row in legislators if str(row[0]) not in active_ids]

    # Classify: "new" vs "established" based on office start date
    # Session start: roughly year_start January — use Jan 1 of the session year
    # as the cutoff. If office started within 6 months of that, classify as "new"
    # We don't have session year here, but we'll use a 2025-07-01 cutoff
    # (6 months before the current sessions started in Jan 2025 or Jan 2026)
    new_cutoff = datetime(2025, 7, 1).date()

    new_zero = []
    established_zero = []
    for pol_id, first_name, last_name, start_date in zero_activity:
        name = f"{first_name} {last_name}"
        if start_date and start_date >= new_cutoff:
            new_zero.append((str(pol_id), name, start_date))
        else:
            established_zero.append((str(pol_id), name, start_date))

    active_count = total - len(zero_activity)
    established_zero_pct = len(established_zero) / total if total > 0 else 0
    passed = established_zero_pct <= MAX_ESTABLISHED_ZERO_ACTIVITY_PCT

    return {
        "total": total,
        "new_zero_activity": new_zero,
        "established_zero_activity": established_zero,
        "active": active_count,
        "established_zero_pct": established_zero_pct,
        "passed": passed,
    }


# ---------------------------------------------------------------------------
# Check E: Orphaned record detection
# ---------------------------------------------------------------------------

def check_orphaned_records(conn, session_ids: list) -> dict:
    """Count bills with no sponsor, votes/cosponsors with broken FK references."""
    if not session_ids:
        return {
            "bills_no_sponsor": 0,
            "sample_bills_no_sponsor": [],
            "votes_invalid_politician": 0,
            "cosponsors_invalid_politician": 0,
        }

    with conn.cursor() as cur:
        # Bills with no primary sponsor
        cur.execute(
            """
            SELECT COUNT(*) FROM essentials.bills
            WHERE session_id = ANY(%s::uuid[])
              AND sponsor_id IS NULL
            """,
            (session_ids,),
        )
        bills_no_sponsor = cur.fetchone()[0]

        # Sample orphaned bills
        cur.execute(
            """
            SELECT bill_number, title FROM essentials.bills
            WHERE session_id = ANY(%s::uuid[])
              AND sponsor_id IS NULL
            ORDER BY bill_number
            LIMIT 3
            """,
            (session_ids,),
        )
        sample_bills_no_sponsor = cur.fetchall()

        # Votes where politician_id is not in essentials.politicians
        cur.execute(
            """
            SELECT COUNT(*) FROM essentials.votes v
            WHERE v.session_id = ANY(%s::uuid[])
              AND v.politician_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM essentials.politicians p WHERE p.id = v.politician_id
              )
            """,
            (session_ids,),
        )
        votes_invalid_politician = cur.fetchone()[0]

        # Bill cosponsors where politician_id is not in essentials.politicians
        cur.execute(
            """
            SELECT COUNT(*) FROM essentials.bill_cosponsors bc
            JOIN essentials.bills b ON b.id = bc.bill_id
            WHERE b.session_id = ANY(%s::uuid[])
              AND NOT EXISTS (
                  SELECT 1 FROM essentials.politicians p WHERE p.id = bc.politician_id
              )
            """,
            (session_ids,),
        )
        cosponsors_invalid_politician = cur.fetchone()[0]

    return {
        "bills_no_sponsor": bills_no_sponsor,
        "sample_bills_no_sponsor": sample_bills_no_sponsor,
        "votes_invalid_politician": votes_invalid_politician,
        "cosponsors_invalid_politician": cosponsors_invalid_politician,
    }


# ---------------------------------------------------------------------------
# Check F: Spot-check verification
# ---------------------------------------------------------------------------

def check_spot_leaders(conn, state_abbr: str, jurisdiction: str, session_ids: list, spot_checks: list) -> list:
    """Verify spot-check leaders have bridge records and legislative activity."""
    results = []

    for spec, description in spot_checks:
        # Resolve politician ID
        if spec.startswith("LOOKUP:"):
            name = spec[len("LOOKUP:"):]
            pol_id = lookup_politician_by_name(conn, name, state_abbr)
            if not pol_id:
                results.append({
                    "description": description,
                    "found": False,
                    "pol_id": None,
                })
                continue
        else:
            pol_id = resolve_partial_uuid(conn, spec)
            if not pol_id:
                results.append({
                    "description": description,
                    "found": False,
                    "pol_id": None,
                })
                continue

        pol_name = get_politician_name(conn, pol_id)

        # Check bridge record
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM essentials.legislative_politician_id_map
                WHERE politician_id = %s AND id_type = 'legiscan'
                """,
                (pol_id,),
            )
            has_bridge = cur.fetchone()[0] > 0

            # Count bills as primary sponsor
            cur.execute(
                """
                SELECT COUNT(*) FROM essentials.bills
                WHERE session_id = ANY(%s::uuid[])
                  AND sponsor_id = %s::uuid
                """,
                (session_ids, pol_id),
            )
            bills_as_sponsor = cur.fetchone()[0]

            # Count bills as cosponsor
            cur.execute(
                """
                SELECT COUNT(*) FROM essentials.bill_cosponsors bc
                JOIN essentials.bills b ON b.id = bc.bill_id
                WHERE b.session_id = ANY(%s::uuid[])
                  AND bc.politician_id = %s::uuid
                """,
                (session_ids, pol_id),
            )
            bills_as_cosponsor = cur.fetchone()[0]

            # Count votes
            cur.execute(
                """
                SELECT COUNT(*) FROM essentials.votes
                WHERE session_id = ANY(%s::uuid[])
                  AND politician_id = %s::uuid
                """,
                (session_ids, pol_id),
            )
            vote_count = cur.fetchone()[0]

        total_bills = bills_as_sponsor + bills_as_cosponsor
        total_activity = total_bills + vote_count
        # Leadership figures with fewer than 5 total bills+votes are flagged LOW
        flagged_low = total_activity < 5

        results.append({
            "description": description,
            "found": True,
            "pol_id": pol_id,
            "pol_name": pol_name,
            "has_bridge": has_bridge,
            "bills_as_sponsor": bills_as_sponsor,
            "bills_as_cosponsor": bills_as_cosponsor,
            "total_bills": total_bills,
            "vote_count": vote_count,
            "flagged_low": flagged_low,
        })

    return results


# ---------------------------------------------------------------------------
# Per-state validation orchestration
# ---------------------------------------------------------------------------

def validate_state(conn, state: str, verbose: bool, legiscan_check: bool, api_key: Optional[str]) -> bool:
    """
    Run all checks for a single state.
    Returns True if the state passes overall criteria, False otherwise.
    """
    config = STATE_CONFIGS[state]
    jurisdiction = config["jurisdiction"]
    state_abbr = config["state_abbr"]
    session_label = config["session_label"]
    year_start = config["current_session_year_start"]

    print(f"\n{'=' * 60}")
    print(f"Validating {state} ({jurisdiction}) — {session_label}")
    print(f"{'=' * 60}")

    # Resolve session IDs
    session_ids = get_session_ids(conn, jurisdiction, year_start)
    if not session_ids:
        print(f"\nERROR: No sessions found for jurisdiction='{jurisdiction}' year_start={year_start}")
        print(f"{state}: FAIL — no sessions in DB to validate")
        return False

    print(f"\nSessions found: {len(session_ids)} session(s) for year_start={year_start}")
    if verbose:
        for sid in session_ids:
            print(f"  - {sid}")

    # --- Check A: DB bill/vote counts ---
    print(f"\n[A] DB Bill/Vote Counts")
    counts = check_db_counts(conn, jurisdiction, session_ids, state_abbr)
    print(f"  Bills in DB:                     {counts['bill_count']:,}")
    print(f"  Vote records in DB:              {counts['vote_count']:,}")
    print(f"  Legislators with sponsorship:    {counts['legislators_with_sponsorship']}")
    print(f"  Legislators with votes:          {counts['legislators_with_votes']}")

    # --- Check B: LegiScan session total comparison ---
    legiscan_result = None
    if legiscan_check:
        print(f"\n[B] LegiScan Session Total Comparison (API call #{read_budget() + 1})")
        if not api_key:
            print("  SKIP — LEGISCAN_API_KEY not set")
        else:
            try:
                legiscan_result = check_legiscan_totals(api_key, state_abbr, year_start, counts["bill_count"])
                if not legiscan_result["found"]:
                    print(f"  ERROR: {legiscan_result.get('error', 'dataset not found')}")
                elif legiscan_result.get("error"):
                    print(f"  ERROR: {legiscan_result['error']}")
                else:
                    lc = legiscan_result["legiscan_bill_count"]
                    pct = legiscan_result["match_pct"]
                    status = "PASS" if legiscan_result["passed"] else "FAIL"
                    print(f"  Session: {legiscan_result['session_name']}")
                    print(f"  LegiScan bill_count: {lc:,}")
                    print(f"  DB bill count:       {counts['bill_count']:,}")
                    print(f"  Match: {pct:.1%} [{status} — threshold {int(BILL_VOTE_THRESHOLD*100)}%]")
            except Exception as exc:
                print(f"  ERROR calling LegiScan API: {exc}")
                legiscan_result = {"passed": False, "error": str(exc)}
    else:
        print(f"\n[B] LegiScan Session Total Comparison")
        print(f"  SKIP — use --legiscan-check to compare against LegiScan totals (2 API calls)")

    # --- Check C: Bridge record completeness ---
    print(f"\n[C] Bridge Record Completeness (legislative_politician_id_map)")
    bridge = check_bridge_records(conn, state_abbr)
    covered_pct = bridge["coverage_pct"]
    bridge_status = "PASS" if bridge["passed"] else "FAIL"
    print(f"  Legislators in DB:      {bridge['total']}")
    print(f"  With bridge records:    {bridge['with_bridge']} ({covered_pct:.1%})")
    print(f"  Missing bridge:         {len(bridge['without_bridge'])} [{bridge_status}]")

    if bridge["without_bridge"]:
        limit = 10 if verbose else 5
        print(f"  Legislators WITHOUT bridge records (showing up to {limit}):")
        for pol_id, name in bridge["without_bridge"][:limit]:
            print(f"    - {name} ({pol_id[:8]})")
        if len(bridge["without_bridge"]) > limit:
            print(f"    ... and {len(bridge['without_bridge']) - limit} more")

    # --- Check D: Zero-activity legislator analysis ---
    print(f"\n[D] Zero-Activity Legislator Analysis")
    activity = check_zero_activity(conn, state_abbr, session_ids)
    new_count = len(activity["new_zero_activity"])
    est_count = len(activity["established_zero_activity"])
    est_pct = activity.get("established_zero_pct", 0)
    activity_status = "PASS" if activity["passed"] else "FAIL"
    print(f"  Total legislators:                 {activity['total']}")
    print(f"  Active (any bill or vote):         {activity['active']}")
    print(f"  New (zero activity, expected):     {new_count}")
    print(f"  Established with zero activity:    {est_count} ({est_pct:.1%}) [{activity_status}]")

    if activity["established_zero_activity"] and verbose:
        print(f"  Established zero-activity legislators:")
        for pol_id, name, start_date in activity["established_zero_activity"][:10]:
            date_str = str(start_date) if start_date else "unknown"
            print(f"    - {name} (start: {date_str}) ({pol_id[:8]})")

    # --- Check E: Orphaned record detection ---
    print(f"\n[E] Orphaned Record Detection")
    orphans = check_orphaned_records(conn, session_ids)
    print(f"  Bills with no primary sponsor:       {orphans['bills_no_sponsor']}")
    print(f"  Votes with invalid politician_id:    {orphans['votes_invalid_politician']}")
    print(f"  Cosponsors with invalid politician:  {orphans['cosponsors_invalid_politician']}")

    if orphans["sample_bills_no_sponsor"]:
        print(f"  Sample unsponsored bills:")
        for bill_number, title in orphans["sample_bills_no_sponsor"]:
            title_trunc = (title[:60] + "...") if title and len(title) > 60 else title
            print(f"    - {bill_number}: {title_trunc}")

    # --- Check F: Spot-check verification ---
    print(f"\n[F] Leadership Spot-Checks")
    spot_results = check_spot_leaders(
        conn, state_abbr, jurisdiction, session_ids, config["spot_check_leaders"]
    )
    spot_all_ok = True
    for sr in spot_results:
        if not sr["found"]:
            print(f"  [{sr['description']}] — NOT FOUND in DB (skipped)")
            continue

        bridge_mark = "bridge:YES" if sr["has_bridge"] else "bridge:NO"
        status = "LOW" if sr["flagged_low"] else "OK"
        print(
            f"  [{sr['pol_name']}] — {sr['total_bills']} bills "
            f"({sr['bills_as_sponsor']} primary), {sr['vote_count']} votes "
            f"| {bridge_mark} [{status}]"
        )
        if sr["flagged_low"]:
            spot_all_ok = False

    # --- Check G: Overall pass/fail ---
    print(f"\n[G] Overall Pass/Fail")
    criteria_passed = True

    bridge_ok = bridge["passed"]
    activity_ok = activity["passed"]

    print(f"  Bridge coverage >= {int(BRIDGE_COVERAGE_THRESHOLD*100)}%: {'PASS' if bridge_ok else 'FAIL'} ({covered_pct:.1%})")
    print(f"  Established zero-activity <= {int(MAX_ESTABLISHED_ZERO_ACTIVITY_PCT*100)}%: {'PASS' if activity_ok else 'FAIL'} ({est_pct:.1%})")

    if not bridge_ok:
        criteria_passed = False
    if not activity_ok:
        criteria_passed = False

    if legiscan_check and legiscan_result is not None:
        legiscan_ok = legiscan_result.get("passed", False)
        lc_pct = legiscan_result.get("match_pct")
        pct_str = f"{lc_pct:.1%}" if lc_pct is not None else "N/A"
        print(f"  LegiScan bill count match >= {int(BILL_VOTE_THRESHOLD*100)}%: {'PASS' if legiscan_ok else 'FAIL'} ({pct_str})")
        if not legiscan_ok:
            criteria_passed = False

    overall = "PASS" if criteria_passed else "FAIL"
    print(f"\n{state}: {overall}")

    # --- API verification hint ---
    spot_with_id = next((sr for sr in spot_results if sr.get("found") and sr.get("pol_id")), None)
    if spot_with_id:
        pol_id = spot_with_id["pol_id"]
        print(f"\nAPI verification hint (run manually if backend is running):")
        print(f"  curl http://localhost:5050/essentials/politician/{pol_id}/legislative")
        print(f"  Expected: JSON with sessions, bills, votes for this legislator")

    return criteria_passed


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def run_dry_run(states: list, legiscan_check: bool):
    """Print what would be checked without connecting to DB."""
    print("\nDRY RUN — would check the following:")
    threshold_pct = int(BILL_VOTE_THRESHOLD * 100)
    bridge_pct = int(BRIDGE_COVERAGE_THRESHOLD * 100)
    activity_pct = int(MAX_ESTABLISHED_ZERO_ACTIVITY_PCT * 100)

    for state in states:
        config = STATE_CONFIGS[state]
        print(f"\n{state} ({config['jurisdiction']}) — {config['session_label']}:")
        print(f"  [A] Count bills, votes, legislators with activity")
        print(f"      (sessions where jurisdiction='{config['jurisdiction']}' year_start={config['current_session_year_start']})")
        if legiscan_check:
            print(f"  [B] Call getDatasetList for {state}, compare bill_count >= {threshold_pct}% match")
        else:
            print(f"  [B] SKIP (no --legiscan-check flag)")
        print(f"  [C] Check bridge record coverage >= {bridge_pct}%")
        print(f"      (essentials.legislative_politician_id_map, id_type='legiscan')")
        print(f"  [D] Identify zero-activity legislators, classify new vs established")
        print(f"      (established with zero activity must be <= {activity_pct}% of roster)")
        print(f"  [E] Count orphaned bills (no sponsor), votes/cosponsors with broken FK")
        print(f"  [F] Spot-check {len(config['spot_check_leaders'])} leadership legislators:")
        for spec, description in config["spot_check_leaders"]:
            print(f"      - {description}")
        print(f"  [G] Overall PASS/FAIL: bridge + activity criteria, plus LegiScan if requested")

    print(f"\nExit code: 0 if all states PASS, 1 otherwise")
    sys.exit(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Validate Indiana and California state legislative data (bills, votes, "
            "bridge records) for completeness. Exits 0 if all states pass, 1 otherwise."
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
        help="Enable verbose output (show full lists of missing bridges, zero-activity legislators)",
    )
    parser.add_argument(
        "--state",
        choices=["IN", "CA", "both"],
        default="both",
        help="State(s) to validate (default: both)",
    )
    parser.add_argument(
        "--legiscan-check",
        action="store_true",
        help="Compare DB counts against LegiScan session totals (costs 1 API call per state)",
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    states = ["IN", "CA"] if args.state == "both" else [args.state]

    if args.dry_run:
        run_dry_run(states, args.legiscan_check)
        return  # unreachable, but for clarity

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error(
            "DATABASE_URL environment variable required. "
            "Set it in EV-Backend/.env.local or the shell environment."
        )
        sys.exit(1)

    api_key = os.getenv("LEGISCAN_API_KEY") if args.legiscan_check else None
    if args.legiscan_check and not api_key:
        logging.warning(
            "LEGISCAN_API_KEY not set — --legiscan-check will be skipped per state. "
            "Set it in EV-Backend/.env.local to enable LegiScan comparison."
        )

    budget_used = read_budget()
    print("State Legislative Data Validation — Phase 61")
    print(f"Threshold: bills match >= {int(BILL_VOTE_THRESHOLD*100)}% | bridge coverage >= {int(BRIDGE_COVERAGE_THRESHOLD*100)}% | established zero-activity <= {int(MAX_ESTABLISHED_ZERO_ACTIVITY_PCT*100)}%")
    if args.legiscan_check:
        print(f"LegiScan check ENABLED — budget used this month: {budget_used}/{BUDGET_LIMIT}")

    results = {}
    conn = psycopg2.connect(db_url)
    try:
        for state in states:
            results[state] = validate_state(
                conn, state,
                verbose=args.verbose,
                legiscan_check=args.legiscan_check,
                api_key=api_key,
            )
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
        print(f"\nResult: All states pass all validation checks.")
        sys.exit(0)
    else:
        print(f"\nResult: One or more states FAILED validation checks.")
        sys.exit(1)


if __name__ == "__main__":
    main()
