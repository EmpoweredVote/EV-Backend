#!/usr/bin/env python3
"""
import_state_committees.py

Imports state legislative committee memberships from the Open States API v3.
Supplements the LegiScan-based import_state_legislative.py — LegiScan's
getSessionPeople returns committee_id=0 for all state legislators, so Open
States is used as the source for committee membership data.

Usage:
    python import_state_committees.py --state IN [--dry-run] [--verbose]
    python import_state_committees.py --state CA [--dry-run] [--verbose]

Requirements:
    - OPENSTATES_API_KEY in environment or EV-Backend/.env.local
    - DATABASE_URL in environment or EV-Backend/.env.local

Open States API v3:
    Base: https://v3.openstates.org
    Auth: ?apikey=... or x-api-key header
    Rate limit: 10/minute free tier → enforce 6-second delay between requests
"""

import argparse
import logging
import os
import sys
import time
import uuid
from typing import Optional

import psycopg2
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env.local from EV-Backend root
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, "..", ".env.local")
load_dotenv(ENV_PATH)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OPENSTATES_BASE_URL = "https://v3.openstates.org"
RATE_LIMIT_DELAY = 6  # seconds between requests (10/min free tier)

STATE_CONFIG = {
    "IN": {
        "jurisdiction": "indiana",
        "openstates_jurisdiction": "Indiana",
        "state_abbr": "IN",
    },
    "CA": {
        "jurisdiction": "california",
        "openstates_jurisdiction": "California",
        "state_abbr": "CA",
    },
}

# Normalize Open States role strings to DB-friendly values
ROLE_NORMALIZATION = {
    "chair": "chair",
    "vice-chair": "vice_chair",
    "vice chair": "vice_chair",
    "ranking member": "ranking_member",
    "co-chair": "chair",
}


# ---------------------------------------------------------------------------
# Open States API client
# ---------------------------------------------------------------------------
def fetch_committees(api_key: str, jurisdiction: str, page: int = 1, per_page: int = 50) -> dict:
    """Fetch one page of committees with memberships for a jurisdiction."""
    url = f"{OPENSTATES_BASE_URL}/committees"
    params = {
        "jurisdiction": jurisdiction,
        "include": "memberships",
        "per_page": per_page,
        "page": page,
        "apikey": api_key,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_committees(api_key: str, jurisdiction: str) -> list:
    """Fetch all committees (all pages) for a jurisdiction, with rate limiting."""
    all_committees = []
    page = 1

    logging.info(f"Fetching committees for jurisdiction '{jurisdiction}' from Open States...")

    while True:
        logging.debug(f"  Fetching page {page}...")
        data = fetch_committees(api_key, jurisdiction, page=page)

        results = data.get("results", [])
        all_committees.extend(results)

        pagination = data.get("pagination", {})
        max_page = pagination.get("max_page", 1)
        total_items = pagination.get("total_items", len(all_committees))

        logging.debug(
            f"  Page {page}/{max_page}: {len(results)} committees "
            f"(total so far: {len(all_committees)}/{total_items})"
        )

        if page >= max_page:
            break

        page += 1
        logging.debug(f"  Rate limiting: sleeping {RATE_LIMIT_DELAY}s...")
        time.sleep(RATE_LIMIT_DELAY)

    logging.info(f"Fetched {len(all_committees)} committees from Open States")
    return all_committees


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def load_existing_committees(conn, jurisdiction: str) -> dict:
    """
    Load existing committees from DB for this jurisdiction.
    Returns dict keyed by normalized lowercase name -> (id, external_id).
    Also returns dict keyed by external_id -> id for OCD ID matching.
    """
    name_map = {}
    ext_id_map = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, external_id, name
            FROM essentials.legislative_committees
            WHERE jurisdiction = %s
            """,
            (jurisdiction,),
        )
        for row in cur.fetchall():
            db_id, external_id, name = row
            normalized = name.strip().lower()
            name_map[normalized] = {"id": db_id, "external_id": external_id, "name": name}
            if external_id:
                ext_id_map[str(external_id)] = db_id

    logging.info(
        f"Loaded {len(name_map)} existing committees from DB for '{jurisdiction}'"
    )
    return name_map, ext_id_map


def get_current_session_id(conn, jurisdiction: str) -> Optional[str]:
    """Get the UUID of the current legislative session for this jurisdiction."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM essentials.legislative_sessions
            WHERE jurisdiction = %s AND is_current = true
            ORDER BY start_year DESC
            LIMIT 1
            """,
            (jurisdiction,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def derive_chamber(committee_name: str, classification: str, memberships: list) -> str:
    """
    Derive chamber from committee name or membership org_classification.
    Returns 'senate', 'house', or 'joint'.
    """
    name_lower = committee_name.lower()

    if "senate" in name_lower:
        return "senate"
    if "house" in name_lower or "assembly" in name_lower:
        return "house"

    # Try to infer from membership org_classification
    classifications = set()
    for m in memberships:
        person = m.get("person") or {}
        current_role = person.get("current_role") or {}
        org_class = current_role.get("org_classification", "")
        if org_class:
            classifications.add(org_class)

    if classifications == {"upper"}:
        return "senate"
    if classifications == {"lower"}:
        return "house"
    if len(classifications) > 1:
        return "joint"

    # Fall back for subcommittees — classification field
    if classification == "subcommittee":
        return "joint"  # Will be overridden by parent resolution if needed

    return "joint"


def upsert_committee(
    conn,
    ocd_id: str,
    jurisdiction: str,
    name: str,
    committee_type: str,
    chamber: str,
    session_id: Optional[str],
    dry_run: bool,
) -> Optional[str]:
    """
    Insert a new committee row for an Open States committee not in DB.
    Returns the new UUID (or a placeholder in dry-run mode).
    """
    new_id = str(uuid.uuid4())

    if dry_run:
        logging.info(
            f"  [DRY RUN] Would INSERT committee: name='{name}', "
            f"jurisdiction='{jurisdiction}', chamber='{chamber}', type='{committee_type}', "
            f"external_id='{ocd_id}', source='openstates'"
        )
        return new_id

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_committees
                (id, external_id, jurisdiction, name, type, chamber, source, is_current, session_id)
            VALUES (%s, %s, %s, %s, %s, %s, 'openstates', true, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (new_id, ocd_id, jurisdiction, name, committee_type, chamber, session_id),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        # If ON CONFLICT DO NOTHING triggered, fetch the existing row's id
        # (this can happen if external_id is a unique index)
        cur.execute(
            """
            SELECT id FROM essentials.legislative_committees
            WHERE jurisdiction = %s AND name ILIKE %s
            LIMIT 1
            """,
            (jurisdiction, name),
        )
        existing = cur.fetchone()
        return existing[0] if existing else new_id


def lookup_politician(conn, person_name: str, state_abbr: str) -> Optional[str]:
    """
    Look up a politician in essentials.politicians by name and state.
    Uses last_name exact match + first_name prefix (ILIKE) for middle name tolerance.
    Returns politician UUID if exactly one match found, None otherwise.
    """
    # Parse the full name into first and last
    parts = person_name.strip().split()
    if len(parts) < 2:
        logging.debug(f"  Cannot parse name '{person_name}' — fewer than 2 parts")
        return None

    first_name = parts[0]
    last_name = parts[-1]  # Last part is last name

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

    if len(rows) == 0:
        logging.debug(f"  No match for '{person_name}' (last='{last_name}', first='{first_name}', state='{state_abbr}')")
        return None
    if len(rows) > 1:
        logging.debug(
            f"  Ambiguous match for '{person_name}': {len(rows)} rows — skipping"
        )
        return None

    return rows[0][0]


def lookup_openstates_bridge(conn, ocd_person_id: str) -> Optional[str]:
    """
    Check the bridge table for an existing openstates ID → politician mapping.
    Returns politician UUID if found, None otherwise.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT politician_id
            FROM essentials.legislative_politician_id_map
            WHERE id_type = 'openstates' AND id_value = %s
            LIMIT 1
            """,
            (ocd_person_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def upsert_bridge_row(conn, politician_id: str, ocd_person_id: str, dry_run: bool) -> None:
    """Insert a bridge row linking politician to their Open States OCD person ID."""
    if dry_run:
        logging.debug(
            f"  [DRY RUN] Would upsert bridge: politician_id={politician_id}, "
            f"id_type='openstates', id_value='{ocd_person_id}'"
        )
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_politician_id_map
                (id, politician_id, id_type, id_value, verified_at, source)
            VALUES (gen_random_uuid(), %s, 'openstates', %s, NOW(), 'openstates-committees')
            ON CONFLICT DO NOTHING
            """,
            (str(politician_id), ocd_person_id),
        )


def upsert_membership(
    conn,
    committee_id: str,
    politician_id: str,
    role: str,
    session_id: Optional[str],
    dry_run: bool,
) -> bool:
    """
    Insert or update a committee membership row.
    Returns True if row was written, False otherwise.
    Uses congress_number=0 for state (non-federal) legislators.
    """
    if dry_run:
        logging.debug(
            f"  [DRY RUN] Would upsert membership: committee={committee_id}, "
            f"politician={politician_id}, role='{role}'"
        )
        return True

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_committee_memberships
                (id, committee_id, politician_id, congress_number, role, is_current, session_id)
            VALUES (gen_random_uuid(), %s, %s, 0, %s, true, %s)
            ON CONFLICT (committee_id, politician_id, congress_number) DO UPDATE SET
                role = EXCLUDED.role,
                is_current = EXCLUDED.is_current,
                session_id = EXCLUDED.session_id
            """,
            (str(committee_id), str(politician_id), role, session_id),
        )
        return cur.rowcount > 0


def normalize_role(raw_role: str) -> str:
    """Normalize Open States role string to DB-friendly value."""
    normalized = raw_role.strip().lower()
    return ROLE_NORMALIZATION.get(normalized, "member")


# ---------------------------------------------------------------------------
# Main import logic
# ---------------------------------------------------------------------------
def import_committees(
    api_key: str,
    conn,
    state: str,
    dry_run: bool,
    verbose: bool,
) -> dict:
    """
    Main import function. Returns stats dict.
    """
    config = STATE_CONFIG[state]
    jurisdiction = config["jurisdiction"]
    openstates_jurisdiction = config["openstates_jurisdiction"]
    state_abbr = config["state_abbr"]

    stats = {
        "committees_fetched": 0,
        "committees_matched": 0,
        "committees_created": 0,
        "memberships_linked": 0,
        "memberships_skipped_no_match": 0,
        "memberships_skipped_ambiguous": 0,
        "bridge_rows_created": 0,
    }

    # 1. Get current session ID
    session_id = get_current_session_id(conn, jurisdiction)
    if session_id:
        logging.info(f"Using session_id: {session_id}")
    else:
        logging.warning(
            f"No current session found for jurisdiction '{jurisdiction}'. "
            "Memberships will be inserted without session_id."
        )

    # 2. Load existing committees from DB (for name matching)
    existing_by_name, existing_by_ext_id = load_existing_committees(conn, jurisdiction)

    # 3. Fetch all committees from Open States API
    os_committees = fetch_all_committees(api_key, openstates_jurisdiction)
    stats["committees_fetched"] = len(os_committees)

    # 4. Process each committee
    for committee in os_committees:
        ocd_id = committee.get("id", "")
        name = committee.get("name", "").strip()
        classification = committee.get("classification", "committee")
        memberships = committee.get("memberships", [])

        if not name:
            logging.warning(f"  Skipping committee with empty name (ocd_id={ocd_id})")
            continue

        logging.debug(
            f"Processing committee: '{name}' ({classification}, {len(memberships)} members)"
        )

        # --- Committee matching (two-pass) ---
        committee_db_id = None
        normalized_name = name.strip().lower()

        # Pass 1: Try name match to existing DB committee
        if normalized_name in existing_by_name:
            match = existing_by_name[normalized_name]
            committee_db_id = match["id"]
            stats["committees_matched"] += 1
            logging.debug(f"  Matched by name: '{name}' → DB id {committee_db_id}")
        # Pass 1b: Try OCD ID match (for re-runs)
        elif ocd_id in existing_by_ext_id:
            committee_db_id = existing_by_ext_id[ocd_id]
            stats["committees_matched"] += 1
            logging.debug(f"  Matched by OCD ID: '{ocd_id}' → DB id {committee_db_id}")
        else:
            # Pass 2: Create new committee
            chamber = derive_chamber(name, classification, memberships)
            committee_db_id = upsert_committee(
                conn,
                ocd_id=ocd_id,
                jurisdiction=jurisdiction,
                name=name,
                committee_type=classification,
                chamber=chamber,
                session_id=session_id,
                dry_run=dry_run,
            )
            stats["committees_created"] += 1
            logging.info(
                f"  Created new committee: '{name}' (chamber='{chamber}', type='{classification}')"
            )
            # Add to name_map so we don't create duplicates within this run
            existing_by_name[normalized_name] = {"id": committee_db_id, "external_id": ocd_id, "name": name}
            existing_by_ext_id[ocd_id] = committee_db_id

        if not committee_db_id:
            logging.warning(f"  Could not get committee_db_id for '{name}' — skipping memberships")
            continue

        # Commit committee row before processing memberships
        if not dry_run:
            conn.commit()

        # --- Membership processing ---
        for membership in memberships:
            person_name = membership.get("person_name") or ""
            role_raw = membership.get("role") or "member"
            person = membership.get("person") or {}
            ocd_person_id = person.get("id") or ""

            if not person_name:
                logging.debug(f"  Skipping membership with empty person_name")
                stats["memberships_skipped_no_match"] += 1
                continue

            # Try bridge table first (fastest path)
            politician_id = None
            if ocd_person_id:
                politician_id = lookup_openstates_bridge(conn, ocd_person_id)
                if politician_id:
                    logging.debug(
                        f"  Found '{person_name}' via bridge table → {politician_id}"
                    )

            # Fall back to name matching
            if not politician_id:
                politician_id = lookup_politician(conn, person_name, state_abbr)
                if politician_id:
                    logging.debug(
                        f"  Matched '{person_name}' by name → {politician_id}"
                    )
                    # Create bridge row for next time
                    if ocd_person_id:
                        upsert_bridge_row(conn, str(politician_id), ocd_person_id, dry_run)
                        stats["bridge_rows_created"] += 1

            if not politician_id:
                # Distinguish no-match from ambiguous for logging (already logged in lookup_politician)
                logging.debug(f"  Could not link '{person_name}' to a politician — skipping")
                stats["memberships_skipped_no_match"] += 1
                continue

            # Normalize role
            role = normalize_role(role_raw)

            # Upsert membership
            written = upsert_membership(
                conn,
                committee_id=str(committee_db_id),
                politician_id=str(politician_id),
                role=role,
                session_id=session_id,
                dry_run=dry_run,
            )
            if written:
                stats["memberships_linked"] += 1
                logging.debug(
                    f"  Linked '{person_name}' → committee '{name}' (role='{role}')"
                )

        # Commit after each committee's memberships
        if not dry_run:
            conn.commit()

    return stats


def report_final_counts(conn, jurisdiction: str) -> None:
    """Query DB and report final membership counts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(m.id) AS memberships,
                   COUNT(DISTINCT m.committee_id) AS committees_with_members,
                   COUNT(DISTINCT m.politician_id) AS politicians_with_memberships
            FROM essentials.legislative_committee_memberships m
            JOIN essentials.legislative_committees c ON c.id = m.committee_id
            WHERE c.jurisdiction = %s
            """,
            (jurisdiction,),
        )
        row = cur.fetchone()
        if row:
            memberships, committees_w, politicians_w = row
            logging.info(f"  DB final: {memberships} membership rows")
            logging.info(f"  DB final: {committees_w} committees with members")
            logging.info(f"  DB final: {politicians_w} politicians with at least one membership")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Import state legislative committee memberships from Open States API v3"
    )
    parser.add_argument(
        "--state",
        required=True,
        choices=["IN", "CA"],
        help="State to import (IN=Indiana, CA=California)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log operations without writing to database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.dry_run:
        logging.info("DRY RUN mode — no database writes will occur")

    # Load credentials
    api_key = os.getenv("OPENSTATES_API_KEY")
    db_url = os.getenv("DATABASE_URL")

    if not api_key:
        logging.error(
            "OPENSTATES_API_KEY environment variable required. "
            "Register at https://openstates.org/accounts/signup/ "
            "and add to EV-Backend/.env.local"
        )
        sys.exit(1)
    if not db_url:
        logging.error("DATABASE_URL environment variable required")
        sys.exit(1)

    config = STATE_CONFIG[args.state]
    jurisdiction = config["jurisdiction"]

    logging.info(f"Starting committee import for {args.state} ({jurisdiction})")

    conn = psycopg2.connect(db_url)
    try:
        stats = import_committees(
            api_key=api_key,
            conn=conn,
            state=args.state,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        logging.info(f"\n{'=' * 60}")
        logging.info(f"COMMITTEE IMPORT COMPLETE: {args.state}")
        logging.info(f"  Committees fetched:           {stats['committees_fetched']}")
        logging.info(f"  Committees matched (existing): {stats['committees_matched']}")
        logging.info(f"  Committees created (new):      {stats['committees_created']}")
        logging.info(f"  Memberships linked:            {stats['memberships_linked']}")
        logging.info(f"  Memberships skipped (no match):{stats['memberships_skipped_no_match']}")
        logging.info(f"  Bridge rows created:           {stats['bridge_rows_created']}")
        logging.info(f"{'=' * 60}")

        if not args.dry_run:
            logging.info("\nFinal DB counts:")
            report_final_counts(conn, jurisdiction)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
