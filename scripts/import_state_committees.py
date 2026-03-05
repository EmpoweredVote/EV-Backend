#!/usr/bin/env python3
"""
import_state_committees.py

Imports state legislative committee memberships from multiple sources:
  - Indiana (--state IN): IGA API (iga.in.gov) — fast, no auth, no rate limits
  - California (--state CA): Open States API v3 — requires OPENSTATES_API_KEY

Supplements the LegiScan-based import_state_legislative.py — LegiScan's
getSessionPeople returns committee_id=0 for all state legislators, so these
APIs are used as the source for committee membership data.

Usage:
    python import_state_committees.py --state IN [--dry-run] [--verbose]
    python import_state_committees.py --state CA [--dry-run] [--verbose]

Requirements:
    - DATABASE_URL in environment or EV-Backend/.env.local
    - OPENSTATES_API_KEY required only for --state CA

IGA API (Indiana):
    Base: https://iga.in.gov/api
    Auth: None (requires browser-like User-Agent header)

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

# ---------------------------------------------------------------------------
# IGA API (Indiana General Assembly) constants
# ---------------------------------------------------------------------------
IGA_BASE_URL = "https://iga.in.gov/api"
IGA_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)
IGA_ROLE_NORMALIZATION = {
    "type_chair": "chair",
    "type_vicechair": "vice_chair",
    "type_rm": "ranking_member",
    "type_rmm": "ranking_member",
    "type_majority_normalmember": "member",
    "type_minority_normalmember": "member",
}
IGA_CHAMBER_MAP = {
    "senate": "upper",
    "house": "lower",
}

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

# Normalize role strings to DB-friendly values (covers both Open States and IGA)
ROLE_NORMALIZATION = {
    # Open States role strings
    "chair": "chair",
    "vice-chair": "vice_chair",
    "vice chair": "vice_chair",
    "ranking member": "ranking_member",
    "co-chair": "chair",
    # IGA normalized values (identity mappings so they pass through)
    "vice_chair": "vice_chair",
    "ranking_member": "ranking_member",
    "member": "member",
}


# ---------------------------------------------------------------------------
# IGA API client (Indiana)
# ---------------------------------------------------------------------------
def fetch_iga_committees(session_lpid: str) -> list:
    """Fetch all committees from IGA API for the given session."""
    url = f"{IGA_BASE_URL}/getCommittees"
    headers = {"User-Agent": IGA_USER_AGENT}
    params = {"session_lpid": session_lpid}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("committees", [])


def fetch_iga_members(committee_id: str, session_lpid: str) -> list:
    """Fetch members for a single IGA committee."""
    url = f"{IGA_BASE_URL}/getMembers"
    headers = {"User-Agent": IGA_USER_AGENT}
    params = {"committee_id": committee_id, "session_lpid": session_lpid}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("members", [])


def fetch_all_iga_data(session_year: int = 2026) -> list:
    """
    Fetch all standing committees + members from IGA API.
    Returns a list of committee dicts in the same shape the
    import_committees() function expects (matching Open States format).
    """
    session_lpid = f"session_{session_year}"
    logging.info(f"Fetching committees from IGA API (session={session_lpid})...")

    raw_committees = fetch_iga_committees(session_lpid)
    logging.info(f"  IGA returned {len(raw_committees)} total committees")

    # Filter to standing committees only (conference committees are bill-specific)
    standing = [c for c in raw_committees if c.get("type") == "standing"]
    logging.info(f"  Filtered to {len(standing)} standing committees")

    result = []
    for committee in standing:
        cid = committee["id"]
        name = committee.get("name", "")
        chamber_lpid = committee.get("chamber_lpid", "")

        # Fetch members
        raw_members = fetch_iga_members(cid, session_lpid)
        logging.debug(f"  {name}: {len(raw_members)} members")

        # Derive org_classification from chamber_lpid
        org_class = IGA_CHAMBER_MAP.get(chamber_lpid, "legislature")

        # Transform members to match Open States membership shape
        memberships = []
        for m in raw_members:
            person_name = f"{m.get('first_name', '')} {m.get('last_name', '')}".strip()
            position_lpid = m.get("position_lpid", "")
            role = IGA_ROLE_NORMALIZATION.get(position_lpid, "member")

            memberships.append({
                "person_name": person_name,
                "role": role,
                "person": {
                    "id": None,  # IGA doesn't use OCD IDs
                    "current_role": {"org_classification": org_class},
                },
            })

        result.append({
            "id": cid,  # IGA UUID used as external_id
            "name": name,
            "classification": "committee",
            "memberships": memberships,
        })

    logging.info(
        f"Fetched {len(result)} standing committees with memberships from IGA"
    )
    return result


# ---------------------------------------------------------------------------
# Open States API client
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_BACKOFF = 10  # seconds between retries


def fetch_committees_page(api_key: str, jurisdiction: str, page: int = 1, per_page: int = 20) -> Optional[dict]:
    """Fetch one page of committees with memberships. Retries on server/rate errors.
    Returns None if all retries exhausted."""
    url = f"{OPENSTATES_BASE_URL}/committees"
    params = {
        "jurisdiction": jurisdiction,
        "include": "memberships",
        "per_page": per_page,
        "page": page,
        "apikey": api_key,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                retry_after = int(e.response.headers.get("Retry-After", 60))
                logging.warning(
                    f"  Rate limited on page {page} (attempt {attempt}/{MAX_RETRIES}), "
                    f"waiting {retry_after}s..."
                )
                time.sleep(retry_after)
                continue
            if status in (500, 502, 503, 504) and attempt < MAX_RETRIES:
                logging.warning(
                    f"  Server error {status} on page {page} "
                    f"(attempt {attempt}/{MAX_RETRIES}), retrying in {RETRY_BACKOFF}s..."
                )
                time.sleep(RETRY_BACKOFF)
                continue
            logging.error(f"  Failed on page {page}: HTTP {status} after {attempt} attempts")
            return None
    logging.error(f"  All {MAX_RETRIES} retries exhausted for page {page}")
    return None


def get_cache_path(jurisdiction: str) -> str:
    """Return path for the committee cache file."""
    return os.path.join(SCRIPT_DIR, f".openstates_cache_{jurisdiction}.json")


def fetch_all_committees(api_key: str, jurisdiction: str) -> list:
    """Fetch all committees with memberships via paginated listing.
    Caches progress to disk so interrupted runs can resume.
    Filters to only return committees that have at least one membership."""
    import json

    cache_path = get_cache_path(jurisdiction)

    # Resume from cache if exists
    cached_committees = []
    resume_page = 1
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            cache_data = json.load(f)
        cached_committees = cache_data.get("committees", [])
        resume_page = cache_data.get("next_page", 1)
        max_page_cached = cache_data.get("max_page", 0)
        logging.info(
            f"Resuming from cache: {len(cached_committees)} committees, "
            f"starting at page {resume_page}/{max_page_cached}"
        )

    committees_with_members = cached_committees
    page = resume_page

    logging.info(f"Fetching committees for '{jurisdiction}' from Open States...")

    while True:
        logging.debug(f"  Fetching page {page}...")
        data = fetch_committees_page(api_key, jurisdiction, page=page)

        if data is None:
            logging.error(f"  Failed to fetch page {page} after retries, saving progress...")
            break

        results = data.get("results", [])
        pagination = data.get("pagination", {})
        max_page = pagination.get("max_page", 1)
        total_items = pagination.get("total_items", 0)

        # Filter to committees with memberships
        for committee in results:
            if committee.get("memberships"):
                committees_with_members.append(committee)

        logging.debug(
            f"  Page {page}/{max_page}: {len(results)} committees, "
            f"{sum(1 for c in results if c.get('memberships'))} with members "
            f"(total kept: {len(committees_with_members)})"
        )

        # Save progress after each page
        with open(cache_path, "w") as f:
            json.dump({
                "committees": committees_with_members,
                "next_page": page + 1,
                "max_page": max_page,
            }, f)

        if page >= max_page:
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    logging.info(
        f"Fetched {len(committees_with_members)} committees with memberships"
    )

    # Clean up cache on success
    if os.path.exists(cache_path) and page >= max_page:
        os.remove(cache_path)
        logging.debug("  Cache file removed (fetch complete)")

    return committees_with_members


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
            ORDER BY start_date DESC
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
    source: str = "openstates",
) -> Optional[str]:
    """
    Insert a new committee row not in DB.
    Returns the new UUID (or a placeholder in dry-run mode).
    """
    new_id = str(uuid.uuid4())

    if dry_run:
        logging.info(
            f"  [DRY RUN] Would INSERT committee: name='{name}', "
            f"jurisdiction='{jurisdiction}', chamber='{chamber}', type='{committee_type}', "
            f"external_id='{ocd_id}', source='{source}'"
        )
        return new_id

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_committees
                (id, external_id, jurisdiction, name, type, chamber, source, is_current, session_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, true, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (new_id, ocd_id, jurisdiction, name, committee_type, chamber, source, session_id),
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
    conn,
    state: str,
    dry_run: bool,
    verbose: bool,
    api_key: Optional[str] = None,
) -> dict:
    """
    Main import function. Returns stats dict.
    """
    config = STATE_CONFIG[state]
    jurisdiction = config["jurisdiction"]
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

    # 3. Fetch committees with memberships
    if state == "IN":
        os_committees = fetch_all_iga_data()
    else:
        os_committees = fetch_all_committees(api_key, config["openstates_jurisdiction"])
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
            source = "iga" if state == "IN" else "openstates"
            committee_db_id = upsert_committee(
                conn,
                ocd_id=ocd_id,
                jurisdiction=jurisdiction,
                name=name,
                committee_type=classification,
                chamber=chamber,
                session_id=session_id,
                dry_run=dry_run,
                source=source,
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
        description="Import state legislative committee memberships (IN=IGA API, CA=Open States)"
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

    if args.state != "IN" and not api_key:
        logging.error(
            "OPENSTATES_API_KEY environment variable required for --state %s. "
            "Register at https://openstates.org/accounts/signup/ "
            "and add to EV-Backend/.env.local",
            args.state,
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
            conn=conn,
            state=args.state,
            dry_run=args.dry_run,
            verbose=args.verbose,
            api_key=api_key,
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
