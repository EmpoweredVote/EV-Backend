#!/usr/bin/env python3
"""
LA County Board of Supervisors Data Importer (Legistar REST API)

Fetches committee assignments and legislation metadata from the Legistar OData API
for the 5 current LA County supervisors. No token required.

Usage:
    python import_local_lacounty.py --dry-run --verbose
    python import_local_lacounty.py --verbose
    python import_local_lacounty.py --committees-only --verbose
    python import_local_lacounty.py --legislation-only --verbose
    python import_local_lacounty.py --legislation-only --max-matters 50 --dry-run --verbose

Environment variables (loaded from EV-Backend/.env.local):
    DATABASE_URL   PostgreSQL connection string

Data sources:
    Legistar OData v3 API — https://webapi.legistar.com/v1/LACounty
    No authentication required for LA County public records.

Notes:
    - OData v3 does NOT support the `in` operator — requests loop per PersonId
    - MatterRequester is NULL for ~98% of recent BOS matters
    - MatterHistories.MoverName populated for pre-2010 data only
    - /VoteRecords returns 404 — no individual vote attribution available
    - Legislation is only imported where MoverName/MatterRequester yields a supervisor match
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

# Load .env.local from EV-Backend root (parent of scripts/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env.local")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_VERSION = "1.0.0"
LEGISTAR_BASE = "https://webapi.legistar.com/v1/LACounty"
BOS_BODY_ID = 76
REQUEST_TIMEOUT = 30
API_DELAY = 0.5  # seconds between Legistar API calls

# Known supervisor PersonIds (from Phase 58-01 feasibility check, verified 2026-03-02)
SUPERVISOR_PERSON_IDS = {
    799: "Hilda L. Solis",
    938: "Janice Hahn",
    937: "Kathryn Barger",
    1141: "Holly J. Mitchell",
    1300: "Lindsey P. Horvath",
}

# Legistar body type IDs
BODY_TYPE_BOARD = 1       # Board / Commission
BODY_TYPE_COMMITTEE = 2   # Committee

# Jurisdiction key for all LA County records
JURISDICTION = "la-county-ca"

# Session external ID (stable identifier, not a UUID)
SESSION_EXTERNAL_ID = "lacounty-current"

# Legistar web base URL for matter detail pages
LEGISTAR_WEB_BASE = "https://lacounty.legistar.com/LegislationDetail.aspx"

# Fuzzy matching thresholds
BRIDGE_FUZZY_THRESHOLD = 80   # for building supervisor bridge from DB names
ATTRIBUTION_FUZZY_THRESHOLD = 75  # for matching MoverName/Requester in matters

# Status label normalization map (MatterStatusName -> standard label)
STATUS_LABEL_MAP = {
    "adopted": "Passed",
    "approved": "Passed",
    "passed": "Passed",
    "enacted": "Passed",
    "failed": "Failed",
    "defeated": "Failed",
    "rejected": "Failed",
    "withdrawn": "Withdrawn",
    "tabled": "Tabled",
    "filed": "Filed",
    "continued": "Pending",
    "deferred": "Pending",
    "referred": "Referred",
    "introduced": "Introduced",
    "received": "Received",
    "presented": "Introduced",
    "heard": "Heard",
    "noted": "Noted",
}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def legistar_get(endpoint: str, params: dict = None) -> list | dict | None:
    """
    GET from Legistar OData API. Returns parsed JSON or None on 404/error.

    Adds a polite delay after each call to respect the public API.
    """
    url = f"{LEGISTAR_BASE}/{endpoint.lstrip('/')}"
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        time.sleep(API_DELAY)
        if r.status_code == 404:
            log.debug(f"  404: {url} (endpoint not available)")
            return None
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        log.warning(f"  HTTP error for {url}: {e}")
        return None
    except requests.RequestException as e:
        log.warning(f"  Request error for {url}: {e}")
        return None
    except Exception as e:
        log.warning(f"  Unexpected error for {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Date parsing helper
# ---------------------------------------------------------------------------

def parse_legistar_date(value) -> datetime | None:
    """
    Parse a Legistar date value. Legistar returns dates as:
    - ISO string: "2024-03-15T00:00:00"
    - OData legacy: "/Date(1710460800000)/" (milliseconds since epoch)
    - None / null / empty string

    Returns a datetime object or None.
    """
    if not value or value in ("null", "NULL", ""):
        return None

    # OData legacy format: /Date(milliseconds)/
    if isinstance(value, str) and value.startswith("/Date("):
        try:
            ms = int(value[6:value.index(")")])
            return datetime.utcfromtimestamp(ms / 1000)
        except (ValueError, IndexError):
            return None

    # ISO string format
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.strptime(value[:19], fmt)
            except ValueError:
                continue

    return None


# ---------------------------------------------------------------------------
# 1. Bridge table: Legistar PersonId -> politician UUID
# ---------------------------------------------------------------------------

def build_supervisor_bridge(conn, dry_run: bool = False, verbose: bool = False) -> dict:
    """
    Match Legistar PersonIds to politician UUIDs using name fuzzy matching.

    Strategy:
    - Load all active politicians from essentials.politicians
    - Use RapidFuzz token_sort_ratio (threshold=80) to match each supervisor name
    - Single match only: skip if 0 or 2+ matches above threshold
    - INSERT bridge rows with id_type='legistar', id_value=str(person_id)
    - Return dict: {legistar_person_id (int) -> politician_uuid (str)}
    """
    log.info("Building supervisor bridge (Legistar PersonId -> DB UUID) ...")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Load all active politicians once
    cur.execute(
        "SELECT id::text, full_name FROM essentials.politicians WHERE is_active = true"
    )
    all_politicians = cur.fetchall()
    log.info(f"  Loaded {len(all_politicians)} active politicians from DB")

    bridge_map = {}

    for person_id, legistar_name in SUPERVISOR_PERSON_IDS.items():
        # Find all candidates scoring above threshold
        candidates = []
        for row in all_politicians:
            score = fuzz.token_sort_ratio(
                legistar_name.lower(), row["full_name"].lower()
            )
            if score >= BRIDGE_FUZZY_THRESHOLD:
                candidates.append((score, row["id"], row["full_name"]))

        candidates.sort(reverse=True)

        if len(candidates) == 0:
            log.warning(f"  MISS: {legistar_name} (PersonId={person_id}) — no match above {BRIDGE_FUZZY_THRESHOLD}")
            continue

        if len(candidates) > 1:
            # Check if top score is significantly higher (>5 points) than second
            if candidates[0][0] - candidates[1][0] > 5:
                # Clear winner — use top match
                score, db_id, db_name = candidates[0]
                log.info(f"  MATCH (best): {legistar_name} -> {db_name} (score={score}, PersonId={person_id})")
            else:
                log.warning(
                    f"  AMBIG: {legistar_name} (PersonId={person_id}) — {len(candidates)} candidates above threshold; "
                    f"top scores: {[(c[0], c[2]) for c in candidates[:3]]}"
                )
                continue
        else:
            score, db_id, db_name = candidates[0]
            if verbose:
                log.info(f"  MATCH: {legistar_name} -> {db_name} (score={score}, PersonId={person_id})")

        score, db_id, db_name = candidates[0]
        bridge_map[person_id] = db_id

        if not dry_run:
            cur.execute(
                """
                INSERT INTO essentials.legislative_politician_id_map
                    (id, politician_id, id_type, id_value, verified_at, source)
                VALUES
                    (gen_random_uuid(), %s::uuid, 'legistar', %s, NOW(), 'legistar-lacounty')
                ON CONFLICT (politician_id, id_type, id_value) DO NOTHING
                """,
                (db_id, str(person_id)),
            )
        else:
            log.info(f"  [dry-run] Would insert bridge: politician_id={db_id}, id_value={person_id}")

    if not dry_run:
        conn.commit()

    cur.close()

    matched = len(bridge_map)
    total = len(SUPERVISOR_PERSON_IDS)
    log.info(f"Bridge built: {matched}/{total} supervisors matched and inserted")

    return bridge_map


# ---------------------------------------------------------------------------
# 2. Session upsert
# ---------------------------------------------------------------------------

def ensure_session(conn, dry_run: bool = False) -> str | None:
    """
    Upsert a LegislativeSession for LA County BOS.

    Returns the session UUID (str) or None in dry-run mode.
    """
    log.info("Ensuring LA County BOS legislative session ...")

    if dry_run:
        log.info("  [dry-run] Would upsert session: jurisdiction=la-county-ca, external_id=lacounty-current")
        return None

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        INSERT INTO essentials.legislative_sessions
            (id, jurisdiction, name, is_current, external_id, source, start_date, end_date)
        VALUES
            (gen_random_uuid(), %s, %s, true, %s, 'legistar', NULL, NULL)
        ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
            name = EXCLUDED.name,
            is_current = EXCLUDED.is_current,
            source = EXCLUDED.source
        """,
        (JURISDICTION, "LA County Board of Supervisors (Current Term)", SESSION_EXTERNAL_ID),
    )
    conn.commit()

    cur.execute(
        "SELECT id::text FROM essentials.legislative_sessions WHERE external_id = %s AND jurisdiction = %s",
        (SESSION_EXTERNAL_ID, JURISDICTION),
    )
    row = cur.fetchone()
    cur.close()

    if row:
        session_id = row["id"]
        log.info(f"  Session ID: {session_id}")
        return session_id

    log.error("  Failed to retrieve session ID after upsert")
    return None


# ---------------------------------------------------------------------------
# 3. Fetch functions
# ---------------------------------------------------------------------------

def fetch_active_office_records(person_id: int, cutoff_date: str = "2025-01-01") -> list:
    """
    GET /OfficeRecords filtered by PersonId and EndDate > cutoff_date.

    Returns list of OfficeRecord dicts. Cutoff filters out old/expired assignments.
    """
    params = {
        "$filter": (
            f"OfficeRecordPersonId eq {person_id} "
            f"and OfficeRecordEndDate gt datetime'{cutoff_date}'"
        ),
        "$top": 100,
    }
    data = legistar_get("OfficeRecords", params=params)
    return data if data is not None else []


def fetch_active_bodies() -> dict:
    """
    GET /Bodies filtered to active boards and committees (BodyTypeId 1 or 2).

    Returns dict: {BodyId (int) -> body_dict} for fast lookup by BodyId.
    """
    params = {
        "$filter": "BodyActiveFlag eq 1",
        "$top": 200,
    }
    data = legistar_get("Bodies", params=params)
    if not data:
        return {}

    # Filter to boards (type=1) and committees (type=2)
    bodies = {
        b["BodyId"]: b
        for b in data
        if b.get("BodyTypeId") in (BODY_TYPE_BOARD, BODY_TYPE_COMMITTEE)
    }
    log.info(f"  Fetched {len(bodies)} active boards/committees from Legistar Bodies endpoint")
    return bodies


def fetch_bos_matters(top: int = 1000, skip: int = 0) -> list:
    """
    GET /Matters filtered to BOS body (BodyId=76), ordered by last modified desc.

    Returns list of Matter dicts.
    """
    params = {
        "$filter": f"MatterBodyId eq {BOS_BODY_ID}",
        "$top": top,
        "$skip": skip,
        "$orderby": "MatterLastModifiedUtc desc",
    }
    data = legistar_get("Matters", params=params)
    return data if data is not None else []


def fetch_matter_histories(matter_id: int) -> list:
    """
    GET /Matters/{matter_id}/Histories.

    Returns list of MatterHistory dicts. Returns empty list on 404 or error.
    """
    data = legistar_get(f"Matters/{matter_id}/Histories")
    return data if data is not None else []


# ---------------------------------------------------------------------------
# 4. Committees import
# ---------------------------------------------------------------------------

def parse_role_from_title(title: str | None) -> str:
    """
    Normalize OfficeRecordTitle to a standard role string.

    Legistar titles vary (e.g., "Chair", "Chairperson", "1st Vice Chair", etc.)
    Map to: "chair", "vice_chair", "alternate", "member"
    """
    if not title:
        return "member"

    title_lower = title.lower().strip()

    if "vice chair" in title_lower or "vice-chair" in title_lower:
        return "vice_chair"
    if "chair" in title_lower:
        return "chair"
    if "alternate" in title_lower:
        return "alternate"

    return "member"


def import_committees(
    bridge_map: dict,
    session_id: str | None,
    conn,
    active_bodies: dict,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Import committee assignments for all matched supervisors.

    For each supervisor:
    - Fetch active OfficeRecords (active after 2025-01-01)
    - Upsert LegislativeCommittee for each body
    - Upsert LegislativeCommitteeMembership linking supervisor to committee

    Returns dict: {external_id -> committee_uuid} for use in legislation import.
    """
    log.info("Importing committee assignments ...")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    committee_cache = {}   # external_id -> committee_uuid
    total_memberships = 0
    total_committees_seen = 0
    supervisors_processed = 0

    for person_id, db_politician_id in bridge_map.items():
        legistar_name = SUPERVISOR_PERSON_IDS.get(person_id, f"PersonId={person_id}")
        log.info(f"  Fetching OfficeRecords for {legistar_name} (PersonId={person_id}) ...")

        records = fetch_active_office_records(person_id)
        if not records:
            log.warning(f"    No active OfficeRecords found for {legistar_name}")
            continue

        log.info(f"    {len(records)} active committee assignments")
        supervisors_processed += 1

        for record in records:
            body_id = record.get("OfficeRecordBodyId")
            body_name = record.get("OfficeRecordBodyName", f"Body {body_id}")
            office_title = record.get("OfficeRecordTitle")
            external_id = f"legistar-{body_id}"

            # Determine committee type from active_bodies lookup
            body_info = active_bodies.get(body_id, {})
            body_type_id = body_info.get("BodyTypeId", BODY_TYPE_COMMITTEE)
            committee_type = "board" if body_type_id == BODY_TYPE_BOARD else "committee"

            role = parse_role_from_title(office_title)

            if verbose:
                log.info(f"      {body_name} | title='{office_title}' -> role='{role}' | type={committee_type}")

            if external_id not in committee_cache:
                total_committees_seen += 1

                if not dry_run:
                    # Upsert committee
                    cur.execute(
                        """
                        INSERT INTO essentials.legislative_committees
                            (id, session_id, external_id, jurisdiction, name, type, chamber,
                             is_current, source)
                        VALUES
                            (gen_random_uuid(), %s::uuid, %s, %s, %s, %s, 'local', true, 'legistar')
                        ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
                            name = EXCLUDED.name,
                            type = EXCLUDED.type,
                            is_current = EXCLUDED.is_current,
                            session_id = EXCLUDED.session_id
                        """,
                        (session_id, external_id, JURISDICTION, body_name, committee_type),
                    )

                    # Retrieve UUID
                    cur.execute(
                        """
                        SELECT id::text FROM essentials.legislative_committees
                        WHERE external_id = %s AND jurisdiction = %s
                        """,
                        (external_id, JURISDICTION),
                    )
                    row = cur.fetchone()
                    if row:
                        committee_cache[external_id] = row["id"]
                    else:
                        log.error(f"    Failed to retrieve committee UUID for {external_id}")
                        continue
                else:
                    log.info(f"    [dry-run] Would upsert committee: {external_id} ({body_name})")
                    committee_cache[external_id] = f"dry-run-{external_id}"

            committee_uuid = committee_cache.get(external_id)
            if not committee_uuid:
                continue

            total_memberships += 1

            if not dry_run:
                cur.execute(
                    """
                    INSERT INTO essentials.legislative_committee_memberships
                        (id, committee_id, politician_id, congress_number, role, is_current, session_id)
                    VALUES
                        (gen_random_uuid(), %s::uuid, %s::uuid, 0, %s, true, %s::uuid)
                    ON CONFLICT (committee_id, politician_id, congress_number) DO UPDATE SET
                        role = EXCLUDED.role,
                        is_current = EXCLUDED.is_current,
                        session_id = EXCLUDED.session_id
                    """,
                    (committee_uuid, db_politician_id, role, session_id),
                )
            else:
                log.info(
                    f"    [dry-run] Would upsert membership: "
                    f"politician={db_politician_id}, committee={external_id}, role={role}"
                )

    if not dry_run:
        conn.commit()

    cur.close()

    log.info(
        f"Committees import complete: "
        f"{total_memberships} memberships across {total_committees_seen} committees "
        f"for {supervisors_processed} supervisors"
    )

    return committee_cache


# ---------------------------------------------------------------------------
# 5. Legislation attribution
# ---------------------------------------------------------------------------

def normalize_status(raw_status: str | None) -> str:
    """
    Normalize MatterStatusName to a standard status label.

    Lowercases and checks against STATUS_LABEL_MAP. Returns "Unknown" if no match.
    """
    if not raw_status:
        return "Unknown"

    lower = raw_status.lower().strip()
    for key, label in STATUS_LABEL_MAP.items():
        if key in lower:
            return label

    return raw_status  # Return raw if no mapping found


def extract_matter_attribution(
    matter: dict,
    histories: list,
    bridge_map: dict,
    verbose: bool = False,
) -> list:
    """
    Try to identify which supervisor(s) are attributable to this matter.

    Attribution sources (in priority order):
    1. matter["MatterRequester"] — direct requester field
    2. MatterHistoryMoverName from histories — motion mover
    3. MatterHistorySeconderName from histories — motion seconder

    Name matching uses RapidFuzz token_sort_ratio against known supervisor names.
    Returns list of matched politician UUIDs. Empty list if no match found.

    Per locked decision: do NOT import matters without attribution.
    """
    supervisor_names = {
        db_id: SUPERVISOR_PERSON_IDS[person_id]
        for person_id, db_id in bridge_map.items()
    }

    def fuzzy_match_to_supervisor(name_text: str) -> list:
        """Match a name string against all known supervisors. Return matching DB UUIDs."""
        if not name_text or not name_text.strip():
            return []

        matched_ids = []
        for db_id, sup_name in supervisor_names.items():
            score = fuzz.token_sort_ratio(name_text.lower(), sup_name.lower())
            if score >= ATTRIBUTION_FUZZY_THRESHOLD:
                matched_ids.append(db_id)
                if verbose:
                    log.info(f"        Attribution match: '{name_text}' -> '{sup_name}' (score={score})")

        return matched_ids

    matched = []

    # Priority 1: MatterRequester
    requester = matter.get("MatterRequester")
    if requester and requester.strip():
        matches = fuzzy_match_to_supervisor(requester)
        if matches:
            matched.extend(matches)

    # Priority 2+3: MatterHistoryMoverName / MatterHistorySeconderName
    if not matched and histories:
        for history in histories:
            mover = history.get("MatterHistoryMoverName")
            if mover and mover.strip() and not matched:
                matches = fuzzy_match_to_supervisor(mover)
                if matches:
                    matched.extend(matches)

            seconder = history.get("MatterHistorySeconderName")
            if seconder and seconder.strip() and not matched:
                matches = fuzzy_match_to_supervisor(seconder)
                if matches:
                    matched.extend(matches)

    # Deduplicate while preserving order
    seen = set()
    unique_matched = []
    for uid in matched:
        if uid not in seen:
            seen.add(uid)
            unique_matched.append(uid)

    return unique_matched


# ---------------------------------------------------------------------------
# 6. Legislation import
# ---------------------------------------------------------------------------

def import_legislation(
    bridge_map: dict,
    session_id: str | None,
    conn,
    dry_run: bool = False,
    verbose: bool = False,
    max_matters: int | None = None,
) -> None:
    """
    Import BOS legislation where a supervisor is identifiable as mover/requester.

    Process:
    - Fetch all BOS matters with pagination ($top=1000)
    - For each matter, fetch histories (for MoverName/SeconderName)
    - Call extract_matter_attribution — skip if no match
    - Upsert LegislativeBill with sponsor_id = first matched supervisor UUID

    Per locked decision: matters without attribution are logged and skipped.
    """
    log.info("Importing BOS legislation ...")

    if not bridge_map:
        log.warning("  No bridge map entries — skipping legislation import")
        return

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    total_fetched = 0
    total_skipped_no_attribution = 0
    total_imported = 0

    skip = 0
    page_size = 1000
    keep_fetching = True

    while keep_fetching:
        log.info(f"  Fetching matters page: skip={skip}, top={page_size} ...")
        matters = fetch_bos_matters(top=page_size, skip=skip)

        if not matters:
            log.info("  No matters returned — pagination complete")
            break

        log.info(f"  Got {len(matters)} matters")

        # Stop pagination if fewer than page_size returned
        if len(matters) < page_size:
            keep_fetching = False

        for matter in matters:
            if max_matters is not None and total_fetched >= max_matters:
                log.info(f"  Reached --max-matters limit ({max_matters}) — stopping")
                keep_fetching = False
                break

            total_fetched += 1
            matter_id = matter.get("MatterId")
            matter_title = matter.get("MatterTitle", "")
            matter_file = matter.get("MatterFile")
            matter_status = matter.get("MatterStatusName")
            matter_intro_date = matter.get("MatterIntroDate")
            matter_type_name = matter.get("MatterTypeName", "")

            if verbose:
                log.info(f"  Matter {matter_id}: {matter_file} — {matter_title[:60]}")

            # Fetch histories for attribution
            histories = fetch_matter_histories(matter_id)

            # Extract attribution
            matched_ids = extract_matter_attribution(
                matter, histories, bridge_map, verbose=verbose
            )

            if not matched_ids:
                total_skipped_no_attribution += 1
                if verbose:
                    log.info(f"    SKIP: no supervisor attribution found")
                continue

            # First matched supervisor is the primary sponsor
            sponsor_id = matched_ids[0]
            total_imported += 1

            # Parse introduced date
            intro_dt = parse_legistar_date(matter_intro_date)

            # Normalize status
            raw_status = matter_status or ""
            status_label = normalize_status(raw_status)

            # Build matter number (prefer MatterFile, fall back to MatterId)
            number = matter_file if matter_file else str(matter_id)

            # Build title (include type prefix if available and not redundant)
            title = matter_title or f"Matter {matter_id}"

            # Build summary from status + type context
            summary_parts = []
            if matter_type_name:
                summary_parts.append(matter_type_name)
            if raw_status:
                summary_parts.append(raw_status)
            summary = " — ".join(summary_parts) if summary_parts else raw_status

            # Legistar web URL
            url = f"{LEGISTAR_WEB_BASE}?ID={matter_id}"

            external_id = f"legistar-{matter_id}"

            if verbose:
                log.info(
                    f"    IMPORT: {number} | sponsor={sponsor_id[:8]}... | "
                    f"status={status_label} | intro={intro_dt}"
                )

            if not dry_run:
                cur.execute(
                    """
                    INSERT INTO essentials.legislative_bills
                        (id, session_id, external_id, jurisdiction, number, title, summary,
                         raw_status, status_label, sponsor_id, introduced_at, url, source)
                    VALUES
                        (gen_random_uuid(), %s::uuid, %s, %s, %s, %s, %s,
                         %s, %s, %s::uuid, %s, %s, 'legistar')
                    ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        raw_status = EXCLUDED.raw_status,
                        status_label = EXCLUDED.status_label,
                        sponsor_id = EXCLUDED.sponsor_id,
                        introduced_at = EXCLUDED.introduced_at,
                        url = EXCLUDED.url
                    """,
                    (
                        session_id,
                        external_id,
                        JURISDICTION,
                        number,
                        title,
                        summary,
                        raw_status,
                        status_label,
                        sponsor_id,
                        intro_dt,
                        url,
                    ),
                )
            else:
                log.info(
                    f"    [dry-run] Would upsert bill: external_id={external_id}, "
                    f"number={number}, status={status_label}"
                )

        if not dry_run:
            conn.commit()
            log.info(f"  Committed batch (skip={skip})")

        skip += page_size

    cur.close()

    attribution_rate = (
        round(total_imported / total_fetched * 100, 1) if total_fetched > 0 else 0
    )

    log.info(
        f"Legislation import complete: "
        f"{total_imported} imported, {total_skipped_no_attribution} skipped (no attribution)"
    )
    log.info(
        f"Attribution rate: {attribution_rate}% of {total_fetched} matters "
        f"({total_imported} with supervisor attribution)"
    )

    if total_fetched > 0 and attribution_rate < 5:
        log.info(
            "NOTE: Low attribution rate (<5%) is expected — MatterRequester is NULL for ~98% "
            "of recent BOS matters, and MoverName is only populated for pre-2010 data. "
            "This is a known data gap documented in FEASIBILITY_LOCAL_DATA.md."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "LA County Board of Supervisors Data Importer (Legistar REST API). "
            "Fetches committee assignments and legislation metadata for the 5 current supervisors."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read API data but do not write to database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed API responses and match results",
    )
    parser.add_argument(
        "--committees-only",
        action="store_true",
        help="Only import committees and memberships (skip legislation)",
    )
    parser.add_argument(
        "--legislation-only",
        action="store_true",
        help="Only import legislation (assumes bridge table already built)",
    )
    parser.add_argument(
        "--max-matters",
        type=int,
        default=None,
        metavar="N",
        help="Limit number of matters processed (for testing)",
    )
    args = parser.parse_args()

    if args.committees_only and args.legislation_only:
        parser.error("Cannot use --committees-only and --legislation-only together")

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"LA County BOS Importer v{SCRIPT_VERSION} starting ...")
    log.info(f"Dry-run: {args.dry_run}")
    log.info(f"Mode: {'committees only' if args.committees_only else 'legislation only' if args.legislation_only else 'full import'}")
    if args.max_matters:
        log.info(f"Max matters: {args.max_matters}")

    # Connect to database
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL environment variable not set")
        log.error("Create EV-Backend/.env.local with DATABASE_URL=postgresql://...")
        sys.exit(1)

    if args.dry_run:
        log.info("Dry-run mode: connecting to DB for bridge building only (no writes)")
        # In dry-run, we still connect for reading politician names
        try:
            conn = psycopg2.connect(database_url)
            log.info("DB connection successful")
        except Exception as e:
            log.error(f"DB connection failed: {e}")
            sys.exit(1)
    else:
        try:
            conn = psycopg2.connect(database_url)
            log.info("DB connection successful")
        except Exception as e:
            log.error(f"DB connection failed: {e}")
            sys.exit(1)

    try:
        # Step 1: Build bridge table (always run unless --legislation-only)
        if not args.legislation_only:
            bridge_map = build_supervisor_bridge(conn, dry_run=args.dry_run, verbose=args.verbose)
        else:
            # Load existing bridge from DB
            log.info("Loading existing bridge from DB (--legislation-only mode) ...")
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT id_value::int as person_id, politician_id::text
                FROM essentials.legislative_politician_id_map
                WHERE id_type = 'legistar' AND source = 'legistar-lacounty'
                """
            )
            rows = cur.fetchall()
            cur.close()
            bridge_map = {row["person_id"]: row["politician_id"] for row in rows}
            log.info(f"  Loaded {len(bridge_map)} bridge entries from DB")

        if not bridge_map:
            log.error("No supervisors matched in bridge table — cannot continue")
            sys.exit(1)

        log.info(f"Bridge map: {len(bridge_map)} supervisors")
        for person_id, db_id in bridge_map.items():
            log.info(f"  PersonId={person_id} ({SUPERVISOR_PERSON_IDS.get(person_id, '?')}) -> {db_id}")

        # Step 2: Ensure session
        session_id = ensure_session(conn, dry_run=args.dry_run)

        # Step 3: Fetch active bodies (for committee type lookup)
        log.info("Fetching active bodies from Legistar ...")
        active_bodies = fetch_active_bodies()

        # Step 4: Import committees
        if not args.legislation_only:
            committee_cache = import_committees(
                bridge_map=bridge_map,
                session_id=session_id,
                conn=conn,
                active_bodies=active_bodies,
                dry_run=args.dry_run,
                verbose=args.verbose,
            )
            log.info(f"Committee cache populated: {len(committee_cache)} committees")

        # Step 5: Import legislation
        if not args.committees_only:
            import_legislation(
                bridge_map=bridge_map,
                session_id=session_id,
                conn=conn,
                dry_run=args.dry_run,
                verbose=args.verbose,
                max_matters=args.max_matters,
            )

        log.info("Import complete.")

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
