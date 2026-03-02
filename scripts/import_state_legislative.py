#!/usr/bin/env python3
"""
State Legislative Data Importer (LegiScan API)

Imports Indiana (IN) and California (CA) state legislative data into the
essentials.legislative_* tables:
  - Sessions
  - Legislator bridge (essentials.politicians <-> LegiScan people_id)
  - Bills (with inline vote import to avoid double getBill calls)
  - Committees (extracted from getBill referral fields)
  - Committee memberships (from getSessionPeople committee_id field)

Usage:
    python import_state_legislative.py --state IN --sessions current,previous --dry-run --verbose
    python import_state_legislative.py --state IN --sessions current,previous
    python import_state_legislative.py --state CA --sessions current

Environment variables (loaded from EV-Backend/.env.local or shell):
    LEGISCAN_API_KEY   LegiScan API key
    DATABASE_URL       PostgreSQL connection string
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Load .env.local from EV-Backend root (parent of scripts/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env.local")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEGISCAN_BASE = "https://api.legiscan.com/"
BUDGET_LIMIT = 30000
COUNTER_PATH = Path.home() / ".ev-backend" / "legiscan_counter.json"

# State session configurations
STATE_SESSIONS = {
    "IN": {
        "name": "Indiana",
        "jurisdiction": "indiana",
        "current_year_start": 2026,
        "previous_year_start": 2025,
    },
    "CA": {
        "name": "California",
        "jurisdiction": "california",
        "current_year_start": 2025,
        "previous_year_start": 2023,
    },
}

# LegiScan status integer -> label mapping (matches federal normalizeBillStatus)
STATUS_MAP = {
    1: "Introduced",
    2: "In Committee",   # Engrossed ~ committee action
    3: "Passed",         # Enrolled ~ passed both chambers
    4: "Passed",
    5: "Vetoed",
    6: "Failed",
    7: "Passed",         # Veto override = passed
    8: "Signed",         # Chaptered = signed into law
}

# ---------------------------------------------------------------------------
# LegiScan budget counter
# Shared with Go LegiScan client: ~/.ev-backend/legiscan_counter.json
# Format: {"month": "2026-03", "queries": 1234}
# ---------------------------------------------------------------------------

def read_budget():
    """Read LegiScan monthly budget counter. Returns queries used this month."""
    if not COUNTER_PATH.exists():
        return 0
    with open(COUNTER_PATH) as f:
        data = json.load(f)
    if data.get("month") != datetime.now().strftime("%Y-%m"):
        return 0  # New month, counter resets
    return data.get("queries", 0)


def increment_budget(count=1):
    """Atomically increment the LegiScan budget counter.

    Uses write-to-temp + os.rename() for crash safety, matching the Go
    implementation's atomic rename pattern.
    """
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

    # Atomic write: write to temp file in same directory, then rename
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


# ---------------------------------------------------------------------------
# LegiScan API wrapper
# ---------------------------------------------------------------------------

def legiscan_query(api_key, op, params=None):
    """Call LegiScan API. Checks budget, increments counter. Returns parsed JSON.

    Raises RuntimeError if budget is nearly exhausted or the API returns
    a non-OK status.
    """
    budget_used = read_budget()
    if budget_used >= BUDGET_LIMIT - 100:
        raise RuntimeError(
            f"LegiScan budget nearly exhausted: {budget_used}/{BUDGET_LIMIT}"
        )

    req_params = {"key": api_key, "op": op}
    if params:
        req_params.update(params)

    resp = requests.get(LEGISCAN_BASE, params=req_params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    increment_budget(1)

    if data.get("status") != "OK":
        raise RuntimeError(f"LegiScan {op} error: {data}")

    return data


# ---------------------------------------------------------------------------
# Session discovery + upsert
# ---------------------------------------------------------------------------

def find_session_id(api_key, state_code, year_start):
    """Find LegiScan session_id for the given state and year_start.

    Returns (session_id, year_end) or (None, None) if not found.
    Only matches regular sessions (special == 0).
    """
    data = legiscan_query(api_key, "getSessionList", {"state": state_code})
    for session in data.get("sessions", []):
        if session.get("year_start") == year_start and session.get("special") == 0:
            return session["session_id"], session.get("year_end", year_start)
    return None, None


def get_or_create_session(conn, jurisdiction, name, external_id, is_current):
    """Create or retrieve a legislative_sessions row. Returns the UUID.

    Looks up by (jurisdiction, external_id). If not found, inserts a new row
    and returns the generated UUID.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id FROM essentials.legislative_sessions
        WHERE jurisdiction = %s AND external_id = %s
        """,
        (jurisdiction, str(external_id)),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO essentials.legislative_sessions
            (id, jurisdiction, name, external_id, is_current, source)
        VALUES (gen_random_uuid(), %s, %s, %s, %s, 'legiscan')
        RETURNING id
        """,
        (jurisdiction, name, str(external_id), is_current),
    )
    session_id = cur.fetchone()[0]
    conn.commit()
    return session_id


# ---------------------------------------------------------------------------
# Legislator bridge builder
# ---------------------------------------------------------------------------

def build_legislator_bridge(api_key, legiscan_session_id, state_code, jurisdiction, conn, dry_run=False):
    """Match LegiScan session people to essentials.politicians.

    Matching strategy: exact first + last name match against politicians
    whose offices point to STATE_UPPER / STATE_LOWER / STATE_EXEC districts.

    Single-match-only guard: if 0 or 2+ politicians match the same name,
    the person is skipped (logged, not errored). This prevents incorrect
    bridge inserts.

    Bridge rows use id_type='legiscan' and source='legiscan-state-people'.

    Returns dict {people_id (int): politician_uuid (str)}.
    """
    data = legiscan_query(api_key, "getSessionPeople", {"id": str(legiscan_session_id)})
    people = data.get("sessionpeople", {}).get("people", [])

    cur = conn.cursor()
    bridge_map = {}
    matched = 0
    skipped_ambiguous = 0
    skipped_none = 0
    already_bridged = 0

    for person in people:
        people_id = person["people_id"]
        first_name = person.get("first_name", "").strip()
        last_name = person.get("last_name", "").strip()

        if not first_name or not last_name:
            skipped_none += 1
            continue

        # Check if bridge already exists from a previous run
        cur.execute(
            """
            SELECT politician_id FROM essentials.legislative_politician_id_map
            WHERE id_type = 'legiscan' AND id_value = %s
            """,
            (str(people_id),),
        )
        existing = cur.fetchone()
        if existing:
            bridge_map[people_id] = existing[0]
            already_bridged += 1
            continue

        # Match by name + state district type (single match only — ambiguous = skip)
        cur.execute(
            """
            SELECT DISTINCT p.id FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE LOWER(p.last_name) = LOWER(%s)
              AND LOWER(p.first_name) = LOWER(%s)
              AND d.district_type IN ('STATE_UPPER', 'STATE_LOWER', 'STATE_EXEC')
            """,
            (last_name, first_name),
        )
        matches = cur.fetchall()

        if len(matches) == 1:
            politician_id = matches[0][0]
            bridge_map[people_id] = politician_id
            matched += 1
            logging.debug(f"  Matched: {first_name} {last_name} -> {politician_id}")
            if not dry_run:
                cur.execute(
                    """
                    INSERT INTO essentials.legislative_politician_id_map
                        (id, politician_id, id_type, id_value, verified_at, source)
                    VALUES (gen_random_uuid(), %s, 'legiscan', %s, NOW(), 'legiscan-state-people')
                    ON CONFLICT DO NOTHING
                    """,
                    (politician_id, str(people_id)),
                )
        elif len(matches) == 0:
            skipped_none += 1
            logging.debug(f"  No match for {first_name} {last_name} ({state_code})")
        else:
            skipped_ambiguous += 1
            logging.warning(
                f"  Ambiguous: {first_name} {last_name} ({state_code}) — "
                f"{len(matches)} candidates, skipping"
            )

    if not dry_run:
        conn.commit()

    logging.info(
        f"Bridge results: {matched} new matches, {already_bridged} existing, "
        f"{skipped_none} no match, {skipped_ambiguous} ambiguous"
    )
    return bridge_map


# ---------------------------------------------------------------------------
# Bill status + vote position normalization
# ---------------------------------------------------------------------------

def normalize_bill_status(status_int, last_action=""):
    """Convert LegiScan status integer (1-8) to label.

    Falls back to last_action text analysis when status_int is missing or
    unrecognized, mirroring the Go normalizeBillStatus logic.
    """
    if isinstance(status_int, int) and status_int in STATUS_MAP:
        return STATUS_MAP[status_int]

    # Text-based fallback
    t = last_action.lower()
    if "signed" in t or "chaptered" in t or "public law" in t:
        return "Signed"
    if "passed" in t or "enrolled" in t:
        return "Passed"
    if "reported" in t or "engrossed" in t:
        return "In Committee"
    if "referred" in t or "committee" in t:
        return "In Committee"
    if "vetoed" in t:
        return "Vetoed"
    if "failed" in t or "dead" in t:
        return "Failed"
    return "Introduced"


def normalize_vote_cast(vote_text):
    """Normalize LegiScan vote_text string to the position enum value."""
    mapping = {
        "yea": "yea",
        "aye": "yea",
        "yes": "yea",
        "nay": "nay",
        "no": "nay",
        "nv": "not_voting",
        "not voting": "not_voting",
        "absent": "absent",
        "present": "present",
    }
    return mapping.get(vote_text.lower().strip(), "not_voting")


# ---------------------------------------------------------------------------
# Committee upsert helpers
# ---------------------------------------------------------------------------

def upsert_committee(cur, committee_data, jurisdiction, session_id, dry_run=False):
    """Upsert a single committee to legislative_committees. Returns DB UUID or None."""
    ext_id = str(committee_data.get("committee_id", ""))
    if not ext_id or ext_id == "0":
        return None

    name = committee_data.get("name", "Unknown Committee")
    chamber_raw = committee_data.get("chamber", "").upper()
    chamber = {"H": "house", "S": "senate"}.get(chamber_raw, "joint")

    if dry_run:
        logging.info(f"  DRY RUN: would upsert committee {ext_id}: {name} ({chamber})")
        return None

    cur.execute(
        """
        INSERT INTO essentials.legislative_committees
            (id, session_id, external_id, jurisdiction, name, type, chamber, is_current, source)
        VALUES (gen_random_uuid(), %s, %s, %s, %s, 'committee', %s, true, 'legiscan')
        ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
            name = EXCLUDED.name,
            chamber = EXCLUDED.chamber,
            session_id = EXCLUDED.session_id
        RETURNING id
        """,
        (str(session_id), ext_id, jurisdiction, name, chamber),
    )
    row = cur.fetchone()
    return row[0] if row else None


def extract_and_upsert_committees(conn, bill_data, jurisdiction, session_id, dry_run=False):
    """Extract committees from a bill's committee + referrals fields. Upsert all.

    Returns {legiscan_committee_id (int): db_uuid (str)}.
    """
    cur = conn.cursor()
    committee_map = {}

    # Current assigned committee
    committee = bill_data.get("committee", {})
    if committee and committee.get("committee_id"):
        db_id = upsert_committee(cur, committee, jurisdiction, session_id, dry_run)
        if db_id:
            committee_map[committee["committee_id"]] = db_id

    # Referral history (bill may have been sent to multiple committees)
    for ref in bill_data.get("referrals", []):
        cid = ref.get("committee_id")
        if cid and cid not in committee_map:
            db_id = upsert_committee(cur, ref, jurisdiction, session_id, dry_run)
            if db_id:
                committee_map[cid] = db_id

    if not dry_run:
        conn.commit()
    return committee_map


# ---------------------------------------------------------------------------
# Committee memberships (from getSessionPeople)
# ---------------------------------------------------------------------------

def upsert_committee_memberships_from_people(
    conn, people, bridge_map, jurisdiction, session_id, committee_db_map, dry_run=False
):
    """Create committee membership rows using the committee_id field from getSessionPeople.

    Each person entry may have a primary committee_id and committee_sponsor flag.
    Uses congress_number=0 for all state-level memberships (required by the
    unique index idx_cmember on (committee_id, politician_id, congress_number)).

    Returns count of memberships created/updated.
    """
    cur = conn.cursor()
    memberships_created = 0

    for person in people:
        people_id = person["people_id"]
        committee_id = person.get("committee_id")
        committee_sponsor = person.get("committee_sponsor", 0)

        if not committee_id or people_id not in bridge_map:
            continue

        if committee_id not in committee_db_map:
            logging.debug(
                f"  Membership skip: committee {committee_id} not in DB map "
                f"(may not have appeared in a bill yet)"
            )
            continue

        politician_id = bridge_map[people_id]
        db_committee_id = committee_db_map[committee_id]
        role = "chair" if committee_sponsor else "member"

        if dry_run:
            logging.info(
                f"  DRY RUN: would create membership people_id={people_id} "
                f"on committee {committee_id} role={role}"
            )
            continue

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
            (str(db_committee_id), str(politician_id), role, str(session_id)),
        )
        memberships_created += 1

    if not dry_run:
        conn.commit()

    logging.info(f"Committee memberships created/updated: {memberships_created}")
    return memberships_created


# ---------------------------------------------------------------------------
# Sponsor linking
# ---------------------------------------------------------------------------

def link_sponsors_to_bill(cur, sponsors, bridge_map, bill_db_id, dry_run=False):
    """Link sponsors from getBill response to the bill.

    sponsor_order == 1 is the primary sponsor (sets bill.sponsor_id).
    All others go to legislative_bill_cosponsors.

    Returns (primary_sponsor_id, cosponsors_linked_count).
    """
    primary_sponsor_id = None
    cosponsors_linked = 0

    for sponsor in sponsors:
        people_id = sponsor.get("people_id")
        order = sponsor.get("sponsor_order", 99)

        if people_id not in bridge_map:
            continue

        politician_id = bridge_map[people_id]

        if order == 1:
            primary_sponsor_id = politician_id
        else:
            if not dry_run:
                cur.execute(
                    """
                    INSERT INTO essentials.legislative_bill_cosponsors
                        (id, bill_id, politician_id)
                    VALUES (gen_random_uuid(), %s, %s)
                    ON CONFLICT (bill_id, politician_id) DO NOTHING
                    """,
                    (str(bill_db_id), str(politician_id)),
                )
                cosponsors_linked += 1

    return primary_sponsor_id, cosponsors_linked


# ---------------------------------------------------------------------------
# Vote import (inline per bill)
# ---------------------------------------------------------------------------

def import_votes_for_bill(
    api_key, conn, bill_data, bill_db_id, session_db_id, jurisdiction, bridge_map, dry_run=False
):
    """Import votes for a single bill. Called inline from import_bills.

    Fetches each roll_call_id listed in the bill's votes array, then upserts
    individual member votes. Only members in bridge_map are included.

    Returns count of vote rows upserted.
    """
    cur = conn.cursor()
    votes_list = bill_data.get("votes", [])
    votes_upserted = 0

    for vote_stub in votes_list:
        roll_call_id = vote_stub.get("roll_call_id")
        if not roll_call_id:
            continue

        try:
            rc_data = legiscan_query(api_key, "getRollCall", {"id": str(roll_call_id)})
            roll_call = rc_data.get("roll_call", {})

            vote_date_str = roll_call.get("date", "")
            vote_date = None
            if vote_date_str:
                try:
                    vote_date = datetime.strptime(vote_date_str, "%Y-%m-%d").date()
                except ValueError:
                    vote_date = datetime.now().date()

            vote_question = roll_call.get("desc", "Vote")
            result = "passed" if roll_call.get("passed", 0) == 1 else "failed"
            yea_count = roll_call.get("yea", 0)
            nay_count = roll_call.get("nay", 0)
            external_vote_id = f"legiscan-{roll_call_id}"

            for member_vote in roll_call.get("votes", []):
                people_id = member_vote.get("people_id")
                if people_id not in bridge_map:
                    continue

                politician_id = bridge_map[people_id]
                position = normalize_vote_cast(member_vote.get("vote_text", ""))

                if dry_run:
                    continue

                cur.execute(
                    """
                    INSERT INTO essentials.legislative_votes
                        (id, politician_id, bill_id, session_id, external_vote_id,
                         vote_question, position, vote_date, result, yea_count, nay_count, source)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, 'legiscan')
                    ON CONFLICT (politician_id, bill_id, session_id, external_vote_id) DO UPDATE SET
                        position = EXCLUDED.position,
                        vote_question = EXCLUDED.vote_question,
                        result = EXCLUDED.result
                    """,
                    (
                        str(politician_id),
                        str(bill_db_id) if bill_db_id else None,
                        str(session_db_id),
                        external_vote_id,
                        vote_question,
                        position,
                        vote_date,
                        result,
                        yea_count,
                        nay_count,
                    ),
                )
                votes_upserted += 1

            if not dry_run:
                conn.commit()

        except Exception as e:
            logging.error(f"  Error importing roll call {roll_call_id}: {e}")
            conn.rollback()

    return votes_upserted


# ---------------------------------------------------------------------------
# Bill import (main loop — includes inline votes and committee extraction)
# ---------------------------------------------------------------------------

def import_bills(
    api_key, conn, legiscan_session_id, session_db_id, jurisdiction, bridge_map, dry_run=False
):
    """Import all bills for a session with inline vote and committee extraction.

    CRITICAL: getMasterList returns a MAP (dict with string keys), not an array.
    Key "0" is session metadata — always skipped. Keys "1".."N" are bill stubs.

    Returns (bills_upserted, cosponsors_total, votes_total, committee_db_map, errors).
    """
    cur = conn.cursor()

    # CRITICAL: getMasterList is a MAP not a list — parse as dict, skip key "0"
    data = legiscan_query(api_key, "getMasterList", {"id": str(legiscan_session_id)})
    masterlist = data.get("masterlist", {})
    bills = [v for k, v in masterlist.items() if k != "0"]

    logging.info(f"Found {len(bills)} bills in session {legiscan_session_id}")

    bills_upserted = 0
    cosponsors_total = 0
    votes_total = 0
    committees_total = 0
    all_committee_db_map = {}  # LegiScan committee_id (int) -> DB UUID (str)
    errors = []

    for i, bill_stub in enumerate(bills):
        bill_id = bill_stub.get("bill_id")
        if not bill_id:
            continue

        try:
            # Fetch full bill details (includes sponsors, committee, referrals, votes)
            bill_data_resp = legiscan_query(api_key, "getBill", {"id": str(bill_id)})
            bill_data = bill_data_resp.get("bill", {})

            # Extract and upsert committees from this bill
            committee_map = extract_and_upsert_committees(
                conn, bill_data, jurisdiction, session_db_id, dry_run
            )
            all_committee_db_map.update(committee_map)
            committees_total += len(committee_map)

            # Normalize bill fields
            number = bill_data.get("number", bill_stub.get("number", ""))
            title = bill_data.get("title", bill_stub.get("title", ""))
            status_int = bill_data.get("status", bill_stub.get("status", 1))
            last_action = bill_data.get("status_desc", bill_stub.get("last_action", ""))
            status_label = normalize_bill_status(status_int, last_action)
            url = bill_data.get("url", "")
            external_id = f"legiscan-{bill_id}"

            # Parse introduced date from history (first action = introduction)
            introduced_at = None
            history = bill_data.get("history", [])
            if history:
                first_date = history[0].get("date", "")
                if first_date:
                    try:
                        introduced_at = datetime.strptime(first_date, "%Y-%m-%d").date()
                    except ValueError:
                        pass

            if dry_run:
                logging.info(
                    f"  DRY RUN: would upsert bill {external_id}: "
                    f"{number} - {title[:60]}"
                )
                bills_upserted += 1
                continue

            # Upsert bill (without sponsor_id initially — set after sponsor linking)
            cur.execute(
                """
                INSERT INTO essentials.legislative_bills
                    (id, session_id, external_id, jurisdiction, number, title, summary,
                     raw_status, status_label, introduced_at, url, source)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, '',
                        %s, %s, %s, %s, 'legiscan')
                ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
                    title = EXCLUDED.title,
                    raw_status = EXCLUDED.raw_status,
                    status_label = EXCLUDED.status_label,
                    url = EXCLUDED.url
                RETURNING id
                """,
                (
                    str(session_db_id),
                    external_id,
                    jurisdiction,
                    number,
                    title,
                    str(status_int),
                    status_label,
                    introduced_at,
                    url,
                ),
            )
            bill_db_id = cur.fetchone()[0]

            # Link sponsors: sponsor_order=1 -> sponsor_id, rest -> cosponsors table
            sponsors = bill_data.get("sponsors", [])
            primary_sponsor_id, cosponsors = link_sponsors_to_bill(
                cur, sponsors, bridge_map, bill_db_id, dry_run
            )
            cosponsors_total += cosponsors

            if primary_sponsor_id:
                cur.execute(
                    "UPDATE essentials.legislative_bills SET sponsor_id = %s WHERE id = %s",
                    (str(primary_sponsor_id), str(bill_db_id)),
                )

            conn.commit()
            bills_upserted += 1

            # Import votes inline (avoids a second getBill call per bill)
            votes_for_bill = import_votes_for_bill(
                api_key, conn, bill_data, bill_db_id, session_db_id,
                jurisdiction, bridge_map, dry_run
            )
            votes_total += votes_for_bill

            # Progress logging every 100 bills
            if (i + 1) % 100 == 0:
                budget_used = read_budget()
                logging.info(
                    f"  Progress: {i + 1}/{len(bills)} bills processed. "
                    f"Budget: {budget_used}/{BUDGET_LIMIT}"
                )
                if budget_used >= BUDGET_LIMIT - 500:
                    logging.warning(
                        f"  Budget low ({budget_used}/{BUDGET_LIMIT}) — "
                        "stopping bill import early"
                    )
                    break

        except Exception as e:
            errors.append(f"Bill {bill_id}: {e}")
            logging.error(f"  Error importing bill {bill_id}: {e}")
            conn.rollback()
            if len(errors) > 50:
                logging.error("Too many errors — aborting bill import")
                break

    logging.info(
        f"Bills: {bills_upserted} upserted, {cosponsors_total} cosponsors, "
        f"{votes_total} votes, {committees_total} committees, {len(errors)} errors"
    )
    return bills_upserted, cosponsors_total, votes_total, all_committee_db_map, errors


# ---------------------------------------------------------------------------
# Main state import orchestrator
# ---------------------------------------------------------------------------

def import_state(api_key, conn, state_code, sessions_to_import, dry_run=False):
    """Import all legislative data for a state (sessions, bridge, bills, votes, committees).

    sessions_to_import: list containing "current" and/or "previous".
    Returns a results summary dict.
    """
    config = STATE_SESSIONS[state_code]
    jurisdiction = config["jurisdiction"]
    results = {
        "bridge_rows": 0,
        "bills": 0,
        "votes": 0,
        "cosponsors": 0,
        "committees": 0,
        "errors": [],
    }

    session_targets = []
    if "current" in sessions_to_import:
        session_targets.append(("current", config["current_year_start"]))
    if "previous" in sessions_to_import:
        session_targets.append(("previous", config["previous_year_start"]))

    all_bridge_map = {}
    all_committee_db_map = {}

    for label, year_start in session_targets:
        logging.info(f"\n{'=' * 60}")
        logging.info(f"Processing {config['name']} {label} session (year_start={year_start})")
        logging.info(f"{'=' * 60}")

        # 1. Find LegiScan session ID
        legiscan_session_id, year_end = find_session_id(api_key, state_code, year_start)
        if not legiscan_session_id:
            msg = f"No session found for {state_code} year_start={year_start}"
            logging.error(msg)
            results["errors"].append(msg)
            continue
        logging.info(f"Found session ID: {legiscan_session_id}")

        # 2. Create/get DB session row
        if year_end and year_end != year_start:
            session_name = f"{year_start}-{year_end} {config['name']} Regular Session"
        else:
            session_name = f"{year_start} {config['name']} Regular Session"
        is_current = label == "current"
        session_db_id = get_or_create_session(
            conn, jurisdiction, session_name, legiscan_session_id, is_current
        )
        logging.info(f"Session DB ID: {session_db_id}")

        # 3. Build legislator bridge (LegiScan people_id -> politicians UUID)
        logging.info("Building legislator bridge...")
        bridge_map = build_legislator_bridge(
            api_key, legiscan_session_id, state_code, jurisdiction, conn, dry_run
        )
        all_bridge_map.update(bridge_map)
        results["bridge_rows"] += len(bridge_map)
        logging.info(f"Bridge: {len(bridge_map)} legislators matched")

        if not bridge_map:
            logging.warning(
                f"No legislators matched for {state_code} session {legiscan_session_id} "
                "— skipping bill/vote import"
            )
            continue

        # 4. Import bills (includes inline vote import and committee extraction)
        logging.info("Importing bills, votes, and committees...")
        bills, cosponsors, votes, committee_db_map, errors = import_bills(
            api_key, conn, legiscan_session_id, session_db_id,
            jurisdiction, bridge_map, dry_run
        )
        all_committee_db_map.update(committee_db_map)
        results["bills"] += bills
        results["cosponsors"] += cosponsors
        results["votes"] += votes
        results["committees"] += len(committee_db_map)
        results["errors"].extend(errors)

        # 5. Create committee memberships from getSessionPeople committee_id field
        logging.info("Creating committee memberships from people data...")
        # Re-fetch session people to get committee_id assignments
        # (already fetched during bridge build, but stored locally here to avoid re-counting budget)
        people_data = legiscan_query(
            api_key, "getSessionPeople", {"id": str(legiscan_session_id)}
        )
        people = people_data.get("sessionpeople", {}).get("people", [])
        upsert_committee_memberships_from_people(
            conn, people, bridge_map, jurisdiction, session_db_id,
            all_committee_db_map, dry_run
        )

        budget_used = read_budget()
        logging.info(f"Session complete. Budget: {budget_used}/{BUDGET_LIMIT}")

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Import state legislative data from LegiScan API"
    )
    parser.add_argument(
        "--state", required=True, choices=list(STATE_SESSIONS.keys()),
        help="State code (IN or CA)"
    )
    parser.add_argument(
        "--sessions", default="current,previous",
        help="Comma-separated session labels: current,previous (default: both)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log projected operations without writing to the database"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug-level logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    api_key = os.environ.get("LEGISCAN_API_KEY")
    db_url = os.environ.get("DATABASE_URL")

    if not api_key:
        logging.error("LEGISCAN_API_KEY environment variable required")
        sys.exit(1)
    if not db_url:
        logging.error("DATABASE_URL environment variable required")
        sys.exit(1)

    # Budget check before starting
    budget_used = read_budget()
    remaining = BUDGET_LIMIT - budget_used
    logging.info(f"LegiScan budget: {budget_used}/{BUDGET_LIMIT} used, {remaining} remaining")
    if remaining < 2000:
        logging.warning(
            f"Budget low ({remaining} remaining). "
            "Consider running with a single session only."
        )

    sessions_to_import = [s.strip() for s in args.sessions.split(",")]
    invalid = [s for s in sessions_to_import if s not in ("current", "previous")]
    if invalid:
        logging.error(f"Unknown session labels: {invalid}. Use 'current' and/or 'previous'.")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    try:
        results = import_state(api_key, conn, args.state, sessions_to_import, args.dry_run)

        logging.info(f"\n{'=' * 60}")
        logging.info(f"IMPORT COMPLETE: {args.state}")
        logging.info(f"  Bridge rows:  {results['bridge_rows']}")
        logging.info(f"  Bills:        {results['bills']}")
        logging.info(f"  Cosponsors:   {results['cosponsors']}")
        logging.info(f"  Votes:        {results['votes']}")
        logging.info(f"  Committees:   {results['committees']}")
        logging.info(f"  Errors:       {len(results['errors'])}")
        if results["errors"]:
            logging.error("First errors:")
            for e in results["errors"][:10]:
                logging.error(f"  - {e}")
        logging.info(f"{'=' * 60}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
