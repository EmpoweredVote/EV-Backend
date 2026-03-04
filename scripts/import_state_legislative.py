#!/usr/bin/env python3
"""
State Legislative Data Importer (LegiScan API) — Dataset Edition

Downloads entire legislative sessions as ZIP archives via LegiScan's Dataset API,
reducing API usage from thousands of individual getBill/getRollCall calls to just
3-5 queries per state. This is the recommended approach per LegiScan guidelines.

Dataset flow:
  1. getDatasetList -> find sessions with their dataset_hash + access_key
  2. getDataset -> download Base64-encoded ZIP containing all bill/vote/people JSON
  3. getSessionPeople -> match legislators to our politicians table + get committee data
  4. Process bills, votes, committees from extracted ZIP (0 additional API calls)

Budget impact: ~5 queries per state (2 sessions) vs ~2,000-15,000 with individual calls.

Usage:
    python import_state_legislative.py --state IN --sessions current,previous --dry-run --verbose
    python import_state_legislative.py --state IN --sessions current,previous
    python import_state_legislative.py --state CA --sessions current

Environment variables (loaded from EV-Backend/.env.local or shell):
    LEGISCAN_API_KEY   LegiScan API key
    DATABASE_URL       PostgreSQL connection string
"""

import argparse
import base64
import io
import json
import logging
import os
import sys
import tempfile
import zipfile
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
DATASET_CACHE_DIR = Path.home() / ".ev-backend" / "datasets"
DATASET_HASH_PATH = Path.home() / ".ev-backend" / "dataset_hashes.json"
IMPORT_TRACKER_PATH = Path.home() / ".ev-backend" / "legiscan_import_tracker.json"

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

# Common nickname <-> formal name mappings for legislator matching.
# Each entry maps both directions: "dave" matches "david" and vice versa.
NICKNAME_GROUPS = [
    ("bill", "william"),
    ("bob", "robert"),
    ("bobby", "robert"),
    ("rob", "robert"),
    ("jim", "james"),
    ("jimmy", "james"),
    ("joe", "joseph"),
    ("mike", "michael"),
    ("tom", "thomas"),
    ("dick", "richard"),
    ("rick", "richard"),
    ("rich", "richard"),
    ("ron", "ronald"),
    ("dan", "daniel"),
    ("danny", "daniel"),
    ("ed", "edward"),
    ("ted", "theodore"),
    ("ted", "edward"),
    ("pat", "patricia"),
    ("pat", "patrick"),
    ("chris", "christopher"),
    ("chris", "christine"),
    ("beth", "elizabeth"),
    ("liz", "elizabeth"),
    ("sue", "susan"),
    ("barb", "barbara"),
    ("cathy", "catherine"),
    ("kathy", "katherine"),
    ("jeff", "jeffrey"),
    ("steve", "steven"),
    ("steve", "stephen"),
    ("tony", "anthony"),
    ("matt", "matthew"),
    ("andy", "andrew"),
    ("greg", "gregory"),
    ("phil", "philip"),
    ("larry", "lawrence"),
    ("jerry", "gerald"),
    ("terry", "terrence"),
    ("peggy", "margaret"),
    ("chuck", "charles"),
    ("charlie", "charles"),
    ("jack", "john"),
    ("will", "william"),
    ("sam", "samuel"),
    ("ben", "benjamin"),
    ("tim", "timothy"),
    ("ken", "kenneth"),
    ("don", "donald"),
    ("dave", "david"),
    ("doug", "douglas"),
    ("al", "albert"),
    ("al", "alan"),
    ("alex", "alexander"),
    ("nick", "nicholas"),
    ("nate", "nathaniel"),
    ("nate", "nathan"),
    ("fred", "frederick"),
    ("frank", "francis"),
    ("frank", "franklin"),
    ("hank", "henry"),
    ("ray", "raymond"),
    ("walt", "walter"),
    ("wes", "wesley"),
    ("lenny", "leonard"),
    ("len", "leonard"),
    ("marty", "martin"),
    ("jon", "jonathan"),
    ("vince", "vincent"),
    ("vic", "victor"),
]


def _build_nickname_map():
    """Build bidirectional nickname lookup: name -> set of variants."""
    nm = {}
    for a, b in NICKNAME_GROUPS:
        nm.setdefault(a, set()).add(b)
        nm.setdefault(b, set()).add(a)
    return nm


NICKNAME_MAP = _build_nickname_map()


def get_name_variants(first_name):
    """Return a set of plausible first name variants (lowercase)."""
    name = first_name.lower().strip()
    variants = {name}
    variants.update(NICKNAME_MAP.get(name, set()))
    return variants


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
# Import tracker — skip sessions that haven't changed
# Format: {"session_id": {"dataset_hash": "...", "bridge_count": N, "bills": N}}
# ---------------------------------------------------------------------------

def read_import_tracker():
    if not IMPORT_TRACKER_PATH.exists():
        return {}
    with open(IMPORT_TRACKER_PATH) as f:
        return json.load(f)


def save_import_tracker(tracker):
    IMPORT_TRACKER_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=IMPORT_TRACKER_PATH.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(tracker, f, indent=2)
        os.rename(tmp, IMPORT_TRACKER_PATH)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def should_skip_session(session_id, dataset_hash, new_bridge_count):
    """Check if a session can be skipped (already imported, no changes)."""
    tracker = read_import_tracker()
    entry = tracker.get(str(session_id))
    if not entry:
        return False
    return (
        entry.get("dataset_hash") == dataset_hash
        and entry.get("bridge_count", 0) >= new_bridge_count
        and entry.get("bills", 0) > 0
    )


def mark_session_imported(session_id, dataset_hash, bridge_count, bills_count):
    tracker = read_import_tracker()
    tracker[str(session_id)] = {
        "dataset_hash": dataset_hash,
        "bridge_count": bridge_count,
        "bills": bills_count,
        "imported_at": datetime.now().isoformat(),
    }
    save_import_tracker(tracker)


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

    resp = requests.get(LEGISCAN_BASE, params=req_params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    increment_budget(1)

    if data.get("status") != "OK":
        raise RuntimeError(f"LegiScan {op} error: {data}")

    return data


# ---------------------------------------------------------------------------
# Dataset functions (download entire sessions as ZIP archives)
# ---------------------------------------------------------------------------

def get_dataset_list(api_key, state_code):
    """Get available datasets for a state. Returns list of dataset dicts.

    Each dict includes: session_id, year_start, year_end, special,
    session_tag, dataset_hash, dataset_date, dataset_size, access_key.
    """
    data = legiscan_query(api_key, "getDatasetList", {"state": state_code})
    datasets = data.get("datasetlist", [])
    # Handle if response is a dict with numeric keys (like getMasterList)
    if isinstance(datasets, dict):
        datasets = list(datasets.values())
    return datasets


def find_dataset_for_session(datasets, year_start):
    """Find dataset matching year_start for a regular session (special=0)."""
    for ds in datasets:
        if ds.get("year_start") == year_start and ds.get("special") == 0:
            return ds
    return None


def read_dataset_hashes():
    """Read cached dataset hashes from disk."""
    if not DATASET_HASH_PATH.exists():
        return {}
    with open(DATASET_HASH_PATH) as f:
        return json.load(f)


def save_dataset_hash(session_id, hash_value):
    """Save dataset hash for a session to prevent duplicate downloads.

    LegiScan TOS requires checking dataset_hash before re-downloading.
    """
    hashes = read_dataset_hashes()
    hashes[str(session_id)] = hash_value
    DATASET_HASH_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=DATASET_HASH_PATH.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(hashes, f, indent=2)
        os.rename(tmp, DATASET_HASH_PATH)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def download_and_extract_dataset(api_key, session_id, access_key, dataset_hash):
    """Download dataset ZIP and extract bill/vote/people data.

    Uses dataset_hash caching to avoid duplicate downloads (required by LegiScan TOS).
    Returns (bills_dict, roll_calls_dict) where keys are integer IDs and
    values are parsed JSON objects matching getBill/getRollCall response format.
    """
    cached_hashes = read_dataset_hashes()
    cache_path = DATASET_CACHE_DIR / f"{session_id}.zip"

    if cached_hashes.get(str(session_id)) == dataset_hash and cache_path.exists():
        logging.info(f"Dataset hash unchanged, using cached ZIP for session {session_id}")
        with open(cache_path, "rb") as f:
            zip_bytes = f.read()
    else:
        logging.info(f"Downloading dataset for session {session_id}...")
        data = legiscan_query(api_key, "getDataset", {
            "id": str(session_id),
            "access_key": access_key,
        })
        dataset = data.get("dataset", {})
        zip_b64 = dataset.get("zip", "")
        if not zip_b64:
            raise RuntimeError(f"Empty ZIP in dataset response for session {session_id}")

        zip_bytes = base64.b64decode(zip_b64)

        # Cache the ZIP file
        DATASET_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "wb") as f:
            f.write(zip_bytes)
        save_dataset_hash(session_id, dataset_hash)
        logging.info(f"Dataset cached ({len(zip_bytes):,} bytes)")

    # Extract and parse all JSON files from the ZIP
    bills = {}
    roll_calls = {}

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue

            raw = zf.read(name)
            try:
                parsed = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                logging.debug(f"  Skipping unparseable file: {name}")
                continue

            # Classify by directory name in path
            name_lower = name.lower()
            if "/bill/" in name_lower:
                bill = parsed.get("bill", parsed)
                bill_id = bill.get("bill_id")
                if bill_id:
                    bills[bill_id] = bill
            elif "/vote/" in name_lower or "/rollcall/" in name_lower:
                rc = parsed.get("roll_call", parsed)
                rc_id = rc.get("roll_call_id")
                if rc_id:
                    roll_calls[rc_id] = rc

    logging.info(
        f"Dataset extracted: {len(bills)} bills, {len(roll_calls)} roll calls"
    )
    return bills, roll_calls


# ---------------------------------------------------------------------------
# Session upsert
# ---------------------------------------------------------------------------

def get_or_create_session(conn, jurisdiction, name, external_id, is_current):
    """Create or retrieve a legislative_sessions row. Returns the UUID."""
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
    the person is skipped (logged, not errored).

    Bridge rows use id_type='legiscan' and source='legiscan-state-people'.

    Returns (bridge_map, people_list):
      - bridge_map: {people_id (int): politician_uuid (str)}
      - people_list: raw people array from getSessionPeople (reused for committee memberships)
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

        # Match by name + state district type (single match only)
        # Try exact match first, then nickname variants as fallback
        name_variants = get_name_variants(first_name)
        cur.execute(
            """
            SELECT DISTINCT p.id, p.first_name FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            WHERE LOWER(p.last_name) = LOWER(%s)
              AND LOWER(p.first_name) = ANY(%s)
              AND d.district_type IN ('STATE_UPPER', 'STATE_LOWER', 'STATE_EXEC')
            """,
            (last_name, list(name_variants)),
        )
        matches = cur.fetchall()

        if len(matches) == 1:
            politician_id = matches[0][0]
            matched_first = matches[0][1]
            bridge_map[people_id] = politician_id
            matched += 1
            variant_note = ""
            if matched_first.lower() != first_name.lower():
                variant_note = f" (nickname: {first_name} -> {matched_first})"
            logging.debug(f"  Matched: {first_name} {last_name} -> {politician_id}{variant_note}")
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
    return bridge_map, people


# ---------------------------------------------------------------------------
# Bill status + vote position normalization
# ---------------------------------------------------------------------------

def normalize_bill_status(status_int, last_action=""):
    """Convert LegiScan status integer (1-8) to label."""
    if isinstance(status_int, int) and status_int in STATUS_MAP:
        return STATUS_MAP[status_int]

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

    committee = bill_data.get("committee", {})
    if committee and committee.get("committee_id"):
        db_id = upsert_committee(cur, committee, jurisdiction, session_id, dry_run)
        if db_id:
            committee_map[committee["committee_id"]] = db_id

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
    """Link sponsors from bill data to the bill.

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
# Vote processing (from pre-loaded dataset data — zero API calls)
# ---------------------------------------------------------------------------

def process_roll_call(conn, roll_call, bill_db_id, session_db_id, bridge_map, dry_run=False):
    """Process a single roll call from pre-loaded dataset data.

    Returns count of vote rows upserted.
    """
    cur = conn.cursor()
    votes_upserted = 0

    roll_call_id = roll_call.get("roll_call_id")
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

    return votes_upserted


# ---------------------------------------------------------------------------
# Bill import from dataset (zero API calls — all data pre-loaded from ZIP)
# ---------------------------------------------------------------------------

def reconnect(db_url, existing_conn=None):
    """Close existing connection (if open) and return a fresh one.

    Called automatically when Supabase drops an idle connection.
    Supabase PgBouncer sessions can time out after ~5 minutes of inactivity.
    """
    if existing_conn is not None:
        try:
            existing_conn.close()
        except Exception:
            pass
    logging.info("Reconnecting to database...")
    return psycopg2.connect(db_url)


def is_connection_error(exc):
    """Return True if exc is a psycopg2 connection/SSL timeout error."""
    return isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError))


def import_bills_from_dataset(
    db_url, conn, bills_data, roll_calls_data, session_db_id, jurisdiction, bridge_map, dry_run=False
):
    """Import all bills from pre-loaded dataset data. Zero API calls.

    bills_data: dict {bill_id (int): bill_object (dict)}
    roll_calls_data: dict {roll_call_id (int): roll_call_object (dict)}

    Returns (db_conn, bills_upserted, cosponsors_total, votes_total, committee_db_map, errors).
    The returned db_conn may be a fresh reconnection if the original timed out.
    """
    cur = conn.cursor()

    bills_upserted = 0
    bills_skipped = 0
    cosponsors_total = 0
    votes_total = 0
    all_committee_db_map = {}
    errors = []

    bill_items = sorted(bills_data.items())
    logging.info(f"Processing {len(bill_items)} bills from dataset")

    # Pre-fetch existing bill external_ids for this jurisdiction to skip already-imported bills
    existing_bill_ids = set()
    try:
        cur.execute(
            "SELECT external_id FROM essentials.legislative_bills WHERE jurisdiction = %s AND session_id = %s",
            (jurisdiction, str(session_db_id)),
        )
        existing_bill_ids = {row[0] for row in cur.fetchall()}
        if existing_bill_ids:
            logging.info(f"Found {len(existing_bill_ids)} existing bills in DB — will skip these")
    except Exception as e:
        logging.warning(f"Could not pre-fetch existing bills: {e}")
        conn.rollback()

    for i, (bill_id, bill_data) in enumerate(bill_items):
        try:
            # Skip bills already in DB (before any processing)
            external_id = f"legiscan-{bill_id}"
            if external_id in existing_bill_ids:
                bills_skipped += 1
                continue

            # Refresh cursor (may have been replaced after reconnect)
            cur = conn.cursor()

            # Extract and upsert committees from this bill
            committee_map = extract_and_upsert_committees(
                conn, bill_data, jurisdiction, session_db_id, dry_run
            )
            all_committee_db_map.update(committee_map)

            # Normalize bill fields
            number = bill_data.get("bill_number", bill_data.get("number", ""))
            title = bill_data.get("title", "")
            status_int = bill_data.get("status", 1)
            last_action = bill_data.get("status_desc", "")
            status_label = normalize_bill_status(status_int, last_action)
            url = bill_data.get("url", "")

            # Parse introduced date from history
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

            # Upsert bill
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

            # Link sponsors
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

            # Process votes from pre-loaded roll call data (zero API calls)
            votes_list = bill_data.get("votes", [])
            for vote_stub in votes_list:
                roll_call_id = vote_stub.get("roll_call_id")
                if not roll_call_id or roll_call_id not in roll_calls_data:
                    continue

                try:
                    votes_for_rc = process_roll_call(
                        conn, roll_calls_data[roll_call_id], bill_db_id,
                        session_db_id, bridge_map, dry_run
                    )
                    votes_total += votes_for_rc
                except Exception as e:
                    logging.error(f"  Error processing roll call {roll_call_id}: {e}")
                    if is_connection_error(e):
                        conn = reconnect(db_url, conn)
                        cur = conn.cursor()
                    else:
                        try:
                            conn.rollback()
                        except Exception:
                            conn = reconnect(db_url, conn)
                            cur = conn.cursor()

            # Progress logging every 200 bills
            if (i + 1) % 200 == 0:
                logging.info(
                    f"  Progress: {i + 1}/{len(bill_items)} bills processed "
                    f"({bills_upserted} upserted, {votes_total} votes)"
                )

        except Exception as e:
            errors.append(f"Bill {bill_id}: {e}")
            logging.error(f"  Error processing bill {bill_id}: {e}")
            if is_connection_error(e):
                # Supabase dropped the connection — reconnect and continue
                logging.warning("Connection lost, reconnecting...")
                conn = reconnect(db_url, conn)
                cur = conn.cursor()
            else:
                try:
                    conn.rollback()
                except Exception:
                    conn = reconnect(db_url, conn)
                    cur = conn.cursor()
            if len(errors) > 50:
                logging.error("Too many errors — aborting bill import")
                break

    logging.info(
        f"Bills: {bills_upserted} upserted, {bills_skipped} skipped (already in DB), "
        f"{cosponsors_total} cosponsors, {votes_total} votes, "
        f"{len(all_committee_db_map)} committees, {len(errors)} errors"
    )
    return conn, bills_upserted, cosponsors_total, votes_total, all_committee_db_map, errors


# ---------------------------------------------------------------------------
# Main state import orchestrator (Dataset Edition)
# ---------------------------------------------------------------------------

def import_state(api_key, db_url, conn, state_code, sessions_to_import, dry_run=False, force=False):
    """Import all legislative data for a state using the Dataset API.

    Flow per session:
      1. getDatasetList (1 query, shared across sessions for the state)
      2. getDataset (1 query per session, or skip if hash cached)
      3. getSessionPeople (1 query per session, for bridge + committee data)
      4. Process bills/votes/committees from ZIP (0 queries)

    Total: ~3-5 queries per state instead of thousands.
    db_url is passed so the bill import can reconnect if Supabase drops the connection.
    """
    config = STATE_SESSIONS[state_code]
    jurisdiction = config["jurisdiction"]
    results = {
        "bridge_rows": 0,
        "bills": 0,
        "votes": 0,
        "cosponsors": 0,
        "committees": 0,
        "memberships": 0,
        "errors": [],
    }

    # 1. Get dataset list for the state (1 API call)
    logging.info(f"Fetching dataset list for {state_code}...")
    datasets = get_dataset_list(api_key, state_code)
    logging.info(f"Found {len(datasets)} datasets for {state_code}")

    session_targets = []
    if "current" in sessions_to_import:
        session_targets.append(("current", config["current_year_start"]))
    if "previous" in sessions_to_import:
        session_targets.append(("previous", config["previous_year_start"]))

    all_committee_db_map = {}

    for label, year_start in session_targets:
        logging.info(f"\n{'=' * 60}")
        logging.info(f"Processing {config['name']} {label} session (year_start={year_start})")
        logging.info(f"{'=' * 60}")

        # 2. Find matching dataset
        ds = find_dataset_for_session(datasets, year_start)
        if not ds:
            msg = f"No dataset found for {state_code} year_start={year_start}"
            logging.error(msg)
            results["errors"].append(msg)
            continue

        legiscan_session_id = ds["session_id"]
        access_key = ds.get("access_key", "")
        dataset_hash = ds.get("dataset_hash", "")
        logging.info(
            f"Found dataset: session_id={legiscan_session_id}, "
            f"hash={dataset_hash[:12]}..., size={ds.get('dataset_size', '?')} bytes"
        )

        # 3. Download and extract dataset (1 API call, or cached)
        bills_data, roll_calls_data = download_and_extract_dataset(
            api_key, legiscan_session_id, access_key, dataset_hash
        )

        # 4. Create/get DB session row
        year_end = ds.get("year_end", year_start)
        if year_end and year_end != year_start:
            session_name = f"{year_start}-{year_end} {config['name']} Regular Session"
        else:
            session_name = f"{year_start} {config['name']} Regular Session"
        is_current = label == "current"
        session_db_id = get_or_create_session(
            conn, jurisdiction, session_name, legiscan_session_id, is_current
        )
        logging.info(f"Session DB ID: {session_db_id}")

        # 5. Build legislator bridge (1 API call: getSessionPeople)
        # Also returns people list for committee membership reuse
        logging.info("Building legislator bridge...")
        bridge_map, session_people = build_legislator_bridge(
            api_key, legiscan_session_id, state_code, jurisdiction, conn, dry_run
        )
        results["bridge_rows"] += len(bridge_map)
        logging.info(f"Bridge: {len(bridge_map)} legislators matched")

        if not bridge_map:
            logging.warning(
                f"No legislators matched for {state_code} session {legiscan_session_id} "
                "— skipping bill/vote import"
            )
            continue

        # 6. Check if session can be skipped (already imported, no changes)
        if not dry_run and not force and should_skip_session(
            legiscan_session_id, dataset_hash, len(bridge_map)
        ):
            logging.info(
                f"Session {legiscan_session_id} already imported with same data and "
                f"{len(bridge_map)} bridges. Skipping. (Use --force to reimport)"
            )
            continue

        # 7. Import bills + votes from dataset (0 API calls)
        logging.info("Importing bills, votes, and committees from dataset...")
        conn, bills, cosponsors, votes, committee_db_map, errors = import_bills_from_dataset(
            db_url, conn, bills_data, roll_calls_data, session_db_id,
            jurisdiction, bridge_map, dry_run
        )
        all_committee_db_map.update(committee_db_map)
        results["bills"] += bills
        results["cosponsors"] += cosponsors
        results["votes"] += votes
        results["committees"] += len(committee_db_map)
        results["errors"].extend(errors)

        # 8. Committee memberships from session_people (0 API calls — reusing step 5 data)
        logging.info("Creating committee memberships from people data...")
        memberships = upsert_committee_memberships_from_people(
            conn, session_people, bridge_map, jurisdiction, session_db_id,
            all_committee_db_map, dry_run
        )
        results["memberships"] += memberships

        # 9. Track successful import
        if not dry_run and bills > 0:
            mark_session_imported(legiscan_session_id, dataset_hash, len(bridge_map), bills)

        budget_used = read_budget()
        logging.info(f"Session complete. Budget: {budget_used}/{BUDGET_LIMIT}")

    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Import state legislative data from LegiScan API (Dataset Edition)"
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
    parser.add_argument(
        "--force", action="store_true",
        help="Force reimport even if session data hasn't changed"
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
    if remaining < 5000 and args.state == "CA":
        logging.warning(
            f"Budget low ({remaining} remaining). Consider running --state IN only "
            "and waiting for monthly reset before CA import."
        )
    if remaining < 100:
        logging.error(
            f"Budget nearly exhausted ({remaining} remaining). "
            "Wait for monthly reset."
        )
        sys.exit(1)

    sessions_to_import = [s.strip() for s in args.sessions.split(",")]
    invalid = [s for s in sessions_to_import if s not in ("current", "previous")]
    if invalid:
        logging.error(f"Unknown session labels: {invalid}. Use 'current' and/or 'previous'.")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    try:
        results = import_state(api_key, db_url, conn, args.state, sessions_to_import, args.dry_run, args.force)

        logging.info(f"\n{'=' * 60}")
        logging.info(f"IMPORT COMPLETE: {args.state}")
        logging.info(f"  Bridge rows:   {results['bridge_rows']}")
        logging.info(f"  Bills:         {results['bills']}")
        logging.info(f"  Cosponsors:    {results['cosponsors']}")
        logging.info(f"  Votes:         {results['votes']}")
        logging.info(f"  Committees:    {results['committees']}")
        logging.info(f"  Memberships:   {results['memberships']}")
        logging.info(f"  Errors:        {len(results['errors'])}")
        if results["errors"]:
            logging.error("First errors:")
            for e in results["errors"][:10]:
                logging.error(f"  - {e}")
        budget_used = read_budget()
        logging.info(f"  Budget used:   {budget_used}/{BUDGET_LIMIT} ({BUDGET_LIMIT - budget_used} remaining)")
        logging.info(f"{'=' * 60}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
