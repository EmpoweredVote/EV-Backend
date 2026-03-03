#!/usr/bin/env python3
"""
Bloomington Common Council Data Importer (OnBoard HTML Scraping)

Scrapes committee memberships and legislation metadata from bloomington.in.gov/onboard.
Matches council members to existing essentials.politicians records via name matching.

Usage:
    python import_local_bloomington.py --dry-run --verbose
    python import_local_bloomington.py --verbose
    python import_local_bloomington.py --committees-only --verbose
    python import_local_bloomington.py --legislation-only --verbose
    python import_local_bloomington.py --max-legislation 10 --dry-run --verbose

Environment variables (loaded from EV-Backend/.env.local):
    DATABASE_URL   PostgreSQL connection string
"""

import argparse
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rapidfuzz import fuzz

# Load .env.local from EV-Backend root (parent of scripts/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env.local")

import os

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_VERSION = "1.0.0"
ONBOARD_BASE = "https://bloomington.in.gov/onboard"
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 2.0  # seconds between OnBoard requests (be respectful)
FUZZY_THRESHOLD = 80

# Nickname expansions — try both original and expanded forms during matching
NICKNAME_MAP = {
    "dave": "david", "david": "dave",
    "bill": "william", "william": "bill",
    "bob": "robert", "robert": "bob",
    "mike": "michael", "michael": "mike",
    "jim": "james", "james": "jim",
    "joe": "joseph", "joseph": "joe",
    "tom": "thomas", "thomas": "tom",
    "dan": "daniel", "daniel": "dan",
    "steve": "steven", "steven": "steve",
    "chris": "christopher", "christopher": "chris",
    "matt": "matthew", "matthew": "matt",
    "rick": "richard", "richard": "rick",
    "dick": "richard",
    "ted": "theodore", "theodore": "ted",
    "ed": "edward", "edward": "ed",
    "sam": "samuel", "samuel": "sam",
    "tony": "anthony", "anthony": "tony",
    "pat": "patrick", "patrick": "pat",
    "jen": "jennifer", "jennifer": "jen",
    "kate": "katherine", "katherine": "kate",
    "beth": "elizabeth", "elizabeth": "beth",
    "sue": "susan", "susan": "sue",
}

# Committee IDs for Bloomington Common Council
# Key = OnBoard committee_id, Value = (display_name, is_subcommittee)
COMMITTEE_DEFS = {
    1: ("Common Council", False),
    77: ("Council Processes Committee", True),
    81: ("Fiscal Committee", True),
    49: ("Sidewalk/Pedestrian Safety Committee", True),
}

JURISDICTION = "bloomington-in"
SESSION_EXTERNAL_ID = "bloomington-current"
SESSION_NAME = "Bloomington Common Council (Current Term)"
SOURCE = "onboard-scrape"

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
# HTTP helpers
# ---------------------------------------------------------------------------

def http_get(url: str, params: dict = None, verbose: bool = False):
    """GET request with error handling. Returns (status_code, response, error)."""
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if verbose:
            log.info(f"  GET {r.url} -> {r.status_code}")
        return r.status_code, r, None
    except requests.RequestException as e:
        log.warning(f"  GET {url} -> ERROR: {e}")
        return None, None, str(e)


# ---------------------------------------------------------------------------
# 1. scrape_council_members
# ---------------------------------------------------------------------------

def scrape_council_members(committee_id: int = 1, verbose: bool = False) -> list:
    """
    GET /onboard/committees/{id}/members and parse member list.

    Returns list of dicts: [{name, onboard_id, seat_text, role}]
    Role is derived from seat_text: 'president', 'chair', 'vice_chair', or 'member'.
    """
    url = f"{ONBOARD_BASE}/committees/{committee_id}/members"
    log.info(f"Scraping committee {committee_id} members from {url} ...")
    time.sleep(REQUEST_DELAY)

    status, resp, err = http_get(url, verbose=verbose)
    if err or status != 200:
        log.warning(f"  Committee {committee_id}: HTTP {status or 'error'} — {err or 'non-200'}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Primary method: extract member names + IDs from /onboard/members/{id} links
    member_link_re = re.compile(r"/onboard/members/(\d+)")
    members = []
    seen_ids = set()

    # Build a map of member_id -> seat text from surrounding context
    # Members are typically in table rows or list items
    for link in soup.find_all("a", href=member_link_re):
        href = link.get("href", "")
        m = member_link_re.search(href)
        if not m:
            continue
        onboard_id = m.group(1)
        if onboard_id in seen_ids:
            continue
        seen_ids.add(onboard_id)

        name = link.get_text(strip=True)
        if not name:
            continue

        # Try to find seat text in the same row/cell
        seat_text = ""
        parent_row = link.find_parent("tr")
        if parent_row:
            cells = parent_row.find_all(["td", "th"])
            cell_texts = [c.get_text(strip=True) for c in cells]
            # Seat text is typically in a cell other than the name cell
            for ct in cell_texts:
                if ct and ct != name and len(ct) < 80:
                    seat_text = ct
                    break
        else:
            # Try sibling text in list item
            parent_li = link.find_parent("li")
            if parent_li:
                full_text = parent_li.get_text(separator=" ", strip=True)
                # Remove the name from the text to get seat context
                seat_text = full_text.replace(name, "").strip().strip("-").strip()

        role = _parse_role(seat_text, name)
        if verbose:
            log.info(f"  Member: {name!r} (id={onboard_id}, seat={seat_text!r}, role={role})")

        members.append({
            "name": name,
            "onboard_id": onboard_id,
            "seat_text": seat_text,
            "role": role,
        })

    # Fallback: parse text lines if no member links found
    if not members:
        log.info(f"  No member links found for committee {committee_id}; trying text fallback ...")
        text = soup.get_text(separator="\n")
        in_current = False
        for line in text.split("\n"):
            line = line.strip()
            if re.search(r"Current Members|Current Membership", line, re.IGNORECASE):
                in_current = True
                continue
            if in_current and re.match(r"^[A-Z][a-z]+[\s\-][A-Z]", line) and "Seat" not in line and len(line) < 60:
                role = _parse_role(line, "")
                members.append({
                    "name": line,
                    "onboard_id": None,
                    "seat_text": line,
                    "role": role,
                })
        if verbose:
            log.info(f"  Text fallback found {len(members)} members")

    log.info(f"  Committee {committee_id}: {len(members)} members extracted")
    return members


def _parse_role(seat_text: str, name: str) -> str:
    """Derive role string from seat_text. Returns 'president', 'chair', 'vice_chair', or 'member'."""
    combined = f"{seat_text} {name}".lower()
    if "president" in combined:
        return "president"
    if "vice chair" in combined or "vice-chair" in combined:
        return "vice_chair"
    if "chair" in combined:
        return "chair"
    return "member"


# ---------------------------------------------------------------------------
# 2. scrape_sub_committee_members (delegates to scrape_council_members)
# ---------------------------------------------------------------------------

def scrape_sub_committee_members(committee_id: int, verbose: bool = False) -> list:
    """
    Scrape member list for a sub-committee page (IDs: 77, 81, 49).
    May have different HTML structure — delegates to scrape_council_members with graceful handling.
    """
    return scrape_council_members(committee_id=committee_id, verbose=verbose)


# ---------------------------------------------------------------------------
# 3. build_politician_bridge
# ---------------------------------------------------------------------------

def build_politician_bridge(members: list, conn, dry_run: bool = False, verbose: bool = False) -> dict:
    """
    Match scraped member names to essentials.politicians by name using RapidFuzz.

    Returns dict mapping onboard_id -> politician_uuid (str).
    Inserts bridge rows into essentials.legislative_politician_id_map.
    Single-match-only: skips if 0 or 2+ matches above FUZZY_THRESHOLD.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Load all active politicians once
    log.info("Loading active politicians from DB for name matching ...")
    cur.execute(
        "SELECT id, full_name FROM essentials.politicians WHERE is_active = true ORDER BY full_name"
    )
    all_politicians = cur.fetchall()
    log.info(f"  Loaded {len(all_politicians)} active politicians")

    bridge_map = {}  # onboard_id -> politician_uuid str
    name_to_uuid = {}  # full_name (lower) -> politician_uuid str (for legislation matching)

    for member in members:
        name = member["name"]
        onboard_id = member.get("onboard_id")

        # Build name variants (original + nickname-expanded)
        name_variants = [name.lower()]
        parts = name.lower().split()
        if parts and parts[0] in NICKNAME_MAP:
            alt_parts = [NICKNAME_MAP[parts[0]]] + parts[1:]
            name_variants.append(" ".join(alt_parts))

        # Find best fuzzy match across all name variants
        candidates = []
        for pol in all_politicians:
            pol_lower = pol["full_name"].lower()
            best_score = max(
                fuzz.token_sort_ratio(variant, pol_lower)
                for variant in name_variants
            )
            if best_score >= FUZZY_THRESHOLD:
                candidates.append((best_score, pol))

        if len(candidates) == 0:
            log.warning(f"  NO MATCH for {name!r} (onboard_id={onboard_id})")
            continue
        if len(candidates) >= 2:
            # Only skip if two candidates have very close scores (within 5 points)
            top_score = candidates[0][0] if candidates else 0
            candidates.sort(key=lambda x: x[0], reverse=True)
            top_score = candidates[0][0]
            second_score = candidates[1][0] if len(candidates) > 1 else 0
            if top_score - second_score < 5:
                log.warning(
                    f"  AMBIGUOUS for {name!r}: {len(candidates)} candidates above threshold "
                    f"(top={top_score}, second={second_score})"
                )
                continue
            # Otherwise take the top match
            matched_pol = candidates[0][1]
            log.info(
                f"  MATCHED {name!r} -> {matched_pol['full_name']!r} "
                f"(score={top_score}, next={second_score}, uuid={matched_pol['id']})"
            )
        else:
            matched_pol = candidates[0][1]
            log.info(
                f"  MATCHED {name!r} -> {matched_pol['full_name']!r} "
                f"(score={candidates[0][0]}, uuid={matched_pol['id']})"
            )

        pol_uuid = str(matched_pol["id"])
        name_to_uuid[matched_pol["full_name"].lower()] = pol_uuid
        name_to_uuid[name.lower()] = pol_uuid

        if onboard_id is not None:
            bridge_map[onboard_id] = pol_uuid

            if not dry_run:
                # Upsert bridge row
                cur.execute(
                    """
                    INSERT INTO essentials.legislative_politician_id_map
                        (politician_id, id_type, id_value, verified_at, source)
                    VALUES (%s, 'onboard', %s, NOW(), %s)
                    ON CONFLICT (politician_id, id_type, id_value) DO NOTHING
                    """,
                    (pol_uuid, onboard_id, SOURCE),
                )
            else:
                log.info(f"    [DRY-RUN] Would INSERT bridge: politician_id={pol_uuid}, id_type=onboard, id_value={onboard_id}")

    if not dry_run:
        conn.commit()

    log.info(f"  Bridge built: {len(bridge_map)} onboard_id -> politician_uuid mappings")
    cur.close()

    # Return extended map: onboard_id -> uuid AND name -> uuid for legislation sponsor matching
    return bridge_map, name_to_uuid


# ---------------------------------------------------------------------------
# 4. ensure_session
# ---------------------------------------------------------------------------

def ensure_session(conn, dry_run: bool = False) -> str:
    """
    Upsert a LegislativeSession for Bloomington Common Council.
    Returns session UUID string.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    log.info(f"Ensuring session: {SESSION_EXTERNAL_ID} ...")

    if dry_run:
        log.info(f"  [DRY-RUN] Would upsert session: jurisdiction={JURISDICTION}, external_id={SESSION_EXTERNAL_ID}")
        cur.close()
        return "00000000-0000-0000-0000-000000000000"

    # Try to find existing session
    cur.execute(
        "SELECT id FROM essentials.legislative_sessions WHERE external_id = %s AND jurisdiction = %s",
        (SESSION_EXTERNAL_ID, JURISDICTION),
    )
    row = cur.fetchone()
    if row:
        session_id = str(row["id"])
        log.info(f"  Session already exists: {session_id}")
        cur.close()
        return session_id

    # Insert new session
    cur.execute(
        """
        INSERT INTO essentials.legislative_sessions
            (jurisdiction, name, is_current, external_id, source)
        VALUES (%s, %s, true, %s, %s)
        RETURNING id
        """,
        (JURISDICTION, SESSION_NAME, SESSION_EXTERNAL_ID, SOURCE),
    )
    row = cur.fetchone()
    session_id = str(row["id"])
    conn.commit()
    log.info(f"  Session created: {session_id}")
    cur.close()
    return session_id


# ---------------------------------------------------------------------------
# 5. import_committees
# ---------------------------------------------------------------------------

def import_committees(bridge_map: dict, name_to_uuid: dict, session_id: str, conn,
                      dry_run: bool = False, verbose: bool = False) -> dict:
    """
    Upsert committees and memberships for all Bloomington committee IDs.

    Returns dict mapping committee_id (int) -> committee_uuid (str).
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    committee_uuid_map = {}  # committee_id int -> uuid str
    total_memberships = 0
    total_committees = 0

    # First pass: upsert all committees (need City Council UUID for parent_id of sub-committees)
    for committee_id, (committee_name, is_subcommittee) in COMMITTEE_DEFS.items():
        external_id = f"onboard-{committee_id}"
        committee_type = "subcommittee" if is_subcommittee else "committee"

        if dry_run:
            log.info(
                f"  [DRY-RUN] Would upsert committee: external_id={external_id}, name={committee_name!r}, "
                f"type={committee_type}"
            )
            committee_uuid_map[committee_id] = f"dry-run-uuid-{committee_id}"
            continue

        # Check if exists
        cur.execute(
            "SELECT id FROM essentials.legislative_committees WHERE external_id = %s AND jurisdiction = %s",
            (external_id, JURISDICTION),
        )
        row = cur.fetchone()
        if row:
            committee_uuid = str(row["id"])
            log.info(f"  Committee {committee_id} ({committee_name}): exists ({committee_uuid})")
        else:
            # Insert — parent_id set in second pass
            cur.execute(
                """
                INSERT INTO essentials.legislative_committees
                    (session_id, external_id, jurisdiction, name, type, chamber, is_current, source)
                VALUES (%s, %s, %s, %s, %s, 'local', true, %s)
                RETURNING id
                """,
                (session_id, external_id, JURISDICTION, committee_name, committee_type, SOURCE),
            )
            row = cur.fetchone()
            committee_uuid = str(row["id"])
            total_committees += 1
            log.info(f"  Committee {committee_id} ({committee_name}): inserted ({committee_uuid})")

        committee_uuid_map[committee_id] = committee_uuid

    if not dry_run:
        conn.commit()

    # Second pass: set parent_id for sub-committees
    city_council_uuid = committee_uuid_map.get(1)
    if city_council_uuid and not dry_run:
        for committee_id, (committee_name, is_subcommittee) in COMMITTEE_DEFS.items():
            if is_subcommittee:
                external_id = f"onboard-{committee_id}"
                cur.execute(
                    """
                    UPDATE essentials.legislative_committees
                    SET parent_id = %s
                    WHERE external_id = %s AND jurisdiction = %s AND parent_id IS NULL
                    """,
                    (city_council_uuid, external_id, JURISDICTION),
                )
        conn.commit()
        log.info(f"  Sub-committee parent_id set to City Council UUID ({city_council_uuid})")

    # Third pass: scrape and upsert memberships
    for committee_id, (committee_name, is_subcommittee) in COMMITTEE_DEFS.items():
        committee_uuid = committee_uuid_map.get(committee_id)
        if not committee_uuid:
            log.warning(f"  Skipping memberships for committee {committee_id}: no UUID")
            continue

        if is_subcommittee:
            members = scrape_sub_committee_members(committee_id, verbose=verbose)
        else:
            members = scrape_council_members(committee_id, verbose=verbose)

        memberships_imported = 0
        memberships_skipped = 0

        for member in members:
            name = member["name"]
            onboard_id = member.get("onboard_id")
            role = member["role"]

            # Try to find politician UUID from bridge_map (by onboard_id) or name_to_uuid (by name)
            pol_uuid = bridge_map.get(onboard_id) if onboard_id else None
            if not pol_uuid:
                pol_uuid = name_to_uuid.get(name.lower())
            if not pol_uuid:
                log.warning(f"  No politician UUID for {name!r} (onboard_id={onboard_id}) — skipping membership")
                memberships_skipped += 1
                continue

            if dry_run:
                log.info(
                    f"  [DRY-RUN] Would upsert membership: committee={committee_name!r}, "
                    f"politician={name!r}, role={role}"
                )
                memberships_imported += 1
                continue

            # Upsert membership — unique index: (committee_id, politician_id, congress_number)
            cur.execute(
                """
                INSERT INTO essentials.legislative_committee_memberships
                    (committee_id, politician_id, congress_number, role, is_current, session_id)
                VALUES (%s, %s, 0, %s, true, %s)
                ON CONFLICT (committee_id, politician_id, congress_number)
                DO UPDATE SET
                    role = EXCLUDED.role,
                    is_current = true,
                    session_id = EXCLUDED.session_id
                """,
                (committee_uuid, pol_uuid, role, session_id),
            )
            memberships_imported += 1

        if not dry_run:
            conn.commit()

        total_memberships += memberships_imported
        log.info(
            f"  Committee {committee_id} ({committee_name}): "
            f"{memberships_imported} memberships imported, {memberships_skipped} skipped"
        )

    cur.close()
    log.info(
        f"Imported {total_memberships} memberships for {len(COMMITTEE_DEFS)} committees "
        f"({total_committees} new committees created)"
    )
    return committee_uuid_map


# ---------------------------------------------------------------------------
# 6. scrape_legislation_list
# ---------------------------------------------------------------------------

def scrape_legislation_list(committee_id: int = 1, max_pages: int = 50,
                             verbose: bool = False) -> list:
    """
    GET /onboard/committees/{committee_id}/legislation?page={N}
    Paginate until no more items are found (or max_pages reached).

    Returns list of dicts: [{id, title, url}]
    """
    leg_pattern = re.compile(rf"/onboard/committees/{committee_id}/legislation/(\d+)")
    all_items = []
    seen_ids = set()

    for page_num in range(1, max_pages + 1):
        url = f"{ONBOARD_BASE}/committees/{committee_id}/legislation"
        params = {"page": page_num}
        log.info(f"  Scraping legislation list page {page_num} ...")
        time.sleep(REQUEST_DELAY)

        status, resp, err = http_get(url, params=params, verbose=verbose)
        if err or status != 200:
            log.warning(f"  Legislation page {page_num}: HTTP {status or 'error'} — stopping pagination")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_items = []

        for link in soup.find_all("a", href=leg_pattern):
            href = link.get("href", "")
            m = leg_pattern.search(href)
            if not m:
                continue
            leg_id = m.group(1)
            if leg_id in seen_ids:
                continue
            seen_ids.add(leg_id)

            title = link.get_text(strip=True)
            full_url = f"{ONBOARD_BASE}/committees/{committee_id}/legislation/{leg_id}"
            page_items.append({
                "id": leg_id,
                "title": title,
                "url": full_url,
            })

        if not page_items:
            log.info(f"  Page {page_num}: no items found — pagination complete")
            break

        all_items.extend(page_items)
        log.info(f"  Page {page_num}: {len(page_items)} items (total so far: {len(all_items)})")

        # If fewer than expected items, likely last page
        if len(page_items) < 5:
            log.info(f"  Page {page_num}: fewer than 5 items — pagination complete")
            break

    log.info(f"Legislation list: {len(all_items)} items across {page_num} pages")
    return all_items


# ---------------------------------------------------------------------------
# 7. scrape_legislation_detail
# ---------------------------------------------------------------------------

# Primary sponsor regex: "Sponsored by Councilmember(s) NAME allows/amends/etc."
SPONSOR_RE_PRIMARY = re.compile(
    r"[Ss]ponsored by Councilmembers?\s+(.+?)"
    r"(?:\s+(?:allows|consolidates|renames|updates|amends|directs|is|repeals|establishes|"
    r"appropriates|authorizes|creates|requires|requests|proposes|makes|would|which|that)|\.|$)",
    re.IGNORECASE,
)

# Fallback: "Sponsored by NAME."
SPONSOR_RE_FALLBACK = re.compile(
    r"[Ss]ponsored by\s+(.+?)(?:\.|$)",
    re.IGNORECASE,
)


def scrape_legislation_detail(committee_id: int, legislation_id: str,
                               verbose: bool = False) -> dict:
    """
    GET /onboard/committees/{committee_id}/legislation/{legislation_id}
    Parse description and attempt sponsor extraction.

    Returns dict: {description, sponsors: [str], raw_sponsor_text}
    """
    url = f"{ONBOARD_BASE}/committees/{committee_id}/legislation/{legislation_id}"
    time.sleep(REQUEST_DELAY)

    status, resp, err = http_get(url, verbose=verbose)
    if err or status != 200:
        return {"description": "", "sponsors": [], "raw_sponsor_text": "", "error": str(err or status)}

    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text(separator=" ")

    description = ""
    raw_sponsor_text = ""
    sponsors = []

    # Try primary regex
    m = SPONSOR_RE_PRIMARY.search(page_text)
    if m:
        raw_sponsor_text = m.group(1).strip()
    else:
        # Try fallback
        m = SPONSOR_RE_FALLBACK.search(page_text)
        if m:
            raw_sponsor_text = m.group(1).strip()

    if raw_sponsor_text:
        # Split multiple sponsors on " and " or ", "
        # First normalize " and " -> ","
        normalized = re.sub(r"\s+and\s+", ", ", raw_sponsor_text, flags=re.IGNORECASE)
        # Split on comma
        parts = [p.strip() for p in normalized.split(",") if p.strip()]
        # Filter out parts that look like verbs/sentences (likely a regex over-match)
        sponsor_parts = []
        for part in parts:
            # A sponsor name should not be too long or contain verb patterns
            if len(part) < 60 and not re.search(
                r"\b(allows|amends|directs|establishes|requires|authorizes|creates|proposes|makes|would|which|that)\b",
                part, re.IGNORECASE
            ):
                sponsor_parts.append(part)
        sponsors = sponsor_parts

        if verbose:
            log.info(f"  Sponsor text: {raw_sponsor_text!r}")
            log.info(f"  Parsed sponsors: {sponsors}")

    # Try to extract a short description from the page
    # Look for a paragraph or div containing "Sponsored by" or description text
    desc_candidates = soup.find_all(["p", "div"], string=re.compile(r"[A-Z]", re.IGNORECASE))
    for el in desc_candidates[:5]:
        text = el.get_text(strip=True)
        if len(text) > 30 and ("Sponsored" in text or "ordinance" in text.lower() or "resolution" in text.lower()):
            description = text[:500]
            break

    return {
        "description": description,
        "sponsors": sponsors,
        "raw_sponsor_text": raw_sponsor_text,
    }


# ---------------------------------------------------------------------------
# 8. import_legislation
# ---------------------------------------------------------------------------

def import_legislation(bridge_map: dict, name_to_uuid: dict, session_id: str, conn,
                       dry_run: bool = False, verbose: bool = False,
                       max_items: int = None) -> dict:
    """
    Scrape and import legislation from committee 1 (City Council).

    Only imports items where at least one sponsor is matched to a politician.
    Skips items with no sponsor attribution — logs them as skipped.

    Returns dict with stats: {imported, skipped_no_sponsor, skipped_no_match, errors}
    """
    stats = {"imported": 0, "skipped_no_sponsor": 0, "skipped_no_match": 0, "errors": 0}

    # Scrape legislation list
    log.info("Scraping legislation list ...")
    if max_items and max_items <= 20:
        # For small limits, one page is enough
        items = scrape_legislation_list(committee_id=1, max_pages=1, verbose=verbose)
    else:
        items = scrape_legislation_list(committee_id=1, verbose=verbose)

    if max_items:
        items = items[:max_items]
        log.info(f"  Limited to {max_items} items per --max-legislation flag")

    log.info(f"Processing {len(items)} legislation items ...")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build a name lookup for sponsor matching from all politicians (not just bridge)
    # Load all active politicians for sponsor name matching
    cur.execute(
        "SELECT id, full_name FROM essentials.politicians WHERE is_active = true ORDER BY full_name"
    )
    all_politicians = cur.fetchall()
    pol_name_list = [(str(p["id"]), p["full_name"]) for p in all_politicians]

    def match_sponsor_name(name: str):
        """Match a sponsor name fragment to a politician UUID. Returns uuid str or None."""
        # First try name_to_uuid from bridge (exact known members)
        uuid = name_to_uuid.get(name.lower())
        if uuid:
            return uuid

        # Build name variants (original + nickname-expanded)
        name_variants = [name.lower()]
        parts = name.lower().split()
        if parts and parts[0] in NICKNAME_MAP:
            alt_parts = [NICKNAME_MAP[parts[0]]] + parts[1:]
            name_variants.append(" ".join(alt_parts))

        # Fuzzy match against all politicians with nickname expansion
        candidates_above_threshold = []
        for pol_uuid, pol_name in pol_name_list:
            pol_lower = pol_name.lower()
            best_score = max(
                fuzz.token_sort_ratio(variant, pol_lower)
                for variant in name_variants
            )
            if best_score >= FUZZY_THRESHOLD:
                candidates_above_threshold.append((best_score, pol_uuid, pol_name))

        if not candidates_above_threshold:
            return None

        candidates_above_threshold.sort(key=lambda x: x[0], reverse=True)
        if len(candidates_above_threshold) == 1:
            return candidates_above_threshold[0][1]

        # Multiple: only return if top score clearly dominates
        top_score = candidates_above_threshold[0][0]
        second_score = candidates_above_threshold[1][0]
        if top_score - second_score >= 5:
            return candidates_above_threshold[0][1]

        return None  # Ambiguous

    for i, item in enumerate(items):
        leg_id = item["id"]
        title = item["title"]
        item_url = item["url"]

        if verbose:
            log.info(f"  [{i+1}/{len(items)}] Processing legislation {leg_id}: {title!r}")
        elif i % 10 == 0:
            log.info(f"  Progress: {i}/{len(items)} items processed ...")

        # Scrape detail page
        detail = scrape_legislation_detail(1, leg_id, verbose=verbose)
        if detail.get("error"):
            log.warning(f"  Item {leg_id}: detail page error — {detail['error']}")
            stats["errors"] += 1
            continue

        sponsors = detail.get("sponsors", [])
        raw_sponsor_text = detail.get("raw_sponsor_text", "")
        description = detail.get("description", "")

        # Skip if no sponsor text found
        if not sponsors and not raw_sponsor_text:
            if verbose:
                log.info(f"  Item {leg_id}: no sponsor text — SKIPPED")
            stats["skipped_no_sponsor"] += 1
            continue

        # Match sponsor names to politician UUIDs
        matched_sponsors = []
        for sponsor_name in sponsors:
            pol_uuid = match_sponsor_name(sponsor_name)
            if pol_uuid:
                matched_sponsors.append(pol_uuid)
                if verbose:
                    log.info(f"  Item {leg_id}: sponsor {sponsor_name!r} -> {pol_uuid}")

        # If raw sponsor text but no sponsors parsed, try matching raw text directly
        if not matched_sponsors and raw_sponsor_text and not sponsors:
            pol_uuid = match_sponsor_name(raw_sponsor_text)
            if pol_uuid:
                matched_sponsors.append(pol_uuid)

        # Skip if no sponsor matched to a politician
        if not matched_sponsors:
            if verbose:
                log.info(f"  Item {leg_id}: sponsor text {raw_sponsor_text!r} — no DB match — SKIPPED")
            stats["skipped_no_match"] += 1
            continue

        external_id = f"onboard-{leg_id}"
        primary_sponsor_id = matched_sponsors[0]
        cosponsor_ids = matched_sponsors[1:] if len(matched_sponsors) > 1 else []

        if dry_run:
            log.info(
                f"  [DRY-RUN] Would import item {leg_id}: sponsor={primary_sponsor_id}, "
                f"cosponsors={len(cosponsor_ids)}, title={title!r}"
            )
            stats["imported"] += 1
            continue

        # Upsert bill
        cur.execute(
            """
            INSERT INTO essentials.legislative_bills
                (session_id, external_id, jurisdiction, title, summary, sponsor_id,
                 introduced_at, url, source)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (external_id, jurisdiction) DO UPDATE SET
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                sponsor_id = EXCLUDED.sponsor_id,
                url = EXCLUDED.url
            RETURNING id
            """,
            (
                session_id,
                external_id,
                JURISDICTION,
                title,
                description,
                primary_sponsor_id,
                item_url,
                SOURCE,
            ),
        )
        bill_row = cur.fetchone()
        bill_id = str(bill_row["id"])

        # Upsert cosponsors
        for cosponsor_uuid in cosponsor_ids:
            cur.execute(
                """
                INSERT INTO essentials.legislative_bill_cosponsors (bill_id, politician_id)
                VALUES (%s, %s)
                ON CONFLICT (bill_id, politician_id) DO NOTHING
                """,
                (bill_id, cosponsor_uuid),
            )

        conn.commit()
        stats["imported"] += 1

    cur.close()

    total_processed = stats["imported"] + stats["skipped_no_sponsor"] + stats["skipped_no_match"] + stats["errors"]
    log.info(
        f"Legislation import complete: {stats['imported']} imported, "
        f"{stats['skipped_no_sponsor']} skipped (no sponsor text), "
        f"{stats['skipped_no_match']} skipped (no DB match), "
        f"{stats['errors']} errors "
        f"(of {total_processed} total processed)"
    )
    return stats


# ---------------------------------------------------------------------------
# 9. main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Bloomington Common Council committee and legislation importer (OnBoard HTML scraping).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and log but do not write to database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed HTTP responses and match results",
    )
    parser.add_argument(
        "--committees-only",
        action="store_true",
        help="Only import committees and memberships (skip legislation)",
    )
    parser.add_argument(
        "--legislation-only",
        action="store_true",
        help="Only import legislation (assumes bridge already built)",
    )
    parser.add_argument(
        "--max-legislation",
        type=int,
        default=None,
        metavar="N",
        help="Limit legislation items processed (for testing)",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"Bloomington OnBoard importer v{SCRIPT_VERSION} starting ...")
    log.info(f"Dry-run: {args.dry_run}")
    log.info(f"Committees-only: {args.committees_only}")
    log.info(f"Legislation-only: {args.legislation_only}")
    if args.max_legislation:
        log.info(f"Max legislation: {args.max_legislation}")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL not set — cannot connect to database")
        sys.exit(1)

    # Connect to database
    try:
        conn = psycopg2.connect(database_url)
        log.info("Database connection established")
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        sys.exit(1)

    try:
        # Ensure session exists
        session_id = ensure_session(conn, dry_run=args.dry_run)
        log.info(f"Session ID: {session_id}")

        if not args.legislation_only:
            # Scrape all committee members (starting with main council)
            log.info("=== Phase 1: Scraping council members ===")
            main_members = scrape_council_members(committee_id=1, verbose=args.verbose)
            log.info(f"Main council: {len(main_members)} members scraped")

            # Build politician bridge
            log.info("=== Phase 2: Building politician bridge ===")
            bridge_map, name_to_uuid = build_politician_bridge(
                main_members, conn, dry_run=args.dry_run, verbose=args.verbose
            )

            # Import committees and memberships
            log.info("=== Phase 3: Importing committees and memberships ===")
            committee_uuid_map = import_committees(
                bridge_map, name_to_uuid, session_id, conn,
                dry_run=args.dry_run, verbose=args.verbose
            )

        if not args.committees_only:
            # For legislation-only: need to rebuild bridge from DB
            if args.legislation_only:
                log.info("=== Legislation-only mode: loading bridge from DB ===")
                bridge_map, name_to_uuid = _load_bridge_from_db(conn, args.verbose)

            # Import legislation
            log.info("=== Phase 4: Importing legislation ===")
            leg_stats = import_legislation(
                bridge_map, name_to_uuid, session_id, conn,
                dry_run=args.dry_run, verbose=args.verbose,
                max_items=args.max_legislation,
            )

        # Summary
        print("\n" + "=" * 60)
        print("BLOOMINGTON IMPORT COMPLETE")
        print("=" * 60)
        if not args.legislation_only:
            print(f"Session: {session_id}")
            print(f"Bridge: {len(bridge_map)} council members mapped")
        if not args.committees_only:
            print(f"Legislation: {leg_stats['imported']} imported")
            print(f"  Skipped (no sponsor text):  {leg_stats['skipped_no_sponsor']}")
            print(f"  Skipped (no DB match):       {leg_stats['skipped_no_match']}")
            print(f"  Errors:                      {leg_stats['errors']}")
        if args.dry_run:
            print("\n[DRY-RUN] No changes written to database.")

    finally:
        conn.close()
        log.info("Database connection closed")

    return 0


def _load_bridge_from_db(conn, verbose: bool = False) -> tuple:
    """
    Load bridge_map and name_to_uuid from DB for legislation-only mode.
    Returns (bridge_map, name_to_uuid) where bridge_map: onboard_id -> pol_uuid
    and name_to_uuid: pol_name_lower -> pol_uuid.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    bridge_map = {}
    name_to_uuid = {}

    cur.execute(
        """
        SELECT m.id_value, m.politician_id, p.full_name
        FROM essentials.legislative_politician_id_map m
        JOIN essentials.politicians p ON p.id = m.politician_id
        WHERE m.id_type = 'onboard'
        """
    )
    rows = cur.fetchall()
    for row in rows:
        bridge_map[row["id_value"]] = str(row["politician_id"])
        name_to_uuid[row["full_name"].lower()] = str(row["politician_id"])

    if verbose:
        log.info(f"  Loaded {len(bridge_map)} bridge entries from DB")

    cur.close()
    return bridge_map, name_to_uuid


if __name__ == "__main__":
    sys.exit(main())
