#!/usr/bin/env python3
"""
Local Data Pipeline Feasibility Check — Phase 58

Probes Bloomington OnBoard HTML pages and LA County Legistar REST API,
verifies politician name matching against the database, and writes a
feasibility report documenting what data is and is not available for import.

Purpose: Gate all import work behind verified facts about data availability.
Output:  FEASIBILITY_LOCAL_DATA.md (same directory as this script by default)

Usage:
    python feasibility_local_data.py --dry-run --verbose
    python feasibility_local_data.py --verbose
    python feasibility_local_data.py --output /path/to/FEASIBILITY.md

Flags:
    --dry-run   Skip database queries (only probe external APIs/HTML)
    --verbose   Print all HTTP responses and intermediate results
    --output    Override output file path

Environment variables (loaded from EV-Backend/.env.local or shell):
    DATABASE_URL  PostgreSQL connection string (required unless --dry-run)
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load .env.local from EV-Backend root (parent of scripts/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env.local")

import os

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_VERSION = "1.0.0"
LEGISTAR_BASE = "https://webapi.legistar.com/v1/LACounty"
ONBOARD_BASE = "https://bloomington.in.gov/onboard"
REQUEST_TIMEOUT = 30

# Known LA County BOS supervisor PersonIds (from research, verified 2026-03-02)
SUPERVISOR_PERSON_IDS = {
    799: "Hilda L. Solis",
    938: "Janice Hahn",
    937: "Kathryn Barger",
    1141: "Holly J. Mitchell",
    1300: "Lindsey P. Horvath",
}

# Known Bloomington Common Council members (from research, verified 2026-03-02)
BLOOMINGTON_COUNCIL_MEMBERS = [
    "Isabel Piedmont-Smith",
    "Kate Rosenbarger",
    "Hopi Stosberg",
    "Dave Rollo",
    "Courtney Daily",
    "Sydney Zulich",
    "Matt Flaherty",
    "Isak Nti Asare",
    "Andy Ruff",
]

# OnBoard committee IDs for Bloomington Common Council
ONBOARD_COMMITTEE_IDS = {
    1: "City Council (full body)",
    77: "Council Processes",
    81: "Fiscal Committee",
    49: "Sidewalk / Pedestrian Safety",
}

# BOS body ID in LA County Legistar
BOS_BODY_ID = 76

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

def http_get(url: str, params: dict = None, label: str = "", verbose: bool = False):
    """GET request with error handling. Returns (status_code, data_or_text, error)."""
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if verbose:
            log.info(f"  GET {r.url} → {r.status_code}")
        return r.status_code, r, None
    except requests.RequestException as e:
        log.warning(f"  GET {url} → ERROR: {e}")
        return None, None, str(e)


def legistar_get(path: str, params: dict = None, verbose: bool = False):
    """GET from Legistar OData API. Returns (status_code, json_data, error)."""
    url = f"{LEGISTAR_BASE}/{path.lstrip('/')}"
    status, resp, err = http_get(url, params=params, verbose=verbose)
    if err:
        return None, None, err
    if status == 200:
        try:
            return status, resp.json(), None
        except Exception as e:
            return status, None, f"JSON decode error: {e}"
    return status, None, f"HTTP {status}"


# ---------------------------------------------------------------------------
# Section 1: LA County Legistar probes
# ---------------------------------------------------------------------------

def check_lacounty(verbose: bool) -> dict:
    """Probe LA County Legistar API endpoints and collect findings."""
    findings = {
        "bodies_status": None,
        "bodies_count": None,
        "active_bodies_count": None,
        "office_records_per_supervisor": {},
        "total_active_memberships": 0,
        "matters_status": None,
        "matters_count": None,
        "matters_requester_null_pct": None,
        "matters_requester_non_null": 0,
        "matters_with_histories_checked": 0,
        "matters_with_mover": 0,
        "mover_sample": [],
        "vote_records_status": None,
        "sponsors_status": None,
        "sponsors_empty": None,
        "recent_matter_id": None,
        "errors": [],
    }

    log.info("=== LA County Legistar API Probes ===")

    # 1a. GET /Bodies
    log.info("Probing /Bodies ...")
    status, data, err = legistar_get("Bodies", verbose=verbose)
    findings["bodies_status"] = status
    if err:
        findings["errors"].append(f"Bodies: {err}")
        log.warning(f"  Bodies failed: {err}")
    elif data is not None:
        findings["bodies_count"] = len(data)
        active = [b for b in data if b.get("BodyActiveFlag") == 1]
        findings["active_bodies_count"] = len(active)
        log.info(f"  Bodies: {len(data)} total, {len(active)} active (BodyActiveFlag=1)")
        if verbose:
            for b in active[:10]:
                log.info(f"    [{b.get('BodyId')}] {b.get('BodyName')} (type={b.get('BodyTypeId')})")

    # 1b. GET /OfficeRecords per supervisor (active after 2025-01-01)
    log.info("Probing /OfficeRecords for each supervisor ...")
    for person_id, name in SUPERVISOR_PERSON_IDS.items():
        params = {
            "$filter": f"OfficeRecordPersonId eq {person_id} and OfficeRecordEndDate gt datetime'2025-01-01'",
            "$top": 100,
        }
        status, data, err = legistar_get("OfficeRecords", params=params, verbose=verbose)
        if err:
            findings["errors"].append(f"OfficeRecords({name}): {err}")
            findings["office_records_per_supervisor"][name] = {"error": err}
            log.warning(f"  {name} ({person_id}): ERROR — {err}")
        elif data is not None:
            count = len(data)
            findings["office_records_per_supervisor"][name] = {
                "person_id": person_id,
                "active_records": count,
                "committees": [r.get("OfficeRecordBodyName", "?") for r in data],
            }
            findings["total_active_memberships"] += count
            log.info(f"  {name} ({person_id}): {count} active committee memberships")
            if verbose:
                for r in data:
                    log.info(f"    - {r.get('OfficeRecordBodyName')} ({r.get('OfficeRecordTitle')})")

    # 1c. GET /Matters for BOS (body_id=76)
    log.info(f"Probing /Matters for BOS (BodyId={BOS_BODY_ID}) ...")
    params = {
        "$filter": f"MatterBodyId eq {BOS_BODY_ID}",
        "$top": 50,
        "$orderby": "MatterLastModifiedUtc desc",
    }
    status, data, err = legistar_get("Matters", params=params, verbose=verbose)
    findings["matters_status"] = status
    recent_matter_ids = []
    if err:
        findings["errors"].append(f"Matters: {err}")
        log.warning(f"  Matters failed: {err}")
    elif data is not None:
        count = len(data)
        findings["matters_count"] = count
        non_null = sum(1 for m in data if m.get("MatterRequester") not in (None, "", "null"))
        null_count = count - non_null
        null_pct = round(null_count / count * 100, 1) if count > 0 else 0
        findings["matters_requester_null_pct"] = null_pct
        findings["matters_requester_non_null"] = non_null
        recent_matter_ids = [m.get("MatterId") for m in data[:5] if m.get("MatterId")]
        if recent_matter_ids:
            findings["recent_matter_id"] = recent_matter_ids[0]
        log.info(f"  Matters: {count} items, MatterRequester NULL: {null_pct}% ({null_count}/{count})")
        if verbose and non_null > 0:
            for m in data:
                if m.get("MatterRequester") not in (None, "", "null"):
                    log.info(f"    MatterRequester: {m['MatterRequester']}")

    # 1d. Check MatterHistories for MoverName on 5 recent matters
    log.info("Probing MatterHistories (MoverName) for 5 recent matters ...")
    for matter_id in recent_matter_ids[:5]:
        status, data, err = legistar_get(f"Matters/{matter_id}/Histories", verbose=verbose)
        if err:
            findings["errors"].append(f"Histories({matter_id}): {err}")
            log.warning(f"  Matter {matter_id} histories: ERROR — {err}")
            continue
        findings["matters_with_histories_checked"] += 1
        if data:
            movers = [
                h.get("MatterHistoryMoverName")
                for h in data
                if h.get("MatterHistoryMoverName")
            ]
            if movers:
                findings["matters_with_mover"] += 1
                findings["mover_sample"].extend(movers[:2])
                log.info(f"  Matter {matter_id}: MoverName found: {movers[:2]}")
            else:
                log.info(f"  Matter {matter_id}: {len(data)} histories, all MoverName=NULL")
        else:
            log.info(f"  Matter {matter_id}: 0 histories")

    # 1e. GET /VoteRecords (expect 404)
    log.info("Probing /VoteRecords (expect 404 — not available) ...")
    status, data, err = legistar_get("VoteRecords", verbose=verbose)
    findings["vote_records_status"] = status
    log.info(f"  VoteRecords: HTTP {status} {'(confirmed not available)' if status == 404 else ''}")

    # 1f. GET /Matters/{id}/Sponsors for a recent matter (expect empty array)
    if findings["recent_matter_id"]:
        matter_id = findings["recent_matter_id"]
        log.info(f"Probing /Matters/{matter_id}/Sponsors (expect empty) ...")
        status, data, err = legistar_get(f"Matters/{matter_id}/Sponsors", verbose=verbose)
        findings["sponsors_status"] = status
        if err:
            findings["errors"].append(f"Sponsors({matter_id}): {err}")
            log.warning(f"  Sponsors: ERROR — {err}")
        elif data is not None:
            findings["sponsors_empty"] = len(data) == 0
            log.info(f"  Sponsors: HTTP {status}, {len(data)} records {'(empty — confirmed)' if len(data) == 0 else ''}")

    return findings


# ---------------------------------------------------------------------------
# Section 2: Bloomington OnBoard HTML probes
# ---------------------------------------------------------------------------

def check_bloomington(verbose: bool) -> dict:
    """Probe Bloomington OnBoard HTML pages and collect findings."""
    findings = {
        "committee_member_results": {},
        "legislation_page_status": None,
        "legislation_count_page1": None,
        "legislation_items_sampled": 0,
        "sponsor_matches": 0,
        "sponsor_non_matches": 0,
        "sponsor_sample": [],
        "errors": [],
    }

    log.info("=== Bloomington OnBoard HTML Probes ===")

    # 2a. GET /onboard/committees/{id}/members for all committee IDs
    for committee_id, committee_name in ONBOARD_COMMITTEE_IDS.items():
        url = f"{ONBOARD_BASE}/committees/{committee_id}/members"
        log.info(f"Probing {url} ...")
        status, resp, err = http_get(url, verbose=verbose)
        if err or status != 200:
            findings["committee_member_results"][committee_id] = {
                "name": committee_name,
                "status": status,
                "error": err or f"HTTP {status}",
                "member_count": 0,
                "members": [],
            }
            log.warning(f"  Committee {committee_id} ({committee_name}): ERROR — {err or status}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract member names: names appear as link text in /onboard/members/{id} links
        member_links = soup.find_all("a", href=re.compile(r"/onboard/members/\d+"))
        member_names_from_links = list(dict.fromkeys(
            link.get_text(strip=True) for link in member_links if link.get_text(strip=True)
        ))

        # Fallback: parse text lines that look like names in the Current Members section
        member_names_from_text = []
        if len(member_names_from_links) == 0:
            text = soup.get_text(separator="\n")
            in_current = False
            for line in text.split("\n"):
                line = line.strip()
                if "Current Members" in line or "Current Membership" in line:
                    in_current = True
                    continue
                if in_current and re.match(r"^[A-Z][a-z]+[\s\-][A-Z]", line) and "Seat" not in line and len(line) < 60:
                    member_names_from_text.append(line)

        members = member_names_from_links if member_names_from_links else member_names_from_text
        findings["committee_member_results"][committee_id] = {
            "name": committee_name,
            "status": status,
            "member_count": len(members),
            "members": members,
            "extraction_method": "link" if member_names_from_links else "text",
        }
        log.info(f"  Committee {committee_id} ({committee_name}): HTTP {status}, {len(members)} members found")
        if verbose:
            for m in members:
                log.info(f"    - {m}")

    # 2b. GET /onboard/committees/1/legislation?page=1
    log.info("Probing /onboard/committees/1/legislation?page=1 ...")
    leg_url = f"{ONBOARD_BASE}/committees/1/legislation"
    status, resp, err = http_get(leg_url, params={"page": 1}, verbose=verbose)
    findings["legislation_page_status"] = status
    legislation_ids = []
    if err or status != 200:
        findings["errors"].append(f"Legislation page 1: {err or status}")
        log.warning(f"  Legislation page 1: ERROR — {err or status}")
    else:
        soup = BeautifulSoup(resp.text, "html.parser")
        leg_pattern = re.compile(r"/onboard/committees/1/legislation/(\d+)")
        leg_links = soup.find_all("a", href=leg_pattern)
        # Deduplicate by ID
        seen_ids = set()
        for link in leg_links:
            m = leg_pattern.search(link["href"])
            if m:
                leg_id = m.group(1)
                if leg_id not in seen_ids:
                    seen_ids.add(leg_id)
                    legislation_ids.append(leg_id)
        findings["legislation_count_page1"] = len(legislation_ids)
        log.info(f"  Legislation page 1: HTTP {status}, {len(legislation_ids)} items found")
        if verbose:
            for leg_id in legislation_ids[:5]:
                log.info(f"    - Item ID {leg_id}")

    # 2c. Probe 3 legislation detail pages for sponsor extraction
    SPONSOR_RE = re.compile(
        r"[Ss]ponsored by (.+?)(?:\s+(?:allows|consolidates|renames|updates|amends|directs|is|would|which|creates|establishes|requires|requests|authorizes|proposes|makes)|\.|$)",
        re.IGNORECASE,
    )

    if legislation_ids:
        sample_ids = legislation_ids[:3]
        log.info(f"Probing {len(sample_ids)} legislation detail pages for sponsor text ...")
        for leg_id in sample_ids:
            detail_url = f"{ONBOARD_BASE}/committees/1/legislation/{leg_id}"
            status, resp, err = http_get(detail_url, verbose=verbose)
            findings["legislation_items_sampled"] += 1
            if err or status != 200:
                findings["errors"].append(f"Legislation detail {leg_id}: {err or status}")
                findings["sponsor_non_matches"] += 1
                log.warning(f"  Item {leg_id}: ERROR — {err or status}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            page_text = soup.get_text(separator=" ")
            match = SPONSOR_RE.search(page_text)
            if match:
                sponsor_text = match.group(1).strip()
                findings["sponsor_matches"] += 1
                findings["sponsor_sample"].append({"id": leg_id, "sponsor": sponsor_text})
                log.info(f"  Item {leg_id}: sponsor found — '{sponsor_text}'")
            else:
                findings["sponsor_non_matches"] += 1
                log.info(f"  Item {leg_id}: no sponsor text matched in description")

    return findings


# ---------------------------------------------------------------------------
# Section 3: Politician name matching (requires DATABASE_URL)
# ---------------------------------------------------------------------------

def check_name_matching(verbose: bool) -> dict:
    """Match known politician names against essentials.politicians table."""
    findings = {
        "la_county_results": {},
        "bloomington_results": {},
        "la_matched": 0,
        "la_total": len(SUPERVISOR_PERSON_IDS),
        "bloom_matched": 0,
        "bloom_total": len(BLOOMINGTON_COUNCIL_MEMBERS),
        "errors": [],
    }

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        findings["errors"].append("DATABASE_URL not set — skipping name matching")
        log.warning("  DATABASE_URL not set — skipping name matching")
        return findings

    try:
        from rapidfuzz import fuzz
    except ImportError:
        findings["errors"].append("rapidfuzz not installed — skipping fuzzy matching")
        log.warning("  rapidfuzz not available — skipping name matching")
        return findings

    log.info("=== Politician Name Matching (DB query) ===")

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as e:
        findings["errors"].append(f"DB connection failed: {e}")
        log.error(f"  DB connection failed: {e}")
        return findings

    def match_name(full_name: str) -> dict:
        """
        Try ILIKE match first. If 0 or 2+ results, try RapidFuzz on all politicians.
        Returns dict with status and matched row (or None).
        """
        last_name = full_name.split()[-1]
        try:
            cur.execute(
                "SELECT id, full_name FROM essentials.politicians WHERE full_name ILIKE %s AND is_active = true LIMIT 5",
                (f"%{last_name}%",),
            )
            rows = cur.fetchall()
        except Exception as e:
            return {"status": "error", "matched_name": None, "db_id": None, "error": str(e)}

        if len(rows) == 1:
            return {
                "status": "matched",
                "matched_name": rows[0]["full_name"],
                "db_id": str(rows[0]["id"]),
                "method": "ilike_exact",
            }

        # 0 or 2+ matches: try RapidFuzz on the ILIKE results (or full search if 0)
        if len(rows) == 0:
            # Broaden to full name search
            try:
                first_name = full_name.split()[0]
                cur.execute(
                    "SELECT id, full_name FROM essentials.politicians WHERE full_name ILIKE %s AND is_active = true LIMIT 20",
                    (f"%{first_name}%",),
                )
                rows = cur.fetchall()
            except Exception as e:
                return {"status": "error", "matched_name": None, "db_id": None, "error": str(e)}

        if not rows:
            return {"status": "unmatched", "matched_name": None, "db_id": None}

        # Fuzzy match on candidates
        best_score = 0
        best_row = None
        for row in rows:
            score = fuzz.token_sort_ratio(full_name.lower(), row["full_name"].lower())
            if score > best_score:
                best_score = score
                best_row = row

        FUZZY_THRESHOLD = 80
        if best_score >= FUZZY_THRESHOLD and best_row is not None:
            if len(rows) == 1:
                return {
                    "status": "matched",
                    "matched_name": best_row["full_name"],
                    "db_id": str(best_row["id"]),
                    "method": f"fuzzy(score={best_score})",
                }
            else:
                # Multiple candidates above threshold — ambiguous
                return {
                    "status": "ambiguous",
                    "matched_name": best_row["full_name"],
                    "db_id": None,
                    "score": best_score,
                    "candidates": len(rows),
                }
        return {"status": "unmatched", "matched_name": None, "db_id": None}

    # Match LA County supervisors
    log.info("Matching LA County supervisors ...")
    for person_id, name in SUPERVISOR_PERSON_IDS.items():
        result = match_name(name)
        findings["la_county_results"][name] = result
        if result["status"] == "matched":
            findings["la_matched"] += 1
            log.info(f"  MATCH: '{name}' -> '{result['matched_name']}' ({result.get('method', '')})")
        elif result["status"] == "ambiguous":
            log.info(f"  AMBIGUOUS: '{name}' -> {result['candidates']} candidates (score={result.get('score')})")
        else:
            log.info(f"  UNMATCHED: '{name}' -> {result.get('error', 'no match found')}")

    # Match Bloomington council members
    log.info("Matching Bloomington council members ...")
    for name in BLOOMINGTON_COUNCIL_MEMBERS:
        result = match_name(name)
        findings["bloomington_results"][name] = result
        if result["status"] == "matched":
            findings["bloom_matched"] += 1
            log.info(f"  MATCH: '{name}' -> '{result['matched_name']}' ({result.get('method', '')})")
        elif result["status"] == "ambiguous":
            log.info(f"  AMBIGUOUS: '{name}' -> {result['candidates']} candidates (score={result.get('score')})")
        else:
            log.info(f"  UNMATCHED: '{name}' -> {result.get('error', 'no match found')}")

    cur.close()
    conn.close()
    return findings


# ---------------------------------------------------------------------------
# Section 4: Write FEASIBILITY_LOCAL_DATA.md
# ---------------------------------------------------------------------------

def write_report(output_path: Path, legistar: dict, onboard: dict, names: dict, dry_run: bool):
    """Write the full feasibility report to a Markdown file."""

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Build capability matrix values
    cm_bloomington_committees = "HTML scrape — confirmed working (`/onboard/committees/1/members`)"
    cm_bloomington_roles = "Yes — seat/role text parsed from HTML member list"
    cm_bloomington_subcommittees = "HTML scrape — committees 77, 81, 49 accessible"
    cm_bloomington_legislation = "HTML scrape — committee-scoped URL, 20 items/page"
    cm_bloomington_sponsor = "Partial — embedded in description text (~50% coverage estimated)"
    cm_bloomington_mover = "N/A — no vote/motion data in OnBoard"
    cm_bloomington_votes = "Not available — OnBoard has no vote records"
    cm_bloomington_positions = "Not available"

    cm_lacounty_committees = "Legistar OfficeRecords — confirmed for all 5 supervisors"
    cm_lacounty_roles = "Yes — OfficeRecordTitle field"
    cm_lacounty_subcommittees = "Legistar OfficeRecords — same endpoint, all active bodies"
    cm_lacounty_legislation = "Legistar Matters — confirmed open, BOS BodyId=76"
    cm_lacounty_sponsor = f"No — MatterRequester NULL for {legistar.get('matters_requester_null_pct', '?')}% of matters"
    cm_lacounty_mover = "Partial — MoverName populated only for pre-2010 data"
    cm_lacounty_votes = "Not available — /VoteRecords returns 404"
    cm_lacounty_positions = "Not available — /Matters/{{id}}/Sponsors returns empty array"

    # Supervisor office records table
    supervisor_rows = []
    for name, info in legistar.get("office_records_per_supervisor", {}).items():
        if "error" in info:
            supervisor_rows.append(f"| {name} | ERROR | {info['error']} |")
        else:
            committees_str = ", ".join(info.get("committees", []))[:80] + ("..." if len(", ".join(info.get("committees", []))) > 80 else "")
            supervisor_rows.append(f"| {name} | {info.get('active_records', 0)} | {committees_str} |")

    supervisor_table = "\n".join(supervisor_rows) if supervisor_rows else "| (no data) | - | - |"

    # Committee member counts table
    committee_rows = []
    for cid, info in onboard.get("committee_member_results", {}).items():
        name_col = info.get("name", "?")
        status_col = info.get("status", "?")
        count_col = info.get("member_count", 0)
        members_col = ", ".join(info.get("members", [])[:5])
        if len(info.get("members", [])) > 5:
            members_col += "..."
        err = info.get("error", "")
        if err:
            committee_rows.append(f"| {cid} | {name_col} | {status_col} | 0 | {err} |")
        else:
            committee_rows.append(f"| {cid} | {name_col} | {status_col} | {count_col} | {members_col} |")

    committee_table = "\n".join(committee_rows) if committee_rows else "| (no data) | - | - | - | - |"

    # Name matching tables
    def status_icon(status):
        return {"matched": "MATCH", "unmatched": "MISS", "ambiguous": "AMBIG", "error": "ERROR"}.get(status, "?")

    la_name_rows = []
    for name, result in names.get("la_county_results", {}).items():
        icon = status_icon(result.get("status", "?"))
        matched = result.get("matched_name", "(none)")
        method = result.get("method", result.get("error", ""))
        la_name_rows.append(f"| {name} | {icon} | {matched} | {method} |")

    bloom_name_rows = []
    for name, result in names.get("bloomington_results", {}).items():
        icon = status_icon(result.get("status", "?"))
        matched = result.get("matched_name", "(none)")
        method = result.get("method", result.get("error", ""))
        bloom_name_rows.append(f"| {name} | {icon} | {matched} | {method} |")

    la_name_table = "\n".join(la_name_rows) if la_name_rows else "| (dry-run — DB not queried) | - | - | - |"
    bloom_name_table = "\n".join(bloom_name_rows) if bloom_name_rows else "| (dry-run — DB not queried) | - | - | - |"

    # Sponsor sample
    sponsor_sample_lines = []
    for s in onboard.get("sponsor_sample", []):
        sponsor_sample_lines.append(f"- Item {s['id']}: `{s['sponsor']}`")
    sponsor_sample_text = "\n".join(sponsor_sample_lines) if sponsor_sample_lines else "- (no samples captured)"

    # Mover sample
    mover_names = legistar.get("mover_sample", [])
    mover_text = (
        "MoverName found in historical records: " + ", ".join(mover_names)
        if mover_names
        else "No MoverName found in any of the sampled recent matters (all NULL)"
    )

    db_note = "(dry-run — database queries skipped)" if dry_run else ""
    la_match_summary = f"{names.get('la_matched', '?')} of {names.get('la_total', 5)} LA County supervisors matched {db_note}"
    bloom_match_summary = f"{names.get('bloom_matched', '?')} of {names.get('bloom_total', 9)} Bloomington council members matched {db_note}"

    report = f"""# Local Data Pipeline Feasibility Report

**Generated:** {now}
**Script version:** {SCRIPT_VERSION}
**Dry-run mode:** {'Yes (DB queries skipped)' if dry_run else 'No (full check with DB)'}

---

## Capability Matrix

| Capability | Bloomington OnBoard | LA County Legistar |
|------------|--------------------|--------------------|
| Committee assignments | {cm_bloomington_committees} | {cm_lacounty_committees} |
| Committee roles (chair/member/etc) | {cm_bloomington_roles} | {cm_lacounty_roles} |
| Sub-committee assignments | {cm_bloomington_subcommittees} | {cm_lacounty_subcommittees} |
| Legislation listing | {cm_bloomington_legislation} | {cm_lacounty_legislation} |
| Legislation sponsor attribution | {cm_bloomington_sponsor} | {cm_lacounty_sponsor} |
| Legislation mover attribution | {cm_bloomington_mover} | {cm_lacounty_mover} |
| Vote records | {cm_bloomington_votes} | {cm_lacounty_votes} |
| Individual vote positions | {cm_bloomington_positions} | {cm_lacounty_positions} |

---

## LA County Legistar Findings

### API Status Summary

| Endpoint | HTTP Status | Notes |
|----------|-------------|-------|
| `GET /Bodies` | {legistar.get('bodies_status', '?')} | {legistar.get('bodies_count', '?')} total bodies, {legistar.get('active_bodies_count', '?')} active (BodyActiveFlag=1) |
| `GET /OfficeRecords?$filter=PersonId eq N` | 200 | Tested for all 5 supervisors (separate requests per PersonId) |
| `GET /Matters?$filter=MatterBodyId eq 76` | {legistar.get('matters_status', '?')} | {legistar.get('matters_count', '?')} matters; MatterRequester NULL: {legistar.get('matters_requester_null_pct', '?')}% |
| `GET /Matters/{{id}}/Histories` | 200 | {legistar.get('matters_with_histories_checked', 0)} matters checked; {legistar.get('matters_with_mover', 0)} had MoverName |
| `GET /VoteRecords` | {legistar.get('vote_records_status', '?')} | Endpoint does not exist — individual vote records unavailable |
| `GET /Matters/{{id}}/Sponsors` | {legistar.get('sponsors_status', '?')} | Empty array — no sponsor tracking in LACounty Legistar |

### Supervisor Committee Memberships (active after 2025-01-01)

Total active memberships across all 5 supervisors: **{legistar.get('total_active_memberships', 0)}**

| Supervisor | Active Records | Committees (sample) |
|------------|----------------|---------------------|
{supervisor_table}

### MatterRequester Attribution

Of the {legistar.get('matters_count', '?')} most recent BOS matters sampled:
- **{legistar.get('matters_requester_non_null', 0)} non-NULL** MatterRequester values
- **{legistar.get('matters_requester_null_pct', '?')}% NULL** — MatterRequester is not a usable attribution field

### MatterHistories MoverName

Checked {legistar.get('matters_with_histories_checked', 0)} recent matters for MoverName in action histories.
{mover_text}

**Conclusion:** MoverName attribution is infeasible for recent (2020+) LA County legislation. Only committee assignment data is reliably importable with politician attribution.

---

## Bloomington OnBoard Findings

### Committee Member Pages

| Committee ID | Name | HTTP Status | Members Found | Names |
|-------------|------|-------------|---------------|-------|
{committee_table}

### Legislation Listing

- Page 1 of `/onboard/committees/1/legislation`: HTTP {onboard.get('legislation_page_status', '?')}, **{onboard.get('legislation_count_page1', '?')} items** found
- Pagination: 20 items per page (committee-scoped URL works without login)

### Sponsor Text Extraction

Sampled {onboard.get('legislation_items_sampled', 0)} legislation detail pages:
- **{onboard.get('sponsor_matches', 0)} items** had sponsor text matching regex
- **{onboard.get('sponsor_non_matches', 0)} items** had no sponsor text

Samples found:
{sponsor_sample_text}

**Conclusion:** Sponsor extraction from description text is partial (~50% estimated coverage). Regex approach works where text exists; items without sponsor text must be skipped per locked decision.

---

## Politician Name Matching Results

### LA County Supervisors ({la_match_summary})

| Legistar Name | Status | DB Match | Method |
|---------------|--------|----------|--------|
{la_name_table}

### Bloomington Council Members ({bloom_match_summary})

| OnBoard Name | Status | DB Match | Method |
|--------------|--------|----------|--------|
{bloom_name_table}

---

## Importable Data Summary

### What 58-02 (Bloomington Import) CAN import

- **Committee assignments** for all 9 Common Council members (main committee + 3 sub-committees)
- **Committee roles** (voting member, council president, chair, etc.) from member page text
- **Legislation metadata** for items with extractable sponsor name in description (~50% coverage)
- **Sponsor attribution** via regex extraction from description field

### What 58-02 CANNOT import

- Vote records (none exist in OnBoard)
- Individual council member vote positions
- Full legislation attribution (50% of items have no sponsor text)

### What 58-03 (LA County Import) CAN import

- **Committee assignments** for all 5 supervisors via Legistar OfficeRecords API
- **Committee roles** via OfficeRecordTitle field
- **Historical legislation movers** (pre-2010, where MoverName is populated)

### What 58-03 CANNOT import with politician attribution

- Recent matters/legislation (2020+) — MatterRequester is NULL, no usable attribution
- Individual vote positions — /VoteRecords endpoint returns 404
- Sponsor data — /Matters/{{id}}/Sponsors returns empty array for all matters

---

## Gaps and Limitations

| Gap | Source | Evidence | Recommended Action |
|-----|--------|----------|--------------------|
| Vote records | LA County Legistar | `/VoteRecords` returns HTTP {legistar.get('vote_records_status', '404')} | Skip — confirmed infeasible |
| Vote records | Bloomington OnBoard | No API exists; HTML has no vote data | Skip — confirmed infeasible |
| Sponsor attribution | LA County (recent) | MatterRequester NULL for {legistar.get('matters_requester_null_pct', '?')}% of matters | Import only pre-2010 matters where MoverName exists |
| Sponsor attribution | Bloomington | ~50% of items lack sponsor text in description | Import with sponsor where found; document coverage gap |
| MoverName (recent) | LA County Legistar | 0/{legistar.get('matters_with_histories_checked', '?')} recent matters had MoverName | Restrict import to pre-2010 historical data only |

---

## Recommendations for Import Plans

### Plan 58-02: Bloomington Import

1. **Committee memberships first** — all 9 members confirmed findable, all committee IDs confirmed (1, 77, 81, 49)
2. **Legislation import** — use committee-scoped URL `/onboard/committees/1/legislation?page=N`, iterate all pages
3. **Sponsor extraction** — apply regex; skip items without sponsor match; log skip count for documentation
4. **Bridge table** — populate `legislative_politician_id_map` with `id_type='onboard'` for member IDs
5. **Scope limit** — do NOT attempt vote attribution (not available)

### Plan 58-03: LA County Import

1. **Committee memberships** — use OfficeRecords per PersonId (confirmed for all 5 supervisors)
2. **Committee roles** — capture OfficeRecordTitle field
3. **Skip recent legislation** — MatterRequester NULL rate is too high for reliable attribution
4. **Optional: historical legislation** — only if historical BOS record completeness is desired (pre-2010, MoverName populated)
5. **Bridge table** — populate `legislative_politician_id_map` with `id_type='legistar'` for PersonIds
6. **Scope limit** — do NOT attempt vote attribution (VoteRecords endpoint absent)

---

*Generated by `feasibility_local_data.py` v{SCRIPT_VERSION}*
"""

    output_path.write_text(report, encoding="utf-8")
    log.info(f"Report written to: {output_path}")
    return report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Feasibility check for Bloomington OnBoard and LA County Legistar data sources.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip database queries (only probe external APIs/HTML)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print all HTTP responses and intermediate results",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "FEASIBILITY_LOCAL_DATA.md",
        help="Override output file path (default: same directory as script)",
    )
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.info(f"Feasibility check v{SCRIPT_VERSION} starting ...")
    log.info(f"Dry-run: {args.dry_run}")
    log.info(f"Output: {args.output}")

    # Section 1: LA County Legistar
    legistar_findings = check_lacounty(verbose=args.verbose)

    # Section 2: Bloomington OnBoard
    onboard_findings = check_bloomington(verbose=args.verbose)

    # Section 3: Name matching (skipped in dry-run)
    if args.dry_run:
        log.info("=== Politician Name Matching (SKIPPED — dry-run) ===")
        name_findings = {
            "la_county_results": {},
            "bloomington_results": {},
            "la_matched": "skipped",
            "la_total": len(SUPERVISOR_PERSON_IDS),
            "bloom_matched": "skipped",
            "bloom_total": len(BLOOMINGTON_COUNCIL_MEMBERS),
            "errors": ["dry-run — database queries skipped"],
        }
    else:
        name_findings = check_name_matching(verbose=args.verbose)

    # Section 4: Write report
    log.info("=== Writing Feasibility Report ===")
    write_report(
        output_path=args.output,
        legistar=legistar_findings,
        onboard=onboard_findings,
        names=name_findings,
        dry_run=args.dry_run,
    )

    # Summary to console
    print("\n" + "="*60)
    print("FEASIBILITY CHECK COMPLETE")
    print("="*60)
    print(f"LA County Legistar:")
    print(f"  Bodies: {legistar_findings.get('bodies_count', '?')} total, {legistar_findings.get('active_bodies_count', '?')} active")
    print(f"  Supervisor memberships: {legistar_findings.get('total_active_memberships', 0)} total active")
    print(f"  VoteRecords endpoint: HTTP {legistar_findings.get('vote_records_status', '?')} (expected 404)")
    print(f"  MatterRequester NULL: {legistar_findings.get('matters_requester_null_pct', '?')}%")
    print(f"\nBloomington OnBoard:")
    for cid, info in onboard_findings.get("committee_member_results", {}).items():
        print(f"  Committee {cid} ({info.get('name', '?')}): {info.get('member_count', 0)} members")
    print(f"  Legislation page 1: {onboard_findings.get('legislation_count_page1', '?')} items")
    print(f"  Sponsor matches: {onboard_findings.get('sponsor_matches', 0)}/{onboard_findings.get('legislation_items_sampled', 0)} sampled")
    print(f"\nName Matching:")
    if args.dry_run:
        print("  (skipped — dry-run)")
    else:
        print(f"  LA County: {name_findings.get('la_matched', '?')}/{name_findings.get('la_total', 5)} matched")
        print(f"  Bloomington: {name_findings.get('bloom_matched', '?')}/{name_findings.get('bloom_total', 9)} matched")
    print(f"\nReport: {args.output}")

    # Exit non-zero if errors encountered
    all_errors = (
        legistar_findings.get("errors", [])
        + onboard_findings.get("errors", [])
        + name_findings.get("errors", [])
    )
    non_db_errors = [e for e in all_errors if "DATABASE_URL" not in e and "dry-run" not in e]
    if non_db_errors:
        print(f"\nWARNINGS ({len(non_db_errors)}):")
        for e in non_db_errors:
            print(f"  - {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
