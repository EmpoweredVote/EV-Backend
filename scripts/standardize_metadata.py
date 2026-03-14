#!/usr/bin/env python3
"""
Essentials Data Standardization — audit and fix metadata inconsistencies.

Connects to the essentials schema in Supabase and applies state-aware
standardization rules to politician/office/chamber/district metadata.
Driven by gov_structure.json which defines canonical government structures
per state (bodies, positions, seat counts, district/ward conventions).

Dry-run by default — use --apply to commit fixes.

Usage:
    cd EV-Backend/scripts
    python3 standardize_metadata.py                    # dry-run, all regions
    python3 standardize_metadata.py --apply            # commit fixes
    python3 standardize_metadata.py --region monroe    # Monroe County only
    python3 standardize_metadata.py --region la        # LA County only
    python3 standardize_metadata.py --verbose          # show every row examined

Requires: pip install psycopg2-binary
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
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
# Gov structure config
# ============================================================

def load_gov_structure():
    """Load state government structure from gov_structure.json."""
    config_path = Path(__file__).parent / "gov_structure.json"
    with open(config_path) as f:
        return json.load(f)


# ============================================================
# Report
# ============================================================

class FixReport:
    """Accumulates fixes and warnings, prints summary."""

    def __init__(self):
        self.fixes = []
        self.warnings = []

    def add_fix(self, rule, table, row_id, field, old_val, new_val, context=""):
        self.fixes.append({
            "rule": rule,
            "table": table,
            "row_id": row_id,
            "field": field,
            "old": old_val,
            "new": new_val,
            "context": context,
        })

    def add_warning(self, rule, description, context=""):
        self.warnings.append({
            "rule": rule,
            "description": description,
            "context": context,
        })

    def print_summary(self):
        # Group by rule
        fix_counts = defaultdict(int)
        warn_counts = defaultdict(int)
        for f in self.fixes:
            fix_counts[f["rule"]] += 1
        for w in self.warnings:
            warn_counts[w["rule"]] += 1

        all_rules = sorted(set(list(fix_counts.keys()) + list(warn_counts.keys())))

        print()
        print("=" * 70)
        print("STANDARDIZATION REPORT")
        print("=" * 70)
        print(f"{'Rule':<45} {'Fixes':>8} {'Warnings':>8}")
        print("-" * 70)
        total_fixes = 0
        total_warnings = 0
        for rule in all_rules:
            fc = fix_counts.get(rule, 0)
            wc = warn_counts.get(rule, 0)
            total_fixes += fc
            total_warnings += wc
            print(f"  {rule:<43} {fc:>8} {wc:>8}")
        print("-" * 70)
        print(f"  {'TOTAL':<43} {total_fixes:>8} {total_warnings:>8}")
        print()

    def print_details(self):
        if self.fixes:
            print("FIXES:")
            for f in self.fixes:
                ctx = f" [{f['context']}]" if f["context"] else ""
                print(f"  [{f['rule']}] {f['table']}.{f['field']}: "
                      f"'{f['old']}' -> '{f['new']}'{ctx}")
            print()
        if self.warnings:
            print("WARNINGS:")
            for w in self.warnings:
                ctx = f" [{w['context']}]" if w["context"] else ""
                print(f"  [{w['rule']}] {w['description']}{ctx}")
            print()


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
    # Use state from government, or fall back to office.representing_state
    row_state = row["state"] or row.get("representing_state") or ""
    if filt.get("state") and row_state != filt["state"]:
        return False
    if filt.get("gov_name_contains"):
        gov_name = (row["gov_name"] or "").lower()
        title = (row["office_title"] or "").lower()
        if filt["gov_name_contains"].lower() not in gov_name:
            # Include state-level officials for that state
            if row_state == filt["state"]:
                return True
            # Also check if the office title references the region (e.g., "Monroe County" in title)
            if filt["gov_name_contains"].lower() in title:
                return True
            return False
    return True


# ============================================================
# Rule 1: Party Name Normalization
# ============================================================

PARTY_MAP = {
    "democrat": "Democratic",
    "democratic": "Democratic",
    "democratic party": "Democratic",
    "dem": "Democratic",
    "d": "Democratic",
    "republican": "Republican",
    "republican party": "Republican",
    "rep": "Republican",
    "gop": "Republican",
    "r": "Republican",
    "independent": "Independent",
    "ind": "Independent",
    "no party preference": "Independent",
    "nonpartisan": "Nonpartisan",
    "non-partisan": "Nonpartisan",
    "non partisan": "Nonpartisan",
    "np": "Nonpartisan",
    "n/a": "Nonpartisan",
    "libertarian": "Libertarian",
    "libertarian party": "Libertarian",
    "green": "Green",
    "green party": "Green",
    "constitution": "Constitution",
    "constitution party": "Constitution",
}


def rule_party_names(rows, report, verbose=False):
    """Normalize party name variations to canonical forms."""
    fixes = []
    for row in rows:
        party = (row["party"] or "").strip()
        if not party:
            continue
        canonical = PARTY_MAP.get(party.lower())
        if canonical and canonical != party:
            report.add_fix(
                "1-party-names", "essentials.politicians", row["politician_id"],
                "party", party, canonical, row["full_name"]
            )
            fixes.append((canonical, row["politician_id"]))
        elif verbose and not canonical:
            # Unknown party — just note it
            if party.lower() not in PARTY_MAP:
                report.add_warning(
                    "1-party-names",
                    f"Unknown party '{party}' for {row['full_name']}",
                    str(row["politician_id"]),
                )
    return fixes


# ============================================================
# Rule 2: Redundant Government Name in Titles
# ============================================================

def strip_gov_prefix(text, gov_name):
    """Remove government name prefix from text, handling common variants."""
    if not text or not gov_name:
        return text

    gov = gov_name.split(",")[0].strip()  # "Monroe County, Indiana, US" -> "Monroe County"

    # Build variant patterns
    variants = [gov]
    # "County of X" <-> "X County"
    if gov.endswith(" County"):
        core = gov[:-7]  # "Monroe"
        variants.append(f"County of {core}")
    elif gov.startswith("County of "):
        core = gov[10:]
        variants.append(f"{core} County")
    # "City of X" <-> "X"
    if gov.startswith("City of "):
        core = gov[8:]
        variants.append(core)

    for v in variants:
        # Check if text starts with the variant (case-insensitive)
        if text.lower().startswith(v.lower()):
            remainder = text[len(v):].lstrip(" -–—")
            if remainder:
                return remainder
        # Check if text contains the variant in parentheses
        paren_pattern = re.compile(r'\s*\(' + re.escape(v) + r'\)\s*', re.IGNORECASE)
        cleaned = paren_pattern.sub('', text).strip()
        if cleaned != text:
            return cleaned

    return text


def rule_redundant_gov_names(rows, report, verbose=False):
    """Strip redundant government names from office titles and chamber names."""
    title_fixes = []
    chamber_fixes = {}  # chamber_id -> (field, old, new) — deduplicate

    for row in rows:
        gov_name = row["gov_name"] or ""
        if not gov_name:
            continue

        # Check office title
        title = row["office_title"] or ""
        cleaned_title = strip_gov_prefix(title, gov_name)
        if cleaned_title != title and cleaned_title:
            report.add_fix(
                "2-redundant-gov-names", "essentials.offices", row["office_id"],
                "title", title, cleaned_title, row["full_name"]
            )
            title_fixes.append((cleaned_title, row["office_id"]))

        # Check chamber name (deduplicate — one chamber serves many politicians)
        cid = row["chamber_id"]
        if cid not in chamber_fixes:
            chamber_name = row["chamber_name"] or ""
            cleaned_name = strip_gov_prefix(chamber_name, gov_name)
            if cleaned_name != chamber_name and cleaned_name:
                report.add_fix(
                    "2-redundant-gov-names", "essentials.chambers", cid,
                    "name", chamber_name, cleaned_name,
                    f"chamber for {row['full_name']}"
                )
                chamber_fixes[cid] = ("name", cleaned_name)

    return title_fixes, chamber_fixes


# ============================================================
# Rule 3: Chamber Name Standardization
# ============================================================

# Common casing fixes for body names
CASING_FIXES = {
    "board of supervisors": "Board of Supervisors",
    "board of commissioners": "Board of Commissioners",
    "board of education": "Board of Education",
    "board of trustees": "Board of Trustees",
    "common council": "Common Council",
    "city council": "City Council",
    "town council": "Town Council",
    "county council": "County Council",
    "township board": "Township Board",
    "school board": "School Board",
    "board of aldermen": "Board of Aldermen",
}


def rule_chamber_names(rows, report, gov_config, verbose=False):
    """Standardize chamber name casing and remove parenthetical qualifiers."""
    chamber_fixes = {}

    for row in rows:
        cid = row["chamber_id"]
        if cid in chamber_fixes:
            continue

        name = row["chamber_name"] or ""
        name_formal = row["chamber_name_formal"] or ""

        new_name = name

        # Remove parenthetical government qualifiers
        paren_match = re.search(r'\s*\([^)]+\)\s*$', new_name)
        if paren_match:
            new_name = new_name[:paren_match.start()].strip()

        # Fix casing
        canonical = CASING_FIXES.get(new_name.lower())
        if canonical:
            new_name = canonical

        if new_name != name and new_name:
            report.add_fix(
                "3-chamber-names", "essentials.chambers", cid,
                "name", name, new_name,
                f"chamber for {row['full_name']}"
            )
            chamber_fixes[cid] = ("name", new_name)

        # Also fix name_formal casing
        if name_formal:
            new_formal = name_formal
            # Check if formal name body portion needs casing fix
            for pattern, fix in CASING_FIXES.items():
                if pattern in new_formal.lower():
                    idx = new_formal.lower().index(pattern)
                    new_formal = new_formal[:idx] + fix + new_formal[idx + len(pattern):]
                    break
            if new_formal != name_formal:
                report.add_fix(
                    "3-chamber-names", "essentials.chambers", cid,
                    "name_formal", name_formal, new_formal,
                    f"chamber for {row['full_name']}"
                )
                if cid not in chamber_fixes:
                    chamber_fixes[cid] = ("name_formal", new_formal)

    return chamber_fixes


# ============================================================
# Rule 4: School Board Position Standardization
# ============================================================

SCHOOL_POSITION_VARIANTS = {
    "trustee", "school board member", "board of education member",
    "board of trustees member", "school board trustee", "board member",
    "member", "director",
}

SCHOOL_LEADERSHIP_TITLES = {"president", "vice president", "clerk", "secretary"}


def rule_school_board_positions(rows, report, gov_config, verbose=False):
    """Normalize school board position names using state config."""
    fixes = []
    for row in rows:
        if row["district_type"] != "SCHOOL":
            continue

        state = row["state"]
        state_config = gov_config.get(state, {})
        school_config = state_config.get("school_district", {})
        canonical_position = school_config.get("position", "Board Member")

        title = (row["office_title"] or "").strip()
        norm_pos = (row["normalized_position_name"] or "").strip()

        # Check if normalized_position_name needs updating
        title_lower = title.lower()
        is_leadership = any(lt in title_lower for lt in SCHOOL_LEADERSHIP_TITLES)

        if norm_pos != canonical_position:
            # For leadership titles, keep office_title but fix normalized_position_name
            # For regular members, fix both if needed
            if is_leadership or title_lower in SCHOOL_POSITION_VARIANTS or not norm_pos:
                report.add_fix(
                    "4-school-board-positions", "essentials.offices", row["office_id"],
                    "normalized_position_name", norm_pos, canonical_position,
                    row["full_name"]
                )
                fixes.append(("normalized_position_name", canonical_position, row["office_id"]))

    return fixes


# ============================================================
# Rule 5: Township Data Standardization
# ============================================================

def rule_township_data(rows, report, gov_config, verbose=False):
    """Standardize township data for states that have townships."""
    gov_fixes = {}      # gov_id -> (field, new_val)
    chamber_fixes = {}  # chamber_id -> (field, new_val)
    office_fixes = []   # (field, new_val, office_id)

    for row in rows:
        state = row["state"]
        state_config = gov_config.get(state, {})
        twp_config = state_config.get("township", {})

        if not twp_config.get("exists"):
            continue

        gov_name = (row["gov_name"] or "").lower()
        gov_type = (row["gov_type"] or "").strip()

        # Only process township governments
        if "township" not in gov_name:
            continue

        gid = row["gov_id"]
        cid = row["chamber_id"]

        # Fix government type
        if gov_type != "Township" and gid not in gov_fixes:
            report.add_fix(
                "5-township-data", "essentials.governments", gid,
                "type", gov_type, "Township",
                row["full_name"]
            )
            gov_fixes[gid] = ("type", "Township")

        # Fix chamber name
        chamber_name = (row["chamber_name"] or "").strip()
        canonical_body = twp_config.get("legislative_body", {}).get("canonical_name", "Township Board")
        if not chamber_name and cid not in chamber_fixes:
            report.add_fix(
                "5-township-data", "essentials.chambers", cid,
                "name", chamber_name, canonical_body,
                f"chamber for {row['full_name']}"
            )
            chamber_fixes[cid] = ("name", canonical_body)

        # Fix normalized_position_name
        title = (row["office_title"] or "").lower()
        norm_pos = (row["normalized_position_name"] or "").strip()
        exec_title = twp_config.get("exec_title", "Trustee")
        board_position = twp_config.get("legislative_body", {}).get("position", "Board Member")

        if "trustee" in title and norm_pos != exec_title:
            report.add_fix(
                "5-township-data", "essentials.offices", row["office_id"],
                "normalized_position_name", norm_pos, exec_title,
                row["full_name"]
            )
            office_fixes.append(("normalized_position_name", exec_title, row["office_id"]))
        elif "board" in title and norm_pos != board_position:
            report.add_fix(
                "5-township-data", "essentials.offices", row["office_id"],
                "normalized_position_name", norm_pos, board_position,
                row["full_name"]
            )
            office_fixes.append(("normalized_position_name", board_position, row["office_id"]))

    return gov_fixes, chamber_fixes, office_fixes


# ============================================================
# Rule 6: Normalized Position Name Gap Fill
# ============================================================

POSITION_DERIVATION = [
    (r"\bmayor\b", "Mayor"),
    (r"\bcommissioner\b", "Commissioner"),
    (r"\bcouncil\s*(member|person|woman|man)\b", "Council Member"),
    (r"\bcouncilmember\b", "Council Member"),
    (r"\bcouncilor\b", "Council Member"),
    (r"\balder(man|person|woman)\b", "Council Member"),
    (r"\bsupervisor\b", "Supervisor"),
    (r"\bboard\s*member\b", "Board Member"),
    (r"\btrustee\b", "Trustee"),
    (r"\bclerk\b", "Clerk"),
    (r"\btreasurer\b", "Treasurer"),
    (r"\bassessor\b", "Assessor"),
    (r"\bsheriff\b", "Sheriff"),
    (r"\battorney\b", "County Attorney"),
    (r"\bprosecutor\b", "County Attorney"),
    (r"\bauditor\b", "Auditor"),
    (r"\brecorder\b", "Recorder"),
    (r"\bcoroner\b", "Coroner"),
    (r"\bsurveyor\b", "Surveyor"),
]


def rule_position_name_gap_fill(rows, report, gov_config, verbose=False):
    """Fill empty normalized_position_name from office_title patterns."""
    fixes = []
    for row in rows:
        norm_pos = (row["normalized_position_name"] or "").strip()
        if norm_pos:
            continue  # Never overwrite existing

        title = (row["office_title"] or "").strip()
        if not title:
            continue

        derived = None
        for pattern, position in POSITION_DERIVATION:
            if re.search(pattern, title, re.IGNORECASE):
                derived = position
                break

        if derived:
            report.add_fix(
                "6-position-gap-fill", "essentials.offices", row["office_id"],
                "normalized_position_name", "", derived,
                row["full_name"]
            )
            fixes.append((derived, row["office_id"]))

    return fixes


# ============================================================
# Rule 7: District/Ward Label & At-Large Standardization
# ============================================================

AT_LARGE_PATTERN = re.compile(r'\bat[\s-]?large\b', re.IGNORECASE)
DISTRICT_NUM_PATTERN = re.compile(r'(?:district|dist\.?|ward|seat)\s*#?\s*(\d+)', re.IGNORECASE)
BARE_NUM_PATTERN = re.compile(r'^#?\s*(\d+)\s*$')


def rule_district_labels(rows, report, gov_config, verbose=False):
    """Normalize district/ward labels and validate at-large vs district seats."""
    label_fixes = []
    # Group rows by chamber for seat count validation
    chamber_rows = defaultdict(list)

    for row in rows:
        label = (row["district_label"] or "").strip()
        title = (row["office_title"] or "").strip()
        state = row["state"]
        dt = row["district_type"] or ""

        # Skip federal/state — those labels are managed by provider data
        if dt.startswith("NATIONAL") or dt.startswith("STATE"):
            continue

        chamber_rows[row["chamber_id"]].append(row)

        if not label:
            continue

        # Determine expected seat_term from config
        state_config = gov_config.get(state, {})
        seat_term = None
        if dt == "COUNTY":
            for body in state_config.get("county", {}).get("legislative_bodies", []):
                seat_term = body.get("seat_term")
                break
        elif dt in ("LOCAL", "LOCAL_EXEC"):
            for body in state_config.get("municipality", {}).get("legislative_bodies", []):
                seat_term = body.get("seat_term")
                break
        elif dt == "SCHOOL":
            seat_term = state_config.get("school_district", {}).get("seat_term")

        # Normalize at-large labels
        if AT_LARGE_PATTERN.search(label):
            canonical = "At-Large"
            if label != canonical:
                report.add_fix(
                    "7-district-labels", "essentials.districts", row["district_id"],
                    "label", label, canonical, row["full_name"]
                )
                label_fixes.append((canonical, row["district_id"]))
            # Check for conflict: title says at-large but label has a number
            if DISTRICT_NUM_PATTERN.search(label):
                report.add_warning(
                    "7-district-labels",
                    f"Label '{label}' has both at-large and district number",
                    row["full_name"]
                )
            continue

        # Normalize district/ward number labels
        num_match = DISTRICT_NUM_PATTERN.search(label)
        if not num_match:
            num_match = BARE_NUM_PATTERN.search(label)

        if num_match and seat_term:
            num = int(num_match.group(1))
            canonical = f"{seat_term} {num}"
            if label != canonical:
                report.add_fix(
                    "7-district-labels", "essentials.districts", row["district_id"],
                    "label", label, canonical, row["full_name"]
                )
                label_fixes.append((canonical, row["district_id"]))

        # Flag conflict: title says "At-Large" but label has a district number
        if AT_LARGE_PATTERN.search(title) and num_match:
            report.add_warning(
                "7-district-labels",
                f"Title contains 'At-Large' but label is '{label}'",
                row["full_name"]
            )

    # Seat count validation (report only)
    for cid, crows in chamber_rows.items():
        if not crows:
            continue
        state = crows[0]["state"]
        dt = crows[0]["district_type"] or ""
        state_config = gov_config.get(state, {})

        # Find matching body config
        body_configs = []
        if dt == "COUNTY":
            body_configs = state_config.get("county", {}).get("legislative_bodies", [])
        elif dt in ("LOCAL", "LOCAL_EXEC"):
            body_configs = state_config.get("municipality", {}).get("legislative_bodies", [])

        chamber_name = (crows[0]["chamber_name"] or "").lower()
        for bc in body_configs:
            if bc["canonical_name"].lower() in chamber_name or chamber_name in bc["canonical_name"].lower():
                seats = bc.get("seats", {})
                expected_district = seats.get("district")
                expected_at_large = seats.get("at_large")

                if expected_district is None and expected_at_large is None:
                    break  # Unknown counts, skip

                actual_district = 0
                actual_at_large = 0
                for r in crows:
                    lbl = (r["district_label"] or "").lower()
                    if "at-large" in lbl or "at large" in lbl:
                        actual_at_large += 1
                    elif lbl:
                        actual_district += 1

                if expected_district is not None and actual_district != expected_district:
                    report.add_warning(
                        "7-district-labels",
                        f"Seat count mismatch: {bc['canonical_name']} expects "
                        f"{expected_district} district seats, found {actual_district}",
                        crows[0]["gov_name"]
                    )
                if expected_at_large is not None and actual_at_large != expected_at_large:
                    report.add_warning(
                        "7-district-labels",
                        f"Seat count mismatch: {bc['canonical_name']} expects "
                        f"{expected_at_large} at-large seats, found {actual_at_large}",
                        crows[0]["gov_name"]
                    )
                break

    return label_fixes


# ============================================================
# Rule 9: Judicial Data Standardization
# ============================================================

# Keywords that identify court level for classification
STATE_COURT_KEYWORDS = ["supreme", "appellate", "appeals", "court of appeal", "tax court"]
LOCAL_COURT_KEYWORDS = ["circuit", "superior", "district court", "municipal court",
                        "county court", "probate", "small claims", "city court",
                        "town court"]


def _find_judicial_level(chamber_name, title, state, gov_config):
    """Find the matching judicial hierarchy level from config."""
    # Check state config first, then federal
    for cfg_key in [state, "FEDERAL"]:
        state_config = gov_config.get(cfg_key, {})
        judicial_config = state_config.get("judicial", {})
        hierarchy = judicial_config.get("hierarchy", [])

        cn_lower = (chamber_name or "").lower()
        t_lower = (title or "").lower()
        for level in hierarchy:
            canon = level.get("canonical_name", "").lower()
            if canon and (canon in cn_lower or canon in t_lower):
                return level
    return None


def rule_judicial_data(rows, report, gov_config, verbose=False):
    """Standardize judicial data: titles, chamber names, position names.

    Sub-rules:
      9a. Strip "(Retain ...?)" suffixes from office titles (BallotReady artifact)
      9b. Normalize judicial position names (Justice vs Judge) using state config
      9c. Standardize judicial chamber names to canonical forms
      9d. Validate district_type = JUDICIAL and is_judicial flag consistency
      9e. Fill normalized_position_name for judicial officers
    """
    title_fixes = []
    chamber_fixes = {}
    office_fixes = []

    for row in rows:
        dt = (row["district_type"] or "").strip()
        title = (row["office_title"] or "").strip()
        chamber_name = (row["chamber_name"] or "").strip()
        norm_pos = (row["normalized_position_name"] or "").strip()
        state = row["state"] or row.get("representing_state") or ""
        is_judicial_flag = row.get("is_judicial", False)

        # Determine if this is a judicial record
        is_judicial_dt = dt == "JUDICIAL"
        has_judge_title = any(
            kw in title.lower()
            for kw in ["judge", "justice", "magistrate", "tax court"]
        )
        has_court_chamber = any(
            kw in chamber_name.lower()
            for kw in STATE_COURT_KEYWORDS + LOCAL_COURT_KEYWORDS
        )
        has_judicial_norm = any(
            kw in norm_pos.lower()
            for kw in ["judge", "justice"]
        ) if norm_pos else False

        if not (is_judicial_dt or is_judicial_flag or has_judge_title
                or has_court_chamber or has_judicial_norm):
            continue

        # 9a: Strip "(Retain ...?)" from title (BallotReady artifact)
        new_title = title
        retain_match = re.search(r'\s*\(Retain\s+.*?\)\s*$', new_title, re.IGNORECASE)
        if retain_match:
            new_title = new_title[:retain_match.start()].strip()
            if new_title != title:
                report.add_fix(
                    "9-judicial-data", "essentials.offices", row["office_id"],
                    "title", title, new_title, row["full_name"]
                )
                title_fixes.append((new_title, row["office_id"]))

        # Find matching judicial level from config
        level_config = _find_judicial_level(chamber_name, title, state, gov_config)

        if level_config:
            # 9b + 9e: Normalize position name using config
            working_title = (new_title or title).lower()
            chief_pos = (level_config.get("chief_position") or "").lower()
            canonical_pos = level_config.get("position", "Judge")

            if chief_pos and chief_pos in working_title:
                expected_pos = level_config["chief_position"]
            else:
                expected_pos = canonical_pos

            # Only fix normalized_position_name if it's empty or a generic variant
            if not norm_pos:
                report.add_fix(
                    "9-judicial-data", "essentials.offices", row["office_id"],
                    "normalized_position_name", norm_pos, expected_pos,
                    row["full_name"]
                )
                office_fixes.append(("normalized_position_name", expected_pos, row["office_id"]))

            # 9c: Standardize chamber name
            cid = row["chamber_id"]
            if cid and cid not in chamber_fixes:
                canon_name = level_config.get("canonical_name", "")

                # For county-level courts, strip redundant gov name from chamber
                if level_config.get("geo_scope") == "county" and canon_name:
                    gov_name = (row["gov_name"] or "")
                    if gov_name:
                        stripped = strip_gov_prefix(chamber_name, gov_name)
                        if stripped != chamber_name and stripped:
                            report.add_fix(
                                "9-judicial-data", "essentials.chambers", cid,
                                "name", chamber_name, stripped,
                                f"chamber for {row['full_name']}"
                            )
                            chamber_fixes[cid] = ("name", stripped)

                # For state-level courts, normalize to canonical
                elif canon_name and chamber_name and chamber_name != canon_name:
                    if (canon_name.lower() in chamber_name.lower()
                            or chamber_name.lower() in canon_name.lower()):
                        report.add_fix(
                            "9-judicial-data", "essentials.chambers", cid,
                            "name", chamber_name, canon_name,
                            f"chamber for {row['full_name']}"
                        )
                        chamber_fixes[cid] = ("name", canon_name)

        elif (has_judge_title or has_judicial_norm) and not norm_pos:
            # No config match but clearly judicial — fill position name generically
            if "justice" in title.lower():
                derived = "Justice"
            elif "chief judge" in title.lower():
                derived = "Chief Judge"
            elif "magistrate" in title.lower():
                derived = "Magistrate"
            else:
                derived = "Judge"

            report.add_fix(
                "9-judicial-data", "essentials.offices", row["office_id"],
                "normalized_position_name", "", derived,
                row["full_name"]
            )
            office_fixes.append(("normalized_position_name", derived, row["office_id"]))

        # 9d: Validate district_type consistency (report only)
        if has_court_chamber and dt and dt != "JUDICIAL" and dt != "COUNTY":
            report.add_warning(
                "9-judicial-data",
                f"Chamber '{chamber_name}' looks judicial but district_type='{dt}'",
                row["full_name"]
            )
        if is_judicial_dt and not has_court_chamber and not has_judge_title and not has_judicial_norm:
            report.add_warning(
                "9-judicial-data",
                f"district_type=JUDICIAL but title/chamber don't look judicial: "
                f"title='{title}', chamber='{chamber_name}'",
                row["full_name"]
            )

    return title_fixes, chamber_fixes, office_fixes


# ============================================================
# Rule 8: District Type Validation (report only)
# ============================================================

def rule_district_type_validation(rows, report, gov_config, verbose=False):
    """Flag district_type issues (report only, no auto-fix)."""
    seen_districts = set()
    seen_chambers = set()

    for row in rows:
        did = row["district_id"]
        if did and did not in seen_districts:
            seen_districts.add(did)

            dt = (row["district_type"] or "").strip()
            gov_type = (row["gov_type"] or "").strip()
            geo_id = (row["geo_id"] or "").strip()

            if not dt:
                report.add_warning(
                    "8-district-type-validation",
                    f"Empty district_type for {row['full_name']}",
                    str(did)
                )

            # County government but LOCAL district type
            if gov_type.lower() == "county" and dt == "LOCAL":
                report.add_warning(
                    "8-district-type-validation",
                    f"district_type=LOCAL but government type=County for {row['full_name']} "
                    f"(may need COUNTY)",
                    str(did)
                )

            # School district without geo_id
            if dt == "SCHOOL" and not geo_id:
                report.add_warning(
                    "8-district-type-validation",
                    f"SCHOOL district missing geo_id for {row['full_name']}",
                    str(did)
                )

        # Missing district entirely
        if not did:
            report.add_warning(
                "8-district-type-validation",
                f"Office has no district record for {row['full_name']}",
                str(row["office_id"])
            )

        # Orphaned government reference (critical data integrity issue)
        cid = row.get("chamber_id")
        chamber_gov_id = row.get("chamber_gov_id")
        gov_id = row.get("gov_id")
        if cid and cid not in seen_chambers:
            seen_chambers.add(cid)
            if chamber_gov_id and not gov_id:
                report.add_warning(
                    "8-district-type-validation",
                    f"Chamber '{row['chamber_name'] or 'unknown'}' references "
                    f"nonexistent government_id={chamber_gov_id} "
                    f"(orphaned — no state/gov_name available)",
                    row["full_name"]
                )


# ============================================================
# Main query and execution
# ============================================================

MAIN_QUERY = """
SELECT
    p.id AS politician_id, p.party, p.full_name, p.first_name, p.last_name,
    o.id AS office_id, o.title AS office_title, o.normalized_position_name,
    o.representing_state,
    c.id AS chamber_id, c.name AS chamber_name, c.name_formal AS chamber_name_formal,
    c.government_id AS chamber_gov_id,
    d.id AS district_id, d.district_type, d.label AS district_label, d.geo_id,
    d.is_judicial, d.retention,
    g.id AS gov_id, g.name AS gov_name, g.type AS gov_type, g.state
FROM essentials.politicians p
JOIN essentials.offices o ON o.politician_id = p.id
LEFT JOIN essentials.chambers c ON o.chamber_id = c.id
LEFT JOIN essentials.districts d ON o.district_id = d.id
LEFT JOIN essentials.governments g ON c.government_id = g.id
WHERE p.is_active = true
ORDER BY COALESCE(g.state, o.representing_state, ''), COALESCE(g.name, ''),
         COALESCE(d.district_type, ''), p.last_name
"""


def apply_fixes(cur, report, verbose=False):
    """Execute all accumulated fixes as UPDATE statements."""
    updates = defaultdict(list)  # table -> [(field, new_val, row_id)]

    for fix in report.fixes:
        updates[fix["table"]].append((fix["field"], fix["new"], fix["row_id"]))

    total = 0
    for table, rows in updates.items():
        for field, new_val, row_id in rows:
            cur.execute(
                f"UPDATE {table} SET {field} = %s WHERE id = %s",
                (new_val, row_id)
            )
            total += 1

    return total


def main():
    parser = argparse.ArgumentParser(description="Standardize essentials metadata")
    parser.add_argument("--apply", action="store_true",
                        help="Apply fixes (default: dry-run)")
    parser.add_argument("--region", choices=list(REGION_FILTERS.keys()),
                        help="Filter to a specific region")
    parser.add_argument("--verbose", action="store_true",
                        help="Show detailed output")
    args = parser.parse_args()

    load_env()
    gov_config = load_gov_structure()

    print("=" * 60)
    print("Essentials Data Standardization")
    print("=" * 60)
    print(f"  Mode:   {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"  Region: {args.region or 'all'}")
    print()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute(MAIN_QUERY)
        all_rows = cur.fetchall()
        print(f"  Loaded {len(all_rows)} active politician records")

        # Apply region filter
        rows = [r for r in all_rows if matches_region(r, args.region)]
        if args.region:
            print(f"  After region filter: {len(rows)} records")
        print()

        report = FixReport()

        # Run all rules
        print("Running Rule 1: Party Name Normalization...")
        party_fixes = rule_party_names(rows, report, args.verbose)
        print(f"  Found {len(party_fixes)} fixes")

        print("Running Rule 2: Redundant Government Name in Titles...")
        title_fixes, r2_chamber_fixes = rule_redundant_gov_names(rows, report, args.verbose)
        print(f"  Found {len(title_fixes)} title fixes, {len(r2_chamber_fixes)} chamber fixes")

        print("Running Rule 3: Chamber Name Standardization...")
        r3_chamber_fixes = rule_chamber_names(rows, report, gov_config, args.verbose)
        print(f"  Found {len(r3_chamber_fixes)} fixes")

        print("Running Rule 4: School Board Position Standardization...")
        school_fixes = rule_school_board_positions(rows, report, gov_config, args.verbose)
        print(f"  Found {len(school_fixes)} fixes")

        print("Running Rule 5: Township Data Standardization...")
        twp_gov_fixes, twp_chamber_fixes, twp_office_fixes = rule_township_data(
            rows, report, gov_config, args.verbose
        )
        print(f"  Found {len(twp_gov_fixes)} gov fixes, "
              f"{len(twp_chamber_fixes)} chamber fixes, "
              f"{len(twp_office_fixes)} office fixes")

        print("Running Rule 6: Normalized Position Name Gap Fill...")
        pos_fixes = rule_position_name_gap_fill(rows, report, gov_config, args.verbose)
        print(f"  Found {len(pos_fixes)} fixes")

        print("Running Rule 7: District/Ward Label & At-Large Standardization...")
        label_fixes = rule_district_labels(rows, report, gov_config, args.verbose)
        print(f"  Found {len(label_fixes)} fixes")

        print("Running Rule 8: District Type Validation...")
        rule_district_type_validation(rows, report, gov_config, args.verbose)
        print(f"  (report-only rule)")

        print("Running Rule 9: Judicial Data Standardization...")
        jud_title_fixes, jud_chamber_fixes, jud_office_fixes = rule_judicial_data(
            rows, report, gov_config, args.verbose
        )
        print(f"  Found {len(jud_title_fixes)} title fixes, "
              f"{len(jud_chamber_fixes)} chamber fixes, "
              f"{len(jud_office_fixes)} office fixes")

        # Print report
        if args.verbose:
            report.print_details()
        report.print_summary()

        # Apply or rollback
        if args.apply and report.fixes:
            # Switch to a regular cursor for updates
            update_cur = conn.cursor()
            total = apply_fixes(update_cur, report, args.verbose)
            conn.commit()
            print(f"APPLIED {total} fixes to database.")
        elif report.fixes:
            print(f"DRY-RUN: {len(report.fixes)} fixes would be applied. "
                  f"Use --apply to commit.")
        else:
            print("No fixes needed.")

    except Exception as e:
        conn.rollback()
        print(f"\nError: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
