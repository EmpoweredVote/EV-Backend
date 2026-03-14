#!/usr/bin/env python3
"""
Fix orphaned government references — chambers whose government_id points to
nonexistent records in essentials.governments.

Discovered by standardize_metadata.py Rule 8: 308 chambers reference government
UUIDs that don't exist, affecting ~395 active politicians including all of Monroe
County's local government, judges, school boards, and townships.

Strategy:
  1. Group orphaned chambers by their government_id
  2. Infer the correct government name/type from chamber names and office titles
  3. Create missing government records (or reuse existing ones)
  4. UPDATE chambers to point to the correct government_id

Dry-run by default — use --apply to commit.

Usage:
    cd EV-Backend/scripts
    python3 fix_orphaned_governments.py                # dry-run, show plan
    python3 fix_orphaned_governments.py --apply        # commit fixes
"""

import argparse
import os
import re
import sys
import uuid
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
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set.")
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
# Fetch orphaned data
# ============================================================

ORPHAN_QUERY = """
SELECT c.id AS chamber_id,
       c.government_id AS orphan_gov_id,
       c.name AS chamber_name,
       c.name_formal AS chamber_name_formal,
       o.title AS office_title,
       o.normalized_position_name,
       o.representing_state,
       d.district_type,
       p.full_name,
       p.id AS politician_id
FROM essentials.chambers c
JOIN essentials.offices o ON o.chamber_id = c.id
JOIN essentials.politicians p ON p.id = o.politician_id
LEFT JOIN essentials.districts d ON o.district_id = d.id
LEFT JOIN essentials.governments g ON c.government_id = g.id
WHERE g.id IS NULL AND p.is_active = true
ORDER BY COALESCE(o.representing_state, ''), c.government_id, c.name
"""


def fetch_orphans(cur):
    """Fetch all orphaned chambers grouped by government_id."""
    cur.execute(ORPHAN_QUERY)
    rows = cur.fetchall()

    groups = defaultdict(list)
    for r in rows:
        groups[r["orphan_gov_id"]].append(r)

    return groups


# ============================================================
# Fetch existing governments (for reuse)
# ============================================================

def fetch_existing_governments(cur):
    """Fetch all existing government records for matching."""
    cur.execute("""
        SELECT id, name, type, state, city
        FROM essentials.governments
        ORDER BY state, name
    """)
    rows = cur.fetchall()

    # Index by lowercase name for matching
    by_name = {}
    for r in rows:
        by_name[(r["name"] or "").lower()] = r

    return rows, by_name


# ============================================================
# Classify orphaned government_ids
# ============================================================

def classify_in_gov(members):
    """Classify an Indiana orphaned government group.
    Returns (name, type, state, city) for the government record.
    """
    chamber_names = set(m["chamber_name"] or "" for m in members)
    titles = set(m["office_title"] or "" for m in members)
    formals = set(m["chamber_name_formal"] or "" for m in members)
    all_text = " ".join(chamber_names) + " " + " ".join(titles) + " " + " ".join(formals)
    all_lower = all_text.lower()

    # State-level judicial
    if any(kw in all_lower for kw in
           ["indiana supreme", "indiana appeals", "indiana tax court"]):
        return "State of Indiana", "State", "IN", None

    # Indianapolis/Marion consolidated city-county
    if "indianapolis" in all_lower or (
            "marion" in all_lower and ("city" in all_lower or "mayor" in all_lower)
            and "county" not in " ".join(chamber_names).lower().split("marion")[0]):
        # Check if this is city-level (mayor, council) vs county-level
        if "mayor" in all_lower or "city" in all_lower or "council" in all_lower:
            return "City of Indianapolis", "City", "IN", "Indianapolis"

    # Township pattern: "County: Township Board/Trustee"
    for cn in chamber_names:
        twp_match = re.match(r"(.+County):\s*(.+Township)", cn, re.IGNORECASE)
        if twp_match:
            twp_name = twp_match.group(2).strip()
            # Clean up: "Benton Township Board" -> "Benton Township"
            twp_name = re.sub(r"\s+(Board|Trustee|Assessor).*$", "", twp_name,
                              flags=re.IGNORECASE).strip()
            return twp_name, "Township", "IN", None

    # School district pattern
    for cn in chamber_names:
        cn_lower = cn.lower()
        if "school" in cn_lower:
            # Extract school district name from chamber name
            # "Monroe County Community School Board - District 1" -> base = "Monroe County Community School"
            # "Indianapolis Public School Board - District 1" -> base = "Indianapolis Public School"
            base = cn.split(" - ")[0].split(" Board")[0].strip()

            # Known school district name mappings
            school_name_map = {
                "monroe county community school": "Monroe County Community School Corporation",
                "indianapolis public school": "Indianapolis Public Schools",
                "eminence school": "Eminence Community School Corporation",
                "martinsville school": "Martinsville Community School Corporation",
                "richland-bean blossom school": "Richland-Bean Blossom Community School Corporation",
            }
            matched_name = school_name_map.get(base.lower())
            if matched_name:
                return matched_name, "School District", "IN", None

            # Fallback: add "Corporation" suffix
            if "school" in base.lower():
                return base + " Corporation", "School District", "IN", None
            return base + " Community School Corporation", "School District", "IN", None

    # City pattern: Common Council, City Clerk, City Mayor
    for cn in chamber_names:
        cn_lower = cn.lower()
        if "common council" in cn_lower or "city clerk" in cn_lower or "city mayor" in cn_lower:
            # Bloomington
            if any("bloomington" in t.lower() for t in titles | formals | chamber_names):
                return "City of Bloomington", "City", "IN", "Bloomington"
            return "City (unknown)", "City", "IN", None

    # Town pattern
    for cn in chamber_names:
        cn_lower = cn.lower()
        if "town council" in cn_lower or "town clerk" in cn_lower:
            if any("ellettsville" in t.lower() for t in titles | formals | chamber_names):
                return "Town of Ellettsville", "Town", "IN", "Ellettsville"
            if any("stinesville" in t.lower() for t in titles | formals | chamber_names):
                return "Town of Stinesville", "Town", "IN", "Stinesville"
            return "Town (unknown)", "Town", "IN", None

    # County pattern (catch-all for county officers, commissioners, council, judicial)
    # Look at each chamber name individually to extract just the county name
    for cn in chamber_names:
        # Match "X County" at the start of chamber name, before any officer title
        county_match = re.match(r"^((?:\w+\s+)*?County)\b", cn, re.IGNORECASE)
        if county_match:
            county_name = county_match.group(1).strip()
            return county_name, "County", "IN", None

    # Also check titles
    for t in titles:
        county_match = re.search(r"((?:\w+\s+)*?County)", t, re.IGNORECASE)
        if county_match:
            county_name = county_match.group(1).strip()
            return county_name, "County", "IN", None

    return "UNKNOWN", "UNKNOWN", "IN", None


def classify_ca_gov(members):
    """Classify a California orphaned government group."""
    formals = set(m["chamber_name_formal"] or "" for m in members)
    chambers = set(m["chamber_name"] or "" for m in members)

    for formal in formals:
        if not formal:
            continue
        # "Orange County Board of Supervisors" -> Orange County
        if "county" in formal.lower():
            county_name = formal.split(" Board")[0].strip()
            return f"{county_name}", "County", "CA", None
        # "Long Beach City Council" -> City of Long Beach
        city_name = (formal.replace("City Council", "")
                     .replace("city council", "").strip())
        if city_name:
            return f"City of {city_name}", "City", "CA", city_name

    # Fallback to chamber_name
    for cn in chambers:
        if "board of supervisors" in cn.lower():
            return "Orange County", "County", "CA", None

    return "UNKNOWN CA", "UNKNOWN", "CA", None


def classify_gov_group(orphan_gov_id, members):
    """Classify what government an orphaned group belongs to."""
    states = set(m["representing_state"] or "" for m in members)
    state = next(iter(states)) if len(states) == 1 else ""

    if state == "IN":
        return classify_in_gov(members)
    elif state == "CA":
        return classify_ca_gov(members)
    else:
        return "UNKNOWN", "UNKNOWN", state or "??", None


# ============================================================
# Build fix plan
# ============================================================

def build_fix_plan(orphan_groups, existing_by_name):
    """Build a plan of government creations and chamber updates.

    Returns:
        creates: list of dicts {id, name, type, state, city} for new governments
        updates: list of (chamber_id, new_gov_id) for chamber updates
        reuses:  list of (orphan_gov_id, existing_gov_id, name)
        unknowns: list of (orphan_gov_id, members) that couldn't be classified
    """
    creates = []
    updates = []
    reuses = []
    unknowns = []

    # Track which canonical government name maps to which ID (existing or newly created)
    canonical_to_id = {}

    # Pre-populate with existing governments
    for name_lower, gov in existing_by_name.items():
        canonical_to_id[name_lower] = gov["id"]

    for orphan_gid, members in orphan_groups.items():
        name, gtype, state, city = classify_gov_group(orphan_gid, members)

        if name == "UNKNOWN" or "unknown" in name.lower():
            unknowns.append((orphan_gid, name, gtype, members))
            continue

        # Normalize the name for matching against existing governments
        # Existing names follow pattern: "City of X, California, US" or "Monroe County, Indiana, US"
        state_names = {"IN": "Indiana", "CA": "California"}
        state_full = state_names.get(state, state)

        # Build candidate match names
        match_candidates = [
            name.lower(),
            f"{name}, {state_full}, US".lower(),
        ]
        if city:
            match_candidates.append(f"City of {city}, {state_full}, US".lower())
            match_candidates.append(f"city of {city}".lower())

        # Special case: counties should match "X County, Indiana, US" pattern
        if gtype == "County" and not name.endswith(", " + state_full + ", US"):
            match_candidates.append(f"{name}, {state_full}, US".lower())

        # Check if we already have a matching government
        matched_id = None
        for candidate in match_candidates:
            if candidate in canonical_to_id:
                matched_id = canonical_to_id[candidate]
                break

        if matched_id:
            # Reuse existing government
            reuses.append((orphan_gid, matched_id, name))
        else:
            # Need to create a new government
            # Use the formal naming convention: "Monroe County, Indiana, US"
            formal_name = f"{name}, {state_full}, US"

            # Check if we already created one with this canonical name
            if formal_name.lower() in canonical_to_id:
                matched_id = canonical_to_id[formal_name.lower()]
                reuses.append((orphan_gid, matched_id, name))
            else:
                new_id = str(uuid.uuid4())
                creates.append({
                    "id": new_id,
                    "name": formal_name,
                    "type": gtype,
                    "state": state,
                    "city": city,
                })
                canonical_to_id[formal_name.lower()] = new_id
                canonical_to_id[name.lower()] = new_id
                matched_id = new_id
                reuses.append((orphan_gid, new_id, name))

        # Build chamber updates — all chambers with this orphan_gov_id get repointed
        chamber_ids = set(m["chamber_id"] for m in members)
        for cid in chamber_ids:
            updates.append((cid, matched_id))

    return creates, updates, reuses, unknowns


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fix orphaned government references in essentials.chambers")
    parser.add_argument("--apply", action="store_true",
                        help="Apply fixes (default: dry-run)")
    args = parser.parse_args()

    load_env()

    print("=" * 70)
    print("Fix Orphaned Government References")
    print("=" * 70)
    print(f"  Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Fetch data
        orphan_groups = fetch_orphans(cur)
        existing_govs, existing_by_name = fetch_existing_governments(cur)

        print(f"  Orphaned government_ids: {len(orphan_groups)}")
        print(f"  Existing governments: {len(existing_govs)}")

        affected_politicians = set()
        for members in orphan_groups.values():
            for m in members:
                affected_politicians.add(m["politician_id"])
        print(f"  Affected active politicians: {len(affected_politicians)}")
        print()

        # Build plan
        creates, updates, reuses, unknowns = build_fix_plan(
            orphan_groups, existing_by_name)

        # Report: Governments to create
        print(f"  Governments to CREATE: {len(creates)}")
        for c in sorted(creates, key=lambda x: (x["state"], x["type"], x["name"])):
            print(f"    [{c['state']}] {c['type']:<20} {c['name']}")

        # Report: Governments reused (existing)
        reuse_existing = [(gid, eid, name) for gid, eid, name in reuses
                          if eid in {g["id"] for g in existing_govs}]
        reuse_new = [(gid, eid, name) for gid, eid, name in reuses
                     if eid not in {g["id"] for g in existing_govs}]

        print(f"\n  Chambers relinked to EXISTING governments: "
              f"{sum(1 for u in updates if u[1] in {g['id'] for g in existing_govs})}")
        seen = set()
        for gid, eid, name in sorted(reuse_existing, key=lambda x: x[2]):
            if eid not in seen:
                seen.add(eid)
                print(f"    -> {name} (existing gov_id={eid[:12]}...)")

        print(f"\n  Chambers relinked to NEW governments: "
              f"{sum(1 for u in updates if u[1] not in {g['id'] for g in existing_govs})}")

        print(f"\n  Total chamber UPDATE operations: {len(updates)}")

        # Report: Unknowns
        if unknowns:
            print(f"\n  UNRESOLVED ({len(unknowns)} — need manual review):")
            for gid, name, gtype, members in unknowns:
                people = [m["full_name"] for m in members]
                chambers = set(m["chamber_name"] for m in members)
                print(f"    gov_id={gid[:12]}... | inferred={name} | type={gtype}")
                print(f"      chambers: {chambers}")
                print(f"      people: {people[:3]}{'...' if len(people) > 3 else ''}")

        # Apply
        if args.apply:
            print("\n  Applying fixes...")

            # Create new governments
            if creates:
                insert_cur = conn.cursor()
                for c in creates:
                    insert_cur.execute(
                        "INSERT INTO essentials.governments (id, name, type, state, city) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (c["id"], c["name"], c["type"], c["state"], c["city"])
                    )
                print(f"    Created {len(creates)} government records")

            # Update chambers
            if updates:
                update_cur = conn.cursor()
                for chamber_id, new_gov_id in updates:
                    update_cur.execute(
                        "UPDATE essentials.chambers SET government_id = %s WHERE id = %s",
                        (new_gov_id, chamber_id)
                    )
                print(f"    Updated {len(updates)} chamber records")

            conn.commit()
            print("\n  DONE — all fixes committed.")

            # Verify
            cur.execute("""
                SELECT COUNT(DISTINCT c.id) AS orphan_count
                FROM essentials.chambers c
                JOIN essentials.offices o ON o.chamber_id = c.id
                JOIN essentials.politicians p ON p.id = o.politician_id
                LEFT JOIN essentials.governments g ON c.government_id = g.id
                WHERE g.id IS NULL AND p.is_active = true
            """)
            remaining = cur.fetchone()["orphan_count"]
            print(f"  Remaining orphaned chambers: {remaining}")

        else:
            print(f"\n  DRY-RUN: Would create {len(creates)} governments "
                  f"and update {len(updates)} chambers.")
            print("  Use --apply to commit.")

    except Exception as e:
        conn.rollback()
        print(f"\nError: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
