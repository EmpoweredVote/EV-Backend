#!/usr/bin/env python3
"""
migrate_old_committees.py

Migrates any existing state committee data from the old schema tables
(essentials.committees + essentials.politician_committees) to the v2026.3
schema tables (essentials.legislative_committees + essentials.legislative_committee_memberships).

This is a one-time migration script. The old tables remain intact after
migration — this script only reads from them and writes to the new tables.

If there is no state committee data in the old tables (the typical case after
the v2026.3 migration), the script exits cleanly with a "nothing to migrate"
message.

Usage:
    python migrate_old_committees.py --dry-run --verbose
    python migrate_old_committees.py --verbose
    python migrate_old_committees.py  # silent mode, summary only

Flags:
    --dry-run   Log operations without writing to database (safe to run first)
    --verbose   Enable DEBUG logging

Environment variables (loaded from EV-Backend/.env.local):
    DATABASE_URL   PostgreSQL connection string

Old schema (migration source):
    essentials.committees        (id, name, urls)
    essentials.politician_committees  (id, politician_id, committee_id, position)

New schema (migration target, v2026.3):
    essentials.legislative_committees
    essentials.legislative_committee_memberships

Migration strategy:
    - Joins politician_committees -> politicians -> offices -> districts
      WHERE district_type IN ('STATE_UPPER', 'STATE_LOWER') to identify
      state legislative committee data.
    - Derives jurisdiction from district.state (IN -> "indiana", CA -> "california").
    - Derives chamber from district_type (STATE_UPPER -> "senate", STATE_LOWER -> "house").
    - Maps position field to role (chair/vice_chair/member).
    - Uses ON CONFLICT DO NOTHING to be idempotent with fresh import data.
"""

import argparse
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Load .env.local from EV-Backend root (parent of scripts/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env.local")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Maps district.state (two-letter abbreviation) to v2026.3 jurisdiction string
STATE_TO_JURISDICTION = {
    "IN": "indiana",
    "CA": "california",
    "IL": "illinois",
    "NY": "new_york",
    "TX": "texas",
    "FL": "florida",
    "OH": "ohio",
    "PA": "pennsylvania",
    "GA": "georgia",
    "WA": "washington",
}

# Maps district_type to committee chamber field
DISTRICT_TYPE_TO_CHAMBER = {
    "STATE_UPPER": "senate",
    "STATE_LOWER": "house",
}

# Maps old position strings to v2026.3 role values
POSITION_TO_ROLE = {
    "chair": "chair",
    "co-chair": "chair",
    "co chair": "chair",
    "vice chair": "vice_chair",
    "vice-chair": "vice_chair",
    "vice_chair": "vice_chair",
    "ranking member": "ranking_member",
    "ranking_member": "ranking_member",
    "member": "member",
    "": "member",
}


def normalize_role(position: str) -> str:
    """Normalize an old position string to a v2026.3 role value."""
    if not position:
        return "member"
    normalized = position.strip().lower()
    return POSITION_TO_ROLE.get(normalized, "member")


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

def fetch_old_state_committees(conn) -> list:
    """
    Fetch all old committee data linked to state legislators.

    Joins: politician_committees -> politicians -> offices -> districts
    Filter: district_type IN ('STATE_UPPER', 'STATE_LOWER')

    Returns list of dicts with:
        committee_id, committee_name
        politician_id, position
        state (two-letter), district_type
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                c.id          AS committee_id,
                c.name        AS committee_name,
                pc.politician_id,
                pc.position,
                d.state,
                d.district_type
            FROM essentials.politician_committees pc
            JOIN essentials.committees c ON c.id = pc.committee_id
            JOIN essentials.politicians p ON p.id = pc.politician_id
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON d.id = o.district_id
            WHERE d.district_type IN ('STATE_UPPER', 'STATE_LOWER')
            ORDER BY c.id, pc.politician_id
            """
        )
        return [dict(row) for row in cur.fetchall()]


def check_new_committee_exists(conn, jurisdiction: str, name: str) -> Optional[str]:
    """
    Check if a committee with the same name+jurisdiction already exists in
    essentials.legislative_committees. Returns its UUID if found, else None.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM essentials.legislative_committees
            WHERE jurisdiction = %s AND name ILIKE %s
            LIMIT 1
            """,
            (jurisdiction, name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def insert_new_committee(
    conn,
    old_committee_id: str,
    jurisdiction: str,
    name: str,
    chamber: str,
    dry_run: bool,
) -> str:
    """
    Insert a new committee row into essentials.legislative_committees.
    Uses the old committee UUID as the external_id for traceability.
    Returns the new UUID (or a placeholder in dry-run mode).
    """
    new_id = str(uuid.uuid4())

    if dry_run:
        logging.info(
            f"  [DRY RUN] Would INSERT committee: name='{name}', "
            f"jurisdiction='{jurisdiction}', chamber='{chamber}', "
            f"external_id='{old_committee_id}', source='migrated'"
        )
        return new_id

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_committees
                (id, external_id, jurisdiction, name, type, chamber, source, is_current, session_id)
            VALUES (%s, %s, %s, %s, 'committee', %s, 'migrated', true, NULL)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (new_id, str(old_committee_id), jurisdiction, name, chamber),
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # ON CONFLICT fired — fetch the existing row
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


def insert_membership(
    conn,
    committee_id: str,
    politician_id: str,
    role: str,
    dry_run: bool,
) -> bool:
    """
    Insert a membership row into essentials.legislative_committee_memberships.
    Uses ON CONFLICT DO NOTHING to avoid duplicating fresh import data.
    Returns True if a row was written (or would be in dry-run), False if skipped.
    """
    if dry_run:
        logging.debug(
            f"  [DRY RUN] Would INSERT membership: committee={committee_id}, "
            f"politician={politician_id}, role='{role}'"
        )
        return True

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO essentials.legislative_committee_memberships
                (id, committee_id, politician_id, congress_number, role, is_current, session_id)
            VALUES (gen_random_uuid(), %s, %s, 0, %s, true, NULL)
            ON CONFLICT (committee_id, politician_id, congress_number) DO NOTHING
            """,
            (str(committee_id), str(politician_id), role),
        )
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Migration logic
# ---------------------------------------------------------------------------

def migrate(conn, dry_run: bool, verbose: bool) -> dict:
    """
    Main migration function. Returns a stats dict.
    """
    stats = {
        "committees_found": 0,
        "committees_migrated": 0,
        "committees_skipped_already_exists": 0,
        "committees_skipped_unknown_state": 0,
        "memberships_migrated": 0,
        "memberships_skipped_conflict": 0,
    }

    # 1. Fetch all old state committee data
    logging.info("Querying old essentials.politician_committees for state legislative data...")
    rows = fetch_old_state_committees(conn)

    if not rows:
        logging.info(
            "No state committee data found in old tables (essentials.politician_committees "
            "joined to STATE_UPPER/STATE_LOWER districts) — nothing to migrate."
        )
        return stats

    # 2. Group rows by committee to avoid duplicate committee inserts
    # committee_map: old_committee_id -> { state, district_type, committee_name, members: [...] }
    committee_map: dict = {}
    for row in rows:
        cid = str(row["committee_id"])
        if cid not in committee_map:
            committee_map[cid] = {
                "committee_name": row["committee_name"],
                "state": row["state"],
                "district_type": row["district_type"],
                "members": [],
            }
        committee_map[cid]["members"].append({
            "politician_id": str(row["politician_id"]),
            "position": row.get("position") or "",
        })

    stats["committees_found"] = len(committee_map)
    logging.info(f"Found {stats['committees_found']} distinct committee(s) with state legislative memberships")

    # 3. For each committee, create or find the v2026.3 record
    # new_committee_id_cache: old_committee_id -> new legislative_committee.id
    new_committee_id_cache: dict = {}

    for old_cid, committee_data in committee_map.items():
        name = committee_data["committee_name"]
        state = committee_data["state"]
        district_type = committee_data["district_type"]

        # Derive jurisdiction and chamber
        jurisdiction = STATE_TO_JURISDICTION.get(state)
        if not jurisdiction:
            logging.warning(
                f"  Unknown state '{state}' for committee '{name}' — skipping. "
                f"Add to STATE_TO_JURISDICTION if needed."
            )
            stats["committees_skipped_unknown_state"] += 1
            continue

        chamber = DISTRICT_TYPE_TO_CHAMBER.get(district_type, "joint")

        # Check if already exists in new table (by name+jurisdiction)
        existing_id = check_new_committee_exists(conn, jurisdiction, name)
        if existing_id:
            logging.debug(
                f"  Committee '{name}' ({jurisdiction}) already exists in new table "
                f"— using existing id {existing_id}"
            )
            new_committee_id_cache[old_cid] = existing_id
            stats["committees_skipped_already_exists"] += 1
        else:
            # Insert new committee row
            logging.info(f"  Migrating committee: '{name}' ({jurisdiction}/{chamber})")
            new_id = insert_new_committee(
                conn=conn,
                old_committee_id=old_cid,
                jurisdiction=jurisdiction,
                name=name,
                chamber=chamber,
                dry_run=dry_run,
            )
            new_committee_id_cache[old_cid] = new_id
            stats["committees_migrated"] += 1

        if not dry_run:
            conn.commit()

    # 4. For each membership, create the v2026.3 membership row
    for old_cid, committee_data in committee_map.items():
        new_cid = new_committee_id_cache.get(old_cid)
        if not new_cid:
            continue  # Committee was skipped due to unknown state

        for member in committee_data["members"]:
            politician_id = member["politician_id"]
            role = normalize_role(member["position"])

            written = insert_membership(
                conn=conn,
                committee_id=new_cid,
                politician_id=politician_id,
                role=role,
                dry_run=dry_run,
            )
            if written:
                stats["memberships_migrated"] += 1
                logging.debug(
                    f"  Membership: politician={politician_id}, committee={new_cid}, "
                    f"role='{role}'"
                )
            else:
                stats["memberships_skipped_conflict"] += 1
                logging.debug(
                    f"  Membership already exists (skipped): politician={politician_id}, "
                    f"committee={new_cid}"
                )

        if not dry_run:
            conn.commit()

    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Migrate state committee data from old essentials.committees / "
            "politician_committees tables to v2026.3 legislative_committees / "
            "legislative_committee_memberships tables."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log operations without writing to database (run this first)",
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

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error(
            "DATABASE_URL environment variable required. "
            "Set it in EV-Backend/.env.local or the shell environment."
        )
        sys.exit(1)

    logging.info("Starting old committee data migration to v2026.3 schema...")

    conn = psycopg2.connect(db_url)
    try:
        stats = migrate(conn=conn, dry_run=args.dry_run, verbose=args.verbose)

        logging.info(f"\n{'=' * 60}")
        if args.dry_run:
            logging.info("MIGRATION DRY RUN COMPLETE")
        else:
            logging.info("MIGRATION COMPLETE")
        logging.info(f"  Committees found in old tables:         {stats['committees_found']}")
        logging.info(f"  Committees migrated (new):              {stats['committees_migrated']}")
        logging.info(f"  Committees already in new table:        {stats['committees_skipped_already_exists']}")
        logging.info(f"  Committees skipped (unknown state):     {stats['committees_skipped_unknown_state']}")
        logging.info(f"  Memberships migrated:                   {stats['memberships_migrated']}")
        logging.info(f"  Memberships skipped (already exist):    {stats['memberships_skipped_conflict']}")
        logging.info(f"{'=' * 60}")

        if stats["committees_found"] == 0:
            logging.info(
                "Result: Old tables had no state committee data. "
                "This is expected if no data was loaded into essentials.politician_committees."
            )
        elif not args.dry_run and stats["committees_migrated"] == 0 and stats["memberships_migrated"] == 0:
            logging.info(
                "Result: All committees and memberships were already present in the new "
                "tables — no new rows needed."
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
