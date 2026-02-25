#!/usr/bin/env python3
"""
Seed essentials.position_descriptions from existing BallotReady office descriptions.

Scans essentials.offices + districts for rows where offices.description is non-empty,
groups by (normalized_position_name, district_type), picks the longest description
per group, and upserts into position_descriptions.

Requires: pip install psycopg2-binary
Usage: DATABASE_URL="postgresql://..." python3 seed_position_descriptions.py
  Or: automatically reads from ../.env.local
"""

import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras

# Register UUID adapter
psycopg2.extras.register_uuid()


def load_env():
    """Load DATABASE_URL from .env.local if not already set."""
    if os.getenv("DATABASE_URL"):
        return
    env_path = Path(__file__).resolve().parent.parent / ".env.local"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())
    if not os.getenv("DATABASE_URL"):
        print("ERROR: DATABASE_URL not set. Export it or create ../.env.local")
        sys.exit(1)


def connect():
    """Connect with proper handling for passwords with special chars."""
    raw_url = os.getenv("DATABASE_URL")
    parsed = urlparse(raw_url)
    if parsed.password and '@' in parsed.password:
        port = parsed.port or 5432
        return psycopg2.connect(
            host=parsed.hostname,
            port=port,
            dbname=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password,
            **dict(x.split('=', 1) for x in parsed.query.split('&') if '=' in x) if parsed.query else {}
        )
    return psycopg2.connect(raw_url)


def main():
    print("=" * 60)
    print("Seed Position Descriptions from BallotReady Data")
    print("=" * 60)

    load_env()
    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 1. Query existing office descriptions grouped by position + district type.
    #    Pick the longest description per group (most detailed).
    #    Also key by office title so offices without normalized_position_name can match.
    print("\nPhase 1: Extracting descriptions from existing offices...")
    cur.execute("""
        SELECT
            o.normalized_position_name,
            o.title,
            d.district_type,
            o.description,
            LENGTH(o.description) AS desc_len
        FROM essentials.offices o
        JOIN essentials.districts d ON d.id = o.district_id
        WHERE o.description IS NOT NULL
          AND o.description != ''
        ORDER BY o.normalized_position_name, d.district_type, desc_len DESC
    """)

    rows = cur.fetchall()
    print(f"  Found {len(rows)} offices with descriptions")

    # Group: pick the longest description per (name, district_type)
    specific = {}  # (name, district_type) -> description
    generic = {}   # name -> longest description across all district types

    def track(name, dt, desc):
        if not name:
            return
        key = (name, dt)
        if key not in specific or len(desc) > len(specific[key]):
            specific[key] = desc
        if name not in generic or len(desc) > len(generic[name]):
            generic[name] = desc

    for row in rows:
        dt = row['district_type'] or ''
        desc = row['description']
        # Key by normalized_position_name (BallotReady's standard name)
        track(row['normalized_position_name'], dt, desc)
        # Also key by office title (for offices without normalized_position_name)
        track(row['title'], dt, desc)
        # Extract short name from BallotReady "//" pattern:
        # "City Executive//Mayor" → "Mayor", "County Assessor//Property Appraiser" → "Property Appraiser"
        npn = row['normalized_position_name'] or ''
        if '//' in npn:
            short = npn.split('//')[-1].strip()
            if short:
                track(short, dt, desc)

    print(f"  Unique (position, district_type) pairs: {len(specific)}")
    print(f"  Unique position names (generic): {len(generic)}")

    # 2. Upsert into position_descriptions
    print("\nPhase 2: Upserting position descriptions...")

    upserted_specific = 0
    upserted_generic = 0

    # Insert specific (position + district_type) descriptions
    for (name, dt), desc in sorted(specific.items()):
        cur.execute("""
            INSERT INTO essentials.position_descriptions
                (id, normalized_position_name, district_type, description, source)
            VALUES (uuid_generate_v4(), %s, %s, %s, 'ballotready')
            ON CONFLICT (normalized_position_name, district_type)
            DO UPDATE SET description = EXCLUDED.description, source = 'ballotready'
            WHERE LENGTH(EXCLUDED.description) > LENGTH(essentials.position_descriptions.description)
        """, (name, dt, desc))
        if cur.rowcount > 0:
            upserted_specific += 1

    # Insert generic (position only, district_type = '') descriptions
    for name, desc in sorted(generic.items()):
        cur.execute("""
            INSERT INTO essentials.position_descriptions
                (id, normalized_position_name, district_type, description, source)
            VALUES (uuid_generate_v4(), %s, '', %s, 'ballotready')
            ON CONFLICT (normalized_position_name, district_type)
            DO UPDATE SET description = EXCLUDED.description, source = 'ballotready'
            WHERE LENGTH(EXCLUDED.description) > LENGTH(essentials.position_descriptions.description)
        """, (name, desc))
        if cur.rowcount > 0:
            upserted_generic += 1

    print(f"  Upserted {upserted_specific} specific descriptions")
    print(f"  Upserted {upserted_generic} generic descriptions")

    # 3. Summary
    cur.execute("SELECT COUNT(*) FROM essentials.position_descriptions")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT normalized_position_name, district_type,
               LEFT(description, 80) AS preview, source
        FROM essentials.position_descriptions
        ORDER BY normalized_position_name, district_type
        LIMIT 20
    """)
    sample = cur.fetchall()

    print(f"\nTotal position descriptions in table: {total}")
    print("\nSample entries:")
    for row in sample:
        dt_label = row['district_type'] or '(generic)'
        print(f"  {row['normalized_position_name']:30s} [{dt_label:15s}] {row['preview']}...")

    conn.commit()
    print("\nDone! All changes committed.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
