#!/usr/bin/env python3
"""
Quick-8: Upload Monroe County Council member photos to Supabase and update photo_custom_url.

Source: https://www.in.gov/counties/monroe/government/council/
Photos downloaded to /tmp/monroe_council_photos/

Run: python3 upload_monroe_council_photos.py
"""

import sys
import os
import psycopg2
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, load_supabase_env, get_supabase_client

load_env()
load_supabase_env()

DATABASE_URL = os.getenv("DATABASE_URL")
BUCKET = "politician_photos"
STORAGE_PREFIX = "monroe_council"

# Map: local filename -> (slug, display name)
MEMBERS = [
    ("Liz-Feitl.png",        "liz-feitl-monroe-county-council",          "Liz Feitl"),
    ("Peter-Iversen.png",    "peter-iversen-monroe-county-council-d1",   "Peter Iversen"),
    ("Kate-Wiltz.png",       "kate-wiltz-monroe-county-council-d2",      "Kate Wiltz"),
    ("Marty-Hawk.png",       "marty-hawk-monroe-county-council-d3",      "Marty Hawk"),
    ("Jennifer-Crossley.png","jennifer-crossley-monroe-county-council-d4","Jennifer Crossley"),
]

PHOTO_DIR = Path("/tmp/monroe_council_photos")


def upload_and_update():
    client = get_supabase_client()
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    results = []

    for filename, slug, display_name in MEMBERS:
        photo_path = PHOTO_DIR / filename
        if not photo_path.exists():
            print(f"  MISSING file: {photo_path}")
            results.append((display_name, slug, None, "FILE_MISSING"))
            continue

        storage_path = f"{STORAGE_PREFIX}/{filename}"
        print(f"  Uploading {display_name} -> {storage_path}...")

        with open(photo_path, "rb") as f:
            image_bytes = f.read()

        try:
            # upsert=true: safe to re-run
            client.storage.from_(BUCKET).upload(
                storage_path,
                image_bytes,
                {"content-type": "image/png", "upsert": "true"},
            )
            cdn_url = client.storage.from_(BUCKET).get_public_url(storage_path)
            print(f"    CDN URL: {cdn_url}")

            # Update politician record
            cur.execute("""
                UPDATE essentials.politicians
                SET photo_custom_url = %s
                WHERE slug = %s
                RETURNING id, full_name, photo_custom_url
            """, (cdn_url, slug))
            row = cur.fetchone()
            if row:
                print(f"    DB updated: {row[1]} (id={row[0]})")
                conn.commit()
                results.append((display_name, slug, cdn_url, "OK"))
            else:
                print(f"    WARNING: No politician found with slug={slug}")
                conn.rollback()
                results.append((display_name, slug, cdn_url, "POLITICIAN_NOT_FOUND"))

        except Exception as e:
            print(f"    ERROR: {e}")
            conn.rollback()
            results.append((display_name, slug, None, f"ERROR: {e}"))

    cur.close()
    conn.close()
    return results


if __name__ == "__main__":
    print("=" * 60)
    print("Monroe County Council Photo Uploader")
    print("=" * 60)

    results = upload_and_update()

    print("\n--- Summary ---")
    ok = 0
    for name, slug, url, status in results:
        print(f"  {name}: {status}")
        if status == "OK":
            ok += 1

    print(f"\n{ok}/{len(MEMBERS)} photos uploaded and linked.")
    if ok < len(MEMBERS):
        sys.exit(1)
