#!/usr/bin/env python3
"""
Quick script to add a single politician headshot.

Usage:
    python scripts/add_headshot.py <politician_id> <image_url> [--license LICENSE]
"""

import sys
import os
import requests
from pathlib import Path

# Add scripts dir to path for utils
sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, load_supabase_env, get_engine, upload_photo_to_storage

from sqlalchemy import text


def add_headshot(politician_id, image_url, photo_license="scraped_no_license"):
    load_env()
    load_supabase_env()
    engine = get_engine()

    # Download image
    print(f"  Downloading: {image_url[:80]}...")
    resp = requests.get(image_url, timeout=30)
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
    ext_map = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}
    ext = ext_map.get(content_type, ".jpg")
    print(f"  Content-Type: {content_type} → ext: {ext}")

    # Upload to Supabase Storage
    storage_path = f"{politician_id}/default{ext}"
    print(f"  Uploading to storage: {storage_path}")
    cdn_url = upload_photo_to_storage(resp.content, storage_path, content_type)
    print(f"  CDN URL: {cdn_url}")

    # Upsert into politician_images
    with engine.begin() as conn:
        # Check for existing default image
        existing = conn.execute(
            text("SELECT id FROM essentials.politician_images WHERE politician_id = :pid AND type = 'default'"),
            {"pid": politician_id},
        ).fetchone()

        if existing:
            conn.execute(
                text("UPDATE essentials.politician_images SET url = :url, photo_license = :lic WHERE id = :id"),
                {"url": cdn_url, "lic": photo_license, "id": existing[0]},
            )
            print(f"  Updated existing record {existing[0]}")
        else:
            conn.execute(
                text("""
                    INSERT INTO essentials.politician_images (id, politician_id, url, type, photo_license)
                    VALUES (uuid_generate_v4(), :pid, :url, 'default', :lic)
                """),
                {"pid": politician_id, "url": cdn_url, "lic": photo_license},
            )
            print("  Inserted new record")

    # Fetch politician name for confirmation
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT first_name, last_name FROM essentials.politicians WHERE id = :pid"),
            {"pid": politician_id},
        ).fetchone()
        if row:
            print(f"\n  Done! Added headshot for {row[0]} {row[1]}")
        else:
            print(f"\n  Done! (politician_id {politician_id} not found in DB — image still uploaded)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/add_headshot.py <politician_id> <image_url> [--license LICENSE]")
        sys.exit(1)

    pid = sys.argv[1]
    url = sys.argv[2]
    lic = "scraped_no_license"
    if "--license" in sys.argv:
        idx = sys.argv.index("--license")
        lic = sys.argv[idx + 1]

    add_headshot(pid, url, lic)
