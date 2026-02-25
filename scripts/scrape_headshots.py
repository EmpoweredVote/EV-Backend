#!/usr/bin/env python3
"""
Config-driven headshot scraper for high-value LA County officials.

Downloads headshot images from verified source URLs in pipeline_config.json,
uploads each image to Supabase Storage CDN, and upserts CDN URLs with
photo_license values into essentials.politician_images.

Usage:
    cd EV-Backend/scripts
    python3 scrape_headshots.py                      # all groups
    python3 scrape_headshots.py --group supervisors  # supervisors only
    python3 scrape_headshots.py --group la_city_council
    python3 scrape_headshots.py --dry-run            # download only, no upload/DB write

Idempotency:
    Re-running is safe. Supabase Storage uses upsert=true (same CDN URL on overwrite).
    DB upsert checks for existing (politician_id, type='default') row — updates if found,
    inserts if not. No duplicate rows created on re-run.

Requirements:
    - DATABASE_URL and SUPABASE_URL/SUPABASE_SERVICE_KEY in EV-Backend/.env.local
    - essentials.politician_images.photo_license column (added Phase 39)
    - politician-photos bucket in Supabase Storage (verified Phase 39)
"""

import os
import sys
import json
import re
import time
import argparse
from pathlib import Path
from urllib.parse import urlparse

import requests
import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, load_supabase_env, load_pipeline_config, upload_photo_to_storage

# ============================================================
# Constants
# ============================================================

SUPERVISOR_STORAGE_PREFIX = "la_county/supervisors"
COUNCIL_STORAGE_PREFIX = "la_county/la_city_council"

# Browser User-Agent required for government CDNs (kc-usercontent.com checks User-Agent)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    # Required for kc-usercontent.com CDN (LA County BOS photos) — returns 403 without Referer
    "Referer": "https://bos.lacounty.gov/",
}

# Wikipedia requires a descriptive User-Agent per their API policy to avoid 429 rate limits.
# See: https://meta.wikimedia.org/wiki/User-Agent_policy
WIKIPEDIA_HEADERS = {
    "User-Agent": "EmpoweredVote/1.0 (https://empowered.vote; headshot-scraper) python-requests/2.32",
}

# Rate limit: 1 second between requests to avoid overwhelming government servers
DOWNLOAD_DELAY_SECONDS = 1.0

# Longer delay for Wikipedia CDN to respect their rate limits (429 seen at 1s intervals)
WIKIPEDIA_DELAY_SECONDS = 2.0


# ============================================================
# Image download
# ============================================================

def download_image(url, timeout=15):
    """Download image bytes from URL with appropriate headers per domain.

    Uses Wikipedia-specific User-Agent for wikimedia.org URLs (required by
    Wikipedia's User-Agent policy to avoid 429 rate limiting).
    Uses browser User-Agent with Referer for government CDN URLs.

    Args:
        url: Direct image URL (.jpg, .png, .webp, etc.)
        timeout: Request timeout in seconds (default 15)

    Returns:
        tuple: (image_bytes: bytes, content_type: str)
            content_type is derived from response Content-Type header.
            Defaults to "image/jpeg" if header is missing or not an image/* type.

    Raises:
        requests.HTTPError: If server returns 4xx/5xx
        requests.RequestException: If connection fails or times out
    """
    # Use Wikipedia-specific headers for wikimedia.org URLs
    if "wikimedia.org" in url or "wikipedia.org" in url:
        headers = WIKIPEDIA_HEADERS
    else:
        headers = BROWSER_HEADERS

    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    # Derive content-type from response header, not URL extension
    # Strip parameters like "; charset=utf-8" -> "image/jpeg"
    raw_ct = resp.headers.get("Content-Type", "image/jpeg")
    content_type = raw_ct.split(";")[0].strip()

    # Fallback to image/jpeg if header is missing or not an image/* type
    if not content_type.startswith("image/"):
        content_type = "image/jpeg"

    return resp.content, content_type


# ============================================================
# DB helpers
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Handles passwords with special characters by parsing the URL and
    connecting with keyword arguments. Consistent with scrape_la_officials.py.
    """
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set. Call load_env() first.")
        sys.exit(1)

    parsed = urlparse(raw_url)
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }
    # Pass through extra query params (e.g., sslmode=require for Supabase)
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v

    return psycopg2.connect(**kwargs)


def find_politician_id(cur, full_name):
    """Lookup politician UUID by full_name (case-insensitive).

    Matches exact full_name against essentials.politicians. Uses the exact
    names from FALLBACK_LA_COUNTY_SUPERVISORS / FALLBACK_LA_CITY_COUNCIL in
    scrape_la_officials.py to ensure consistent matching.

    Args:
        cur: psycopg2 RealDictCursor
        full_name: Exact politician name string (e.g., "Hilda L. Solis")

    Returns:
        str: UUID string if found, None if not found in DB
    """
    cur.execute("""
        SELECT p.id
        FROM essentials.politicians p
        WHERE p.full_name ILIKE %s
          AND p.is_active = true
        ORDER BY p.last_synced DESC
        LIMIT 1
    """, (full_name,))
    row = cur.fetchone()
    return str(row["id"]) if row else None


def upsert_politician_image(cur, politician_id, cdn_url, photo_license):
    """Upsert a single default headshot for a politician.

    Checks for an existing (politician_id, type='default') row in
    essentials.politician_images. If found: UPDATE url and photo_license.
    If not found: INSERT a new row with gen_random_uuid().

    This satisfies the idempotency requirement: re-running does not create
    duplicate rows for the same politician.

    Args:
        cur: psycopg2 RealDictCursor
        politician_id: UUID string from essentials.politicians
        cdn_url: Supabase Storage public CDN URL
        photo_license: License string (e.g., "scraped_no_license", "press_use", "cc_by_sa_4.0")

    Returns:
        str: "updated" if existing row was updated, "inserted" if new row was created
    """
    cur.execute("""
        SELECT id FROM essentials.politician_images
        WHERE politician_id = %s AND type = 'default'
        LIMIT 1
    """, (politician_id,))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
            UPDATE essentials.politician_images
            SET url = %s, photo_license = %s
            WHERE id = %s
        """, (cdn_url, photo_license, existing["id"]))
        return "updated"
    else:
        cur.execute("""
            INSERT INTO essentials.politician_images
                (id, politician_id, url, type, photo_license)
            VALUES (gen_random_uuid(), %s, %s, 'default', %s)
        """, (politician_id, cdn_url, photo_license))
        return "inserted"


# ============================================================
# Core processing
# ============================================================

def process_headshot(cur, entry, storage_prefix, dry_run=False):
    """Download, upload, and upsert a single politician headshot.

    Main per-politician logic:
    1. Skip if photo_url is None/empty (null entries in config)
    2. Lookup politician UUID in DB by full_name
    3. Download image bytes with browser headers
    4. Upload to Supabase Storage (skip if dry_run)
    5. Upsert CDN URL + photo_license into politician_images (skip if dry_run)

    Args:
        cur: psycopg2 RealDictCursor (used for DB lookups and upserts)
        entry: dict from pipeline_config.json headshots array with keys:
               name, photo_url, photo_license, storage_filename
        storage_prefix: Supabase Storage prefix (e.g., "la_county/supervisors")
        dry_run: If True, download image but skip upload and DB write

    Returns:
        dict: Result with keys: name, status, cdn_url (None on skip/error)
    """
    name = entry.get("name", "")
    photo_url = entry.get("photo_url")
    photo_license = entry.get("photo_license", "scraped_no_license")
    storage_filename = entry.get("storage_filename")

    # Step 1: Skip null photo_url entries
    if not photo_url:
        note = entry.get("note", "no photo_url configured")
        print(f"  SKIP: {name} — {note}")
        return {"name": name, "status": "skipped", "cdn_url": None}

    # Step 2: Find politician UUID in DB
    politician_id = find_politician_id(cur, name)
    if not politician_id:
        print(f"  WARN: {name} — not found in DB (is_active=true), skipping")
        return {"name": name, "status": "not_found", "cdn_url": None}

    # Step 3: Download image
    try:
        image_bytes, content_type = download_image(photo_url)
        print(f"  Downloaded: {name} ({len(image_bytes):,} bytes, {content_type})")
    except requests.HTTPError as e:
        print(f"  ERROR: {name} — HTTP {e.response.status_code} downloading {photo_url}")
        return {"name": name, "status": "error", "cdn_url": None}
    except requests.RequestException as e:
        print(f"  ERROR: {name} — {e} downloading {photo_url}")
        return {"name": name, "status": "error", "cdn_url": None}

    if dry_run:
        print(f"  DRY-RUN: {name} — would upload to {storage_prefix}/{storage_filename}")
        return {"name": name, "status": "dry_run", "cdn_url": None}

    # Step 4: Upload to Supabase Storage
    storage_path = f"{storage_prefix}/{storage_filename}"
    try:
        cdn_url = upload_photo_to_storage(image_bytes, storage_path, content_type)
        print(f"  Uploaded: {name} -> {cdn_url}")
    except Exception as e:
        print(f"  ERROR: {name} — upload failed: {e}")
        return {"name": name, "status": "error", "cdn_url": None}

    # Step 5: Upsert into politician_images
    try:
        action = upsert_politician_image(cur, politician_id, cdn_url, photo_license)
        print(f"  DB {action}: {name} (license={photo_license})")
    except Exception as e:
        print(f"  ERROR: {name} — DB upsert failed: {e}")
        return {"name": name, "status": "error", "cdn_url": cdn_url}

    return {"name": name, "status": action, "cdn_url": cdn_url}


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape and upload headshots for high-value LA County officials"
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Pipeline config region key (default: default_region from pipeline_config.json)"
    )
    parser.add_argument(
        "--group",
        choices=["supervisors", "la_city_council", "all"],
        default="all",
        help="Which group to process (default: all)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download images but skip Supabase upload and DB writes"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 40 — Scrape Headshots for LA County Officials")
    print("=" * 60)
    if args.dry_run:
        print("  DRY-RUN MODE: no uploads or DB writes")
    print()

    # Load environment variables
    load_env()
    if not args.dry_run:
        load_supabase_env()

    # Load pipeline config for the target region
    region_config = load_pipeline_config(args.region)
    headshots_config = region_config.get("headshots")
    if not headshots_config:
        print("Error: No 'headshots' section in pipeline_config.json for this region")
        sys.exit(1)

    # Connect to DB
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Counters
    total = {"processed": 0, "uploaded": 0, "updated": 0, "inserted": 0, "skipped": 0, "errors": 0, "dry_run": 0}

    try:
        # Process supervisors
        if args.group in ("supervisors", "all"):
            print("Processing: LA County Supervisors")
            print("-" * 40)
            supervisors = headshots_config.get("supervisors", [])
            for i, entry in enumerate(supervisors):
                if i > 0:
                    photo_url = entry.get("photo_url") or ""
                    delay = WIKIPEDIA_DELAY_SECONDS if ("wikimedia.org" in photo_url or "wikipedia.org" in photo_url) else DOWNLOAD_DELAY_SECONDS
                    time.sleep(delay)
                try:
                    result = process_headshot(cur, entry, SUPERVISOR_STORAGE_PREFIX, dry_run=args.dry_run)
                    total["processed"] += 1
                    status = result["status"]
                    if status in ("updated", "inserted"):
                        total["uploaded"] += 1
                        total[status] += 1
                    elif status == "skipped" or status == "not_found":
                        total["skipped"] += 1
                    elif status == "error":
                        total["errors"] += 1
                    elif status == "dry_run":
                        total["dry_run"] += 1
                except Exception as e:
                    print(f"  ERROR processing {entry.get('name', '?')}: {e}")
                    import traceback
                    traceback.print_exc()
                    total["errors"] += 1

            if not args.dry_run:
                conn.commit()
                print(f"\n  Committed supervisor headshots.")
            print()

        # Process LA City Council
        if args.group in ("la_city_council", "all"):
            print("Processing: LA City Council Members")
            print("-" * 40)
            council = headshots_config.get("la_city_council", [])
            for i, entry in enumerate(council):
                if i > 0:
                    photo_url = entry.get("photo_url") or ""
                    delay = WIKIPEDIA_DELAY_SECONDS if ("wikimedia.org" in photo_url or "wikipedia.org" in photo_url) else DOWNLOAD_DELAY_SECONDS
                    time.sleep(delay)
                try:
                    result = process_headshot(cur, entry, COUNCIL_STORAGE_PREFIX, dry_run=args.dry_run)
                    total["processed"] += 1
                    status = result["status"]
                    if status in ("updated", "inserted"):
                        total["uploaded"] += 1
                        total[status] += 1
                    elif status == "skipped" or status == "not_found":
                        total["skipped"] += 1
                    elif status == "error":
                        total["errors"] += 1
                    elif status == "dry_run":
                        total["dry_run"] += 1
                except Exception as e:
                    print(f"  ERROR processing {entry.get('name', '?')}: {e}")
                    import traceback
                    traceback.print_exc()
                    total["errors"] += 1

            if not args.dry_run:
                conn.commit()
                print(f"\n  Committed council headshots.")
            print()

    except Exception as e:
        conn.rollback()
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    # Summary
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total processed : {total['processed']}")
    if args.dry_run:
        print(f"  Would upload    : {total['dry_run']}")
    else:
        print(f"  Uploaded        : {total['uploaded']}")
        print(f"    Inserted      : {total['inserted']}")
        print(f"    Updated       : {total['updated']}")
    print(f"  Skipped         : {total['skipped']}")
    print(f"  Errors          : {total['errors']}")

    # Exit code: 1 if all attempted uploads failed (excluding skips)
    attempted = total["processed"] - total["skipped"]
    if attempted > 0 and total["errors"] == attempted:
        print("\nERROR: All upload attempts failed.")
        sys.exit(1)
    elif total["errors"] > 0:
        print(f"\nWARNING: {total['errors']} errors occurred.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
