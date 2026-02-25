#!/usr/bin/env python3
"""
Fetch building photos from Wikimedia Commons for top 20 LA County cities,
upload to Supabase Storage CDN, and upsert metadata into essentials.building_photos.

Usage:
    cd EV-Backend/scripts
    python3 fetch_building_photos.py                # fetch all 11 cities
    python3 fetch_building_photos.py --dry-run      # fetch + print, no upload/DB write
    python3 fetch_building_photos.py --region la_county

Idempotency:
    Re-running is safe. Supabase Storage uses upsert=true (same CDN URL on overwrite).
    DB upsert uses ON CONFLICT (place_geoid) DO UPDATE — no duplicate rows on re-run.

Requirements:
    - DATABASE_URL, SUPABASE_URL, SUPABASE_SERVICE_KEY in EV-Backend/.env.local
    - essentials.building_photos table (created Phase 39)
    - politician-photos bucket in Supabase Storage (verified Phase 39)
    - pipeline_config.json building_photos section (added Phase 41-01)
"""

import os
import sys
import json
import re
import time
import datetime
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

WIKIMEDIA_API = "https://commons.wikimedia.org/w/api.php"

# Wikipedia requires a descriptive User-Agent per their API policy.
# See: https://meta.wikimedia.org/wiki/User-Agent_policy
WIKIMEDIA_HEADERS = {
    "User-Agent": "EmpoweredVote/1.0 (https://empowered.vote; building-photos) python-requests/2.32"
}

BUILDING_PHOTO_STORAGE_PREFIX = "la_county/building_photos"

# Rate limit: Wikimedia API allows ~1 request/second for unauthenticated calls
DOWNLOAD_DELAY = 1.0

# Wikimedia LicenseShortName → internal license enum
# Source: commons.wikimedia.org extmetadata API field values
LICENSE_MAP = {
    "Public domain": "public_domain",
    "CC BY-SA 4.0": "cc_by_sa_4.0",
    "CC BY-SA 3.0": "cc_by_sa_3.0",
    "CC BY 2.5": "cc_by_2.5",
    "CC0": "cc0",
    "CC BY 4.0": "cc_by_4.0",
    "CC BY 3.0": "cc_by_3.0",
    "CC BY 2.0": "cc_by_2.0",
}


# ============================================================
# Wikimedia Commons API
# ============================================================

def fetch_wikimedia_image_info(wiki_title):
    """Fetch image URL and license info from Wikimedia Commons API.

    Calls the Wikimedia Commons imageinfo API with extmetadata to get
    the full-resolution image URL, license short name, and artist attribution.

    Args:
        wiki_title: Wikimedia file title e.g. "Torrance_CA_City_Hall.jpg"
                    (without the "File:" prefix)

    Returns:
        dict with keys: url, license, attribution, wiki_title
        Returns None if file not found or imageinfo missing.
    """
    params = {
        "action": "query",
        "prop": "imageinfo",
        "format": "json",
        "iiprop": "url|extmetadata",
        "titles": f"File:{wiki_title}",
    }
    try:
        resp = requests.get(WIKIMEDIA_API, params=params, headers=WIKIMEDIA_HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ERROR: Wikimedia API request failed for {wiki_title}: {e}")
        return None

    data = resp.json()
    pages = data.get("query", {}).get("pages", {})

    for page_id, page in pages.items():
        if page_id == "-1":
            # File not found on Wikimedia Commons
            return None
        imageinfo = page.get("imageinfo", [])
        if not imageinfo:
            return None
        ii = imageinfo[0]
        em = ii.get("extmetadata", {})

        # Strip HTML tags from Artist field — Wikimedia returns HTML like:
        # <a href="//commons.wikimedia.org/wiki/User:Foo">Foo</a>
        artist_html = em.get("Artist", {}).get("value", "")
        artist = re.sub(r'<[^>]+>', '', artist_html).strip()

        # Normalize Wikimedia license string to internal enum
        wiki_license = em.get("LicenseShortName", {}).get("value", "unknown")
        license_val = LICENSE_MAP.get(wiki_license, wiki_license.lower().replace(" ", "_").replace("-", "_"))

        return {
            "url": ii.get("url", ""),
            "license": license_val,
            "attribution": artist,
            "wiki_title": wiki_title,
        }

    return None


# ============================================================
# Image download
# ============================================================

def download_image(url, timeout=30):
    """Download image bytes from a Wikimedia Commons URL.

    Uses the Wikimedia-specific User-Agent (required by Wikimedia policy
    to avoid 429 rate limiting on upload.wikimedia.org CDN).

    Args:
        url: Direct image URL from Wikimedia imageinfo API
        timeout: Request timeout in seconds (default 30 — large images can be slow)

    Returns:
        tuple: (image_bytes: bytes, content_type: str)
            content_type from response Content-Type header.
            Defaults to "image/jpeg" if header is missing or not image/*.

    Raises:
        requests.HTTPError: If server returns 4xx/5xx
        requests.RequestException: If connection fails or times out
    """
    resp = requests.get(url, headers=WIKIMEDIA_HEADERS, timeout=timeout)
    resp.raise_for_status()

    # Derive content-type from response header, not URL extension
    # Some files end in .JPG but are served as image/jpeg by Wikimedia
    raw_ct = resp.headers.get("Content-Type", "image/jpeg")
    content_type = raw_ct.split(";")[0].strip()

    # Fallback: if not an image/* type, default to image/jpeg
    if not content_type.startswith("image/"):
        content_type = "image/jpeg"

    return resp.content, content_type


# ============================================================
# DB helpers
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Handles passwords with special characters by parsing the URL and
    connecting with keyword arguments. Same pattern as scrape_headshots.py.
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


def upsert_building_photo(cur, place_geoid, cdn_url, source_url, license_val, attribution, wiki_title):
    """Upsert a row into essentials.building_photos.

    Idempotent: ON CONFLICT (place_geoid) DO UPDATE overwrites all fields.
    place_geoid is the PRIMARY KEY so this is safe for re-runs.

    Args:
        cur: psycopg2 cursor
        place_geoid: Census Place GEOID string (e.g., "0644000" for LA City)
        cdn_url: Supabase Storage public CDN URL (stable after upsert=true upload)
        source_url: Original Wikimedia Commons full image URL (for attribution)
        license_val: Internal license enum (e.g., "public_domain", "cc_by_sa_4.0")
        attribution: Plain-text artist attribution from Wikimedia extmetadata
        wiki_title: Wikimedia file title (for provenance tracking)
    """
    cur.execute("""
        INSERT INTO essentials.building_photos
            (place_geoid, url, source_url, license, attribution, wiki_title, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (place_geoid) DO UPDATE SET
            url = EXCLUDED.url,
            source_url = EXCLUDED.source_url,
            license = EXCLUDED.license,
            attribution = EXCLUDED.attribution,
            wiki_title = EXCLUDED.wiki_title,
            fetched_at = EXCLUDED.fetched_at
    """, (
        place_geoid,
        cdn_url,
        source_url,
        license_val,
        attribution,
        wiki_title,
        datetime.datetime.utcnow(),
    ))


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fetch building photos from Wikimedia Commons for top 20 LA County cities"
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Pipeline config region key (default: default_region from pipeline_config.json)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch image info and download but skip Supabase upload and DB writes"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 41 — Fetch Building Photos for LA County Cities")
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
    building_photos_config = region_config.get("building_photos")
    if not building_photos_config:
        print("Error: No 'building_photos' section in pipeline_config.json for this region")
        sys.exit(1)

    print(f"  Cities to process: {len(building_photos_config)}")
    print()

    # Connect to DB (even in dry-run — used for verification query at end)
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Counters
    total = {
        "processed": 0,
        "uploaded": 0,
        "skipped": 0,
        "errors": 0,
        "dry_run": 0,
    }
    cdn_urls = {}  # place_geoid -> cdn_url for summary output

    try:
        for i, entry in enumerate(building_photos_config):
            city = entry.get("city", "Unknown")
            place_geoid = entry.get("place_geoid", "")
            wiki_title = entry.get("wiki_title", "")
            config_license = entry.get("license", "unknown")

            print(f"[{i+1}/{len(building_photos_config)}] {city} ({place_geoid})")
            print(f"  Wikimedia: {wiki_title}")

            if i > 0:
                # Rate limit between Wikimedia API calls (1 req/sec max)
                time.sleep(DOWNLOAD_DELAY)

            # Step 1: Fetch image info from Wikimedia Commons API
            info = fetch_wikimedia_image_info(wiki_title)
            if info is None:
                print(f"  SKIP: No image found on Wikimedia Commons for {wiki_title}")
                total["skipped"] += 1
                continue

            source_url = info["url"]
            license_val = info["license"]
            attribution = info["attribution"]
            print(f"  License: {license_val} | Attribution: {attribution[:60] if attribution else 'none'}")

            # Step 2: Download image bytes
            try:
                time.sleep(DOWNLOAD_DELAY)  # Respect Wikimedia CDN rate limits
                image_bytes, content_type = download_image(source_url)
                print(f"  Downloaded: {len(image_bytes):,} bytes ({content_type})")
            except requests.HTTPError as e:
                print(f"  ERROR: HTTP {e.response.status_code} downloading image from {source_url}")
                total["errors"] += 1
                continue
            except requests.RequestException as e:
                print(f"  ERROR: Download failed: {e}")
                total["errors"] += 1
                continue

            if args.dry_run:
                storage_path = f"{BUILDING_PHOTO_STORAGE_PREFIX}/{place_geoid}.jpg"
                print(f"  DRY-RUN: would upload to {storage_path}")
                print(f"  DRY-RUN: would upsert building_photos row for {place_geoid}")
                total["dry_run"] += 1
                total["processed"] += 1
                continue

            # Step 3: Determine file extension from content type
            ext_map = {
                "image/jpeg": "jpg",
                "image/jpg": "jpg",
                "image/png": "png",
                "image/gif": "gif",
                "image/webp": "webp",
                "image/tiff": "tif",
            }
            ext = ext_map.get(content_type, "jpg")
            storage_path = f"{BUILDING_PHOTO_STORAGE_PREFIX}/{place_geoid}.{ext}"

            # Step 4: Upload to Supabase Storage
            try:
                cdn_url = upload_photo_to_storage(image_bytes, storage_path, content_type)
                print(f"  Uploaded: {cdn_url}")
                cdn_urls[place_geoid] = cdn_url
            except Exception as e:
                print(f"  ERROR: Supabase upload failed: {e}")
                total["errors"] += 1
                continue

            # Step 5: Upsert into essentials.building_photos
            try:
                upsert_building_photo(
                    cur,
                    place_geoid=place_geoid,
                    cdn_url=cdn_url,
                    source_url=source_url,
                    license_val=license_val,
                    attribution=attribution,
                    wiki_title=wiki_title,
                )
                print(f"  DB upserted: {place_geoid}")
                total["uploaded"] += 1
            except Exception as e:
                print(f"  ERROR: DB upsert failed: {e}")
                total["errors"] += 1
                continue

            total["processed"] += 1
            print()

        # Commit all upserts in a single transaction
        if not args.dry_run:
            conn.commit()
            print(f"\n  Committed {total['uploaded']} building photo rows.")

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
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total processed : {total['processed']}")
    if args.dry_run:
        print(f"  Would upload    : {total['dry_run']}")
    else:
        print(f"  Uploaded to CDN : {total['uploaded']}")
    print(f"  Skipped         : {total['skipped']}")
    print(f"  Errors          : {total['errors']}")

    if not args.dry_run and cdn_urls:
        print()
        print("CDN URLs (for buildingImages.js CURATED_LOCAL update):")
        print("-" * 40)
        for place_geoid, url in cdn_urls.items():
            print(f"  {place_geoid}: {url}")

    # Exit with error code if all attempts failed
    attempted = total["processed"] + total["errors"]
    if attempted > 0 and total["errors"] == attempted:
        print("\nERROR: All upload attempts failed.")
        sys.exit(1)
    elif total["errors"] > 0:
        print(f"\nWARNING: {total['errors']} errors occurred.")
    else:
        print("\nDone.")


if __name__ == "__main__":
    main()
