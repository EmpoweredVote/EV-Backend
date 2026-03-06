#!/usr/bin/env python3
"""
CSV-driven batch upload pipeline for LA County city council headshots.

Reads the Phase 63 headshot_research_manifest.csv, filters rows where
research_status='found', downloads each image from found_url, uploads to
Supabase Storage CDN under the politician-photos bucket, and upserts the
CDN URL into essentials.politician_images using the politician_id UUID from
the manifest (no fuzzy name matching required).

Usage:
    cd EV-Backend/scripts
    python3 upload_manifest_headshots.py --dry-run       # test downloads only
    python3 upload_manifest_headshots.py                 # real upload + DB upsert
    python3 upload_manifest_headshots.py --manifest /path/to/other.csv

Dry-run mode downloads all images (validates URLs are reachable) but does NOT
write to Supabase Storage or the database. Use --dry-run first to verify
download success rate before committing to real uploads.

Anti-patterns avoided:
    - Port 6543 pooler: causes "prepared statement already exists" on bulk inserts.
      get_connection() uses port 5432 (direct).
    - Base64 image bytes: upload_photo_to_storage() requires raw bytes.
    - Batch transactions: conn.commit() is called per-row to isolate failures.
    - Fuzzy name matching: politician_id UUIDs are read directly from CSV.
"""

import argparse
import csv
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests

# Insert scripts/ directory into path so utils.py is importable
sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, load_supabase_env, upload_photo_to_storage


# ============================================================
# Constants (copied verbatim from scrape_city_headshots.py)
# ============================================================

DOWNLOAD_DELAY = 0.5  # seconds between downloads

# Browser User-Agent for government sites
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

# Wikipedia-compliant User-Agent
# Required per https://meta.wikimedia.org/wiki/User-Agent_policy
WIKIPEDIA_HEADERS = {
    "User-Agent": "EmpoweredVote/1.0 (https://empowered.vote; headshot-scraper) python-requests/2.32",
}


# ============================================================
# DB connection (copied verbatim from scrape_city_headshots.py lines 564-594)
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Uses direct connection (port 5432), NOT the pooler (port 6543).
    Pooler breaks bulk imports with "prepared statement already exists" errors.
    Handles passwords with special characters via urlparse keyword arguments.

    Returns:
        psycopg2.connection: Open database connection with autocommit=False.
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


# ============================================================
# Storage path generation (copied verbatim from scrape_city_headshots.py lines 533-557)
# ============================================================

def make_storage_path(city_id, member_name, extension="jpg"):
    """Generate deterministic Supabase Storage path for a city council headshot.

    Convention: la_county/cities/{city_slug}/{name_slug}.{extension}
    Strips trailing '_city_council' from city_id to get city_slug.
    Slugifies member_name: lowercase, non-alphanumeric -> hyphens, strip edges.

    Args:
        city_id: City identifier from manifest (e.g., "burbank_city_council")
        member_name: Council member full name (e.g., "Jess Talamantes")
        extension: File extension without dot (e.g., "jpg", "png", "webp")

    Returns:
        str: Storage path (e.g., "la_county/cities/burbank/jess-talamantes.jpg")

    Examples:
        make_storage_path("burbank_city_council", "Jess Talamantes", "jpg")
        -> "la_county/cities/burbank/jess-talamantes.jpg"

        make_storage_path("long_beach_city_council", "Mary Zendejas", "png")
        -> "la_county/cities/long_beach/mary-zendejas.png"
    """
    city_slug = re.sub(r"_city_council$", "", city_id)
    name_slug = re.sub(r"[^a-z0-9]+", "-", member_name.lower()).strip("-")
    return f"la_county/cities/{city_slug}/{name_slug}.{extension}"


# ============================================================
# DB upsert for politician_images (copied verbatim from scrape_city_headshots.py lines 664-702)
# ============================================================

def upsert_politician_image(cur, politician_id, cdn_url, photo_license):
    """Upsert a default headshot for a politician into essentials.politician_images.

    Checks for existing (politician_id, type='default') row.
    If found: UPDATE url and photo_license.
    If not found: INSERT new row with gen_random_uuid().

    This ensures idempotent re-runs — no duplicate rows created.

    Args:
        cur: psycopg2 RealDictCursor
        politician_id: UUID string from essentials.politicians
        cdn_url: Supabase Storage public CDN URL
        photo_license: License string (e.g., "scraped_no_license", "cc_by_sa_4.0")

    Returns:
        str: "updated" if existing row was updated, "inserted" if new row created.
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
# Image download (copied verbatim from scrape_city_headshots.py lines 709-760)
# ============================================================

def download_image(url, timeout=15, referer=None):
    """Download image bytes from URL with appropriate headers per domain.

    Uses Wikipedia-specific User-Agent for wikimedia.org/wikipedia.org URLs.
    Uses browser User-Agent for government CDN URLs.
    Derives content_type from response Content-Type header (not URL extension).

    If the initial request returns 403, retries with a Referer header derived
    from the image URL's domain (some government sites require same-origin Referer).

    Args:
        url: Direct image URL (.jpg, .png, .webp, etc.)
        timeout: Request timeout in seconds (default 15)
        referer: Optional Referer URL to include in request headers

    Returns:
        tuple: (image_bytes: bytes, content_type: str)

    Raises:
        requests.HTTPError: If server returns 4xx/5xx after retries
        requests.RequestException: If connection fails or times out
    """
    if "wikimedia.org" in url or "wikipedia.org" in url:
        headers = WIKIPEDIA_HEADERS
    else:
        headers = dict(BROWSER_HEADERS)
        if referer:
            headers["Referer"] = referer

    resp = requests.get(url, headers=headers, timeout=timeout)

    # Retry with Referer header derived from image domain if 403
    if resp.status_code == 403 and not referer:
        from urllib.parse import urlparse as _urlparse
        parsed = _urlparse(url)
        domain_referer = f"{parsed.scheme}://{parsed.netloc}/"
        retry_headers = dict(BROWSER_HEADERS)
        retry_headers["Referer"] = domain_referer
        resp = requests.get(url, headers=retry_headers, timeout=timeout)

    resp.raise_for_status()

    # Derive content-type from response header (not URL extension)
    # Strip parameters like "; charset=utf-8" -> "image/jpeg"
    raw_ct = resp.headers.get("Content-Type", "image/jpeg")
    content_type = raw_ct.split(";")[0].strip()

    # Fallback to image/jpeg if header is missing or not an image/* type
    if not content_type.startswith("image/"):
        content_type = "image/jpeg"

    return resp.content, content_type


# ============================================================
# Content-type to extension helper (extracted from inline dict in scrape_city_headshots.py)
# ============================================================

def content_type_to_ext(content_type):
    """Map MIME type to file extension.

    Args:
        content_type: MIME type string (e.g., "image/jpeg", "image/png")

    Returns:
        str: File extension without dot (defaults to "jpg" for unknown types)
    """
    ext_map = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
    }
    return ext_map.get(content_type, "jpg")


# ============================================================
# Photo license classifier (from scrape_city_headshots.py lines 926-929)
# ============================================================

def get_photo_license(found_url):
    """Determine photo license based on image URL origin.

    Wikimedia/Wikipedia images are licensed CC BY-SA 4.0.
    All other sourced images (government websites, etc.) are scraped
    with no explicit license.

    Args:
        found_url: The source URL the image was downloaded from

    Returns:
        str: License identifier string
    """
    if "wikimedia.org" in found_url or "wikipedia.org" in found_url:
        return "cc_by_sa_4.0"
    return "scraped_no_license"


# ============================================================
# Main upload pipeline
# ============================================================

def run_upload(manifest_path, dry_run=False):
    """Read CSV manifest and upload all found headshots to Supabase Storage.

    Filters manifest rows to research_status='found' with a non-empty found_url.
    Downloads each image, uploads to Supabase Storage CDN, and upserts the CDN URL
    into essentials.politician_images using the politician_id UUID from the manifest.

    In dry-run mode: downloads images to validate URLs, but does NOT write to
    Supabase Storage or the database. Useful for pre-flight validation.

    Commits per-row (not per-batch) to isolate individual failures.

    Args:
        manifest_path: Path to headshot_research_manifest.csv
        dry_run: If True, download only — no storage upload or DB writes

    Returns:
        tuple: (ok: int, skip: int, fail: int)
    """
    load_env()
    load_supabase_env()

    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        print(f"Error: Manifest not found at {manifest_path}")
        sys.exit(1)

    # Read and filter manifest rows
    with open(manifest_path, newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    rows = [
        r for r in all_rows
        if r.get("research_status") == "found" and r.get("found_url", "").strip()
    ]

    print(f"Manifest: {len(all_rows)} total rows, {len(rows)} rows with research_status=found")
    if dry_run:
        print("Mode: DRY-RUN (downloads only — no storage upload or DB writes)")
    else:
        print("Mode: REAL UPLOAD (will write to Supabase Storage and DB)")
    print()

    # Open DB connection (only needed for real uploads, but open early to fail fast)
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    ok, skip, fail = 0, 0, 0

    for i, row in enumerate(rows):
        politician_id = row["politician_id"]
        found_url = row["found_url"].strip()
        city_id = row["city_id"]
        member_name = row["member_name"]

        # Download image
        try:
            image_bytes, content_type = download_image(found_url)
        except Exception as e:
            print(f"  FAIL download: {member_name} — {e}")
            fail += 1
            continue

        ext = content_type_to_ext(content_type)
        storage_path = make_storage_path(city_id, member_name, ext)

        if dry_run:
            size_kb = len(image_bytes) / 1024
            print(f"  DRY-RUN: {member_name} -> {storage_path} ({size_kb:.1f} KB, {content_type})")
            ok += 1
            # Still sleep between downloads to avoid hammering servers
            if i < len(rows) - 1:
                time.sleep(DOWNLOAD_DELAY)
            continue

        # Upload to Supabase Storage CDN
        try:
            cdn_url = upload_photo_to_storage(image_bytes, storage_path, content_type)
        except Exception as e:
            print(f"  FAIL upload: {member_name} — {e}")
            fail += 1
            continue

        # Upsert CDN URL into essentials.politician_images
        try:
            action = upsert_politician_image(cur, politician_id, cdn_url, get_photo_license(found_url))
            conn.commit()  # commit per-row to isolate failures
        except Exception as e:
            print(f"  FAIL upsert: {member_name} — {e}")
            conn.rollback()
            fail += 1
            continue

        print(f"  {action}: {member_name} -> {cdn_url}")
        ok += 1

        # Rate limiting between downloads (skip on last item)
        if i < len(rows) - 1:
            time.sleep(DOWNLOAD_DELAY)

    cur.close()
    conn.close()

    print()
    print(f"Done: {ok} ok, {skip} skipped, {fail} failed")

    return ok, skip, fail


# ============================================================
# CLI entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Upload headshots from Phase 63 research manifest to Supabase Storage CDN.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 upload_manifest_headshots.py --dry-run
    python3 upload_manifest_headshots.py
    python3 upload_manifest_headshots.py --manifest /path/to/manifest.csv
        """,
    )
    parser.add_argument(
        "--manifest",
        default=str(Path(__file__).parent / "headshot_research_manifest.csv"),
        help="Path to headshot_research_manifest.csv (default: same directory as this script)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download images only — do NOT upload to Supabase Storage or write to DB",
    )
    args = parser.parse_args()

    ok, skip, fail = run_upload(args.manifest, args.dry_run)

    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
