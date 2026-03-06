#!/usr/bin/env python3
"""
Shared utilities for EV-Backend import scripts.

Usage in scripts:
    from utils import get_engine, load_env, next_ext_id
"""

import os
import sys
from pathlib import Path

from sqlalchemy import create_engine

# =====================================================================
# External ID counter for v1.7 synthetic IDs.
# Range: -300001 and below (decrementing).
#
# v1.5 range is -100001 and below (promote_scraped_officials.py).
# v1.6 uses -200001 to avoid collisions with any v1.5 IDs.
# v1.7 uses -300001 to avoid collisions with v1.6 IDs.
# New import scripts MUST use next_ext_id() from this module.
# =====================================================================
_EXT_ID_COUNTER = -300001


def load_env():
    """Load DATABASE_URL from .env.local if not already set.

    Reads EV-Backend/.env.local (one directory above the scripts/ folder).
    If DATABASE_URL is already set in the environment, does nothing.

    Note: Supabase direct connection (port 5432) is required.
    The pooler port (6543) breaks bulk imports with
    "prepared statement already exists" errors.
    """
    if os.getenv("DATABASE_URL"):
        return
    env_path = Path(__file__).parent.parent / ".env.local"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABASE_URL="):
                    os.environ["DATABASE_URL"] = line.split("=", 1)[1]
                    print(f"  Loaded DATABASE_URL from {env_path}")
                    return
    print("Error: DATABASE_URL not set and .env.local not found")
    sys.exit(1)


def get_engine():
    """Create SQLAlchemy engine, URL-encoding the password if needed.

    Handles passwords containing special characters (e.g., '@', '/', '+')
    by parsing the URL and re-encoding only the password component.

    Exits with an error message if DATABASE_URL is not set.
    """
    from urllib.parse import urlparse, quote_plus, urlunparse
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL is not set. Call load_env() first.")
        sys.exit(1)
    parsed = urlparse(raw_url)
    # Re-encode the password to handle special chars like @
    if parsed.password:
        encoded_pw = quote_plus(parsed.password)
        netloc = f"{parsed.username}:{encoded_pw}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        safe_url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    else:
        safe_url = raw_url
    return create_engine(safe_url)


def next_ext_id():
    """Return the next synthetic external ID in the v1.7 range.

    IDs start at -300001 and decrement with each call.
    This range is reserved for v1.7 LA County import scripts.

    v1.5 promote_scraped_officials.py uses its own local EXT_ID_COUNTER
    starting at -100001 — do NOT modify that script's counter.

    Returns:
        int: Next available external ID (negative integer).
    """
    global _EXT_ID_COUNTER
    val = _EXT_ID_COUNTER
    _EXT_ID_COUNTER -= 1
    return val


def load_supabase_env():
    """Load SUPABASE_URL and SUPABASE_SERVICE_KEY from .env.local.

    Must be called before get_supabase_client().
    Reads from the same .env.local as load_env() (EV-Backend/.env.local).
    """
    if os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY"):
        return
    env_path = Path(__file__).parent.parent / ".env.local"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("SUPABASE_URL="):
                    os.environ["SUPABASE_URL"] = line.split("=", 1)[1]
                elif line.startswith("SUPABASE_SERVICE_KEY="):
                    os.environ["SUPABASE_SERVICE_KEY"] = line.split("=", 1)[1]
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY not set and not found in .env.local")
        sys.exit(1)
    print("  Loaded Supabase credentials from .env.local")


def get_supabase_client():
    """Create Supabase client with service role key (bypasses RLS).

    Call load_supabase_env() first to ensure env vars are set.
    Uses the project REST URL (SUPABASE_URL), NOT the DATABASE_URL.

    Returns:
        supabase.Client: Authenticated Supabase client.
    """
    from supabase import create_client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set. Call load_supabase_env() first.")
        sys.exit(1)
    return create_client(url, key)


PHOTO_BUCKET = "politician_photos"


def upload_photo_to_storage(image_bytes, storage_path, content_type="image/jpeg"):
    """Upload image bytes to Supabase Storage and return the public CDN URL.

    CRITICAL: Pass raw image bytes, NOT base64-encoded. Base64 encoding
    corrupts binary files in Supabase Storage.

    CRITICAL: content_type MUST be explicit. The SDK defaults to text/plain,
    which makes the CDN serve the file with the wrong MIME type, breaking
    <img> tags in the frontend.

    Args:
        image_bytes: Raw image bytes (from requests.get().content)
        storage_path: Path within bucket, e.g., "la_county/supervisors/{id}.jpg"
        content_type: MIME type — default "image/jpeg"; use "image/png" for PNGs

    Returns:
        str: Public CDN URL (stable; survives re-upload/overwrite)

    Raises:
        Exception: If upload fails
    """
    client = get_supabase_client()
    # upsert=true: overwrite on re-scrape (same CDN URL reused — locked decision)
    client.storage.from_(PHOTO_BUCKET).upload(
        storage_path,
        image_bytes,
        {"content-type": content_type, "upsert": "true"},
    )
    # get_public_url() returns the stable public URL (no expiry, no auth required)
    return client.storage.from_(PHOTO_BUCKET).get_public_url(storage_path)


def load_pipeline_config(region=None):
    """Load pipeline config and return the region-specific section.

    Args:
        region: Region key (e.g., "la_county"). If None, uses default_region from config.

    Returns:
        dict: Region config dict with cities, URLs, etc.
    """
    import json
    config_path = Path(__file__).parent / "pipeline_config.json"
    with open(config_path) as f:
        config = json.load(f)
    region_key = region or config.get("default_region", "la_county")
    if region_key not in config["regions"]:
        print(f"Error: Region '{region_key}' not found in pipeline_config.json")
        print(f"  Available regions: {list(config['regions'].keys())}")
        sys.exit(1)
    return config["regions"][region_key]
