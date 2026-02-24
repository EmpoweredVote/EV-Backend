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
# External ID counter for v1.6 synthetic IDs.
# Range: -200001 and below (decrementing).
#
# v1.5 range is -100001 and below (promote_scraped_officials.py).
# v1.6 uses -200001 to avoid collisions with any v1.5 IDs.
# New import scripts MUST use next_ext_id() from this module.
# =====================================================================
_EXT_ID_COUNTER = -200001


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
    """Return the next synthetic external ID in the v1.6 range.

    IDs start at -200001 and decrement with each call.
    This range is reserved for v1.6 LA County import scripts.

    v1.5 promote_scraped_officials.py uses its own local EXT_ID_COUNTER
    starting at -100001 — do NOT modify that script's counter.

    Returns:
        int: Next available external ID (negative integer).
    """
    global _EXT_ID_COUNTER
    val = _EXT_ID_COUNTER
    _EXT_ID_COUNTER -= 1
    return val
