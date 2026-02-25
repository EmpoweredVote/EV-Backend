#!/usr/bin/env python3
"""
Batch headshot scraper for 89 LA County city councils.

Iterates through all 89 cities in city_sources.json, fetches each city's
council page, extracts per-member headshot portrait URLs using a name-proximity
heuristic, downloads images, uploads to Supabase Storage CDN, and upserts
CDN URLs with photo_license into essentials.politician_images.

Key behaviors:
- Cloudflare detection: checks BOTH HTTP status (403/429/503) AND cf-ray/
  server headers. Cloudflare-blocked cities are marked "blocked" in
  city_sources.json and skipped on re-run (not retried).
- Playwright fallback: used when requests returns < 500 chars of text content
  (JS-heavy council pages).
- Inter-city rate limiting: 1.5s minimum between city page fetches (PIPE-02).
- Per-city DB COMMIT: one failed city does not roll back previous successes.
- Duplicate URL detection: if all members of a city share the same extracted
  URL (likely a logo), the city is marked for manual review instead of
  uploading the same wrong image for everyone.
- Idempotent re-runs: cities with headshot_status "scraped" or "blocked" are
  skipped on re-run.
- Levenshtein fuzzy matching: last-name fallback (threshold <= 1) for roster
  names that differ slightly from DB names.

Usage:
    cd EV-Backend/scripts
    python3 scrape_city_headshots.py                   # all cities
    python3 scrape_city_headshots.py --dry-run         # no upload/DB writes
    python3 scrape_city_headshots.py --city burbank_city_council
    python3 scrape_city_headshots.py --resume          # skip to first pending
    python3 scrape_city_headshots.py --limit 10        # process at most 10 cities
    python3 scrape_city_headshots.py --check-coverage  # coverage validation only
    python3 scrape_city_headshots.py --force-retry     # reset failed cities and re-attempt

Requirements:
    - DATABASE_URL and SUPABASE_URL/SUPABASE_SERVICE_KEY in EV-Backend/.env.local
    - city_sources.json with 89 city configs and roster arrays
    - essentials.politician_images.photo_license column (added Phase 39)
    - politician-photos bucket in Supabase Storage (verified Phase 39)
"""

import os
import sys
import json
import re
import time
import argparse
import traceback
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from rapidfuzz.distance import Levenshtein

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, load_supabase_env, upload_photo_to_storage


# ============================================================
# Module-level Playwright browser instance (shared across all cities)
# ============================================================
# A single browser is launched once and reused for all cities that need
# Playwright. This avoids the "Playwright Sync API inside asyncio loop"
# error that occurs when sync_playwright().start() is called multiple
# times in the same process (each call creates a new event loop context
# that conflicts with the previous one).
_PW_INSTANCE = None
_PW_BROWSER = None


def get_playwright_browser():
    """Get or create the shared Playwright browser instance.

    Lazily initializes a headless Chromium browser on first call.
    Subsequent calls return the same browser object.

    Returns:
        playwright.sync_api.Browser: Shared headless Chromium browser.
    """
    global _PW_INSTANCE, _PW_BROWSER
    if _PW_BROWSER is None or not _PW_BROWSER.is_connected():
        from playwright.sync_api import sync_playwright
        _PW_INSTANCE = sync_playwright().start()
        _PW_BROWSER = _PW_INSTANCE.chromium.launch(headless=True)
    return _PW_BROWSER


def stop_playwright_browser():
    """Stop and clean up the shared Playwright browser instance."""
    global _PW_INSTANCE, _PW_BROWSER
    if _PW_BROWSER is not None:
        try:
            _PW_BROWSER.close()
        except Exception:
            pass
        _PW_BROWSER = None
    if _PW_INSTANCE is not None:
        try:
            _PW_INSTANCE.stop()
        except Exception:
            pass
        _PW_INSTANCE = None


# ============================================================
# Constants
# ============================================================

# Minimum seconds between city page fetches (PIPE-02 rate limit requirement)
INTER_CITY_DELAY = 1.5

# Seconds between individual image downloads within a city
DOWNLOAD_DELAY = 0.5

# HTTP request timeout in seconds
TIMEOUT = 15

# Maximum Levenshtein edit distance for fuzzy last-name matching
LEVENSHTEIN_THRESHOLD = 1

# Valid image file extensions for portrait detection
PORTRAIT_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}

# Regex to exclude non-portrait images (logos, icons, banners, etc.)
EXCLUDE_PATTERNS = re.compile(
    r"(logo|icon|banner|header|footer|background|bg[-_]|pattern|seal|flag|"
    r"map|arrow|chevron|search|menu|nav|sprite|button|placeholder|blank|"
    r"default-avatar|generic|twitter|facebook|linkedin|instagram|youtube|"
    r"social[-_]|share[-_]|email[-_]|phone[-_]|fax[-_]|x[-_]icon|xicon|"
    r"twitter-x|snapchat|tiktok|pinterest|rss[-_]|feed[-_]|"
    r"chat[_-]bubble|chat_icon|speech[_-]bubble|comment[_-]|"
    r"document[-_]icon|pdf[-_]icon|file[-_]icon|download[-_]icon|"
    r"print[-_]|share[-_]|translate[-_]|language[-_]|accessibility[-_]|"
    r"star[-_]|rating[-_]|alert[-_]|warning[-_]|info[-_]icon)",
    re.I,
)

# Browser User-Agent for government sites (matches scrape_headshots.py)
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

# Wikipedia-compliant User-Agent (matches scrape_headshots.py pattern)
# Required per https://meta.wikimedia.org/wiki/User-Agent_policy
WIKIPEDIA_HEADERS = {
    "User-Agent": "EmpoweredVote/1.0 (https://empowered.vote; headshot-scraper) python-requests/2.32",
}


# ============================================================
# Cloudflare detection
# ============================================================

def is_cloudflare_blocked(response):
    """Detect Cloudflare protection from HTTP response.

    CRITICAL: Checks BOTH HTTP status AND cf-ray/server header.
    A 403 without cf-ray is a regular HTTP error, not Cloudflare.
    A 403 with cf-ray is Cloudflare blocking — mark as "blocked" and skip.

    Args:
        response: requests.Response object

    Returns:
        bool: True if Cloudflare blocking detected, False otherwise.
    """
    if response.status_code in (403, 429, 503):
        server = response.headers.get("server", "").lower()
        has_cf_ray = "cf-ray" in response.headers
        has_cf_server = "cloudflare" in server
        if has_cf_ray or has_cf_server:
            return True
    return False


# ============================================================
# HTML fetching with Playwright fallback
# ============================================================

def fetch_council_page(url, keep_page=False):
    """Fetch HTML of a council page with Playwright fallback.

    Strategy:
    1. Try requests.get() with BROWSER_HEADERS
    2. If Cloudflare detected: return (None, False, "cloudflare", None, None)
    3. If text content < 500 chars: fall back to Playwright
    4. Playwright: headless Chromium, wait for networkidle
    5. If Playwright HTML contains Cloudflare challenge: return (None, True, "cloudflare", None, None)

    Imports playwright lazily (only when needed) to avoid hard dependency.

    If keep_page=True and Playwright was needed, returns (html, True, None, page, browser)
    so the caller can run page.evaluate() for CSS extraction before closing.
    Normal mode returns (html, used_playwright, blocked_reason, None, None).

    Args:
        url: Council page URL to fetch
        keep_page: If True and Playwright was used, do NOT close browser — return (html, True, None, page, browser)

    Returns:
        tuple: (html: str|None, used_playwright: bool, blocked_reason: str|None, page: obj|None, browser: obj|None)
            blocked_reason is "cloudflare" or "fetch_failed" on failure, None on success.
    """
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=TIMEOUT, allow_redirects=True)

        # Check for Cloudflare BEFORE raise_for_status
        if is_cloudflare_blocked(resp):
            return None, False, "cloudflare", None, None

        # Accept any response with sufficient content, regardless of HTTP status.
        # Many government CMS sites return 404 status but still serve the full
        # council page (CivicPlus, Revize, etc.) when the path is a soft-404.
        # Only raise on 5xx server errors (not on 4xx content-bearing responses).
        soup = BeautifulSoup(resp.text, "html.parser")
        body_text = soup.get_text(strip=True)

        if len(body_text) > 500:
            if resp.status_code >= 500:
                # Server error — try Playwright
                print(f"    Server error {resp.status_code} — falling back to Playwright")
            else:
                # 200, 301/302 final, or soft-404 with content — use it
                return resp.text, False, None, None, None

        else:
            if resp.status_code >= 400:
                try:
                    resp.raise_for_status()
                except Exception as e:
                    print(f"    requests failed: {e} — falling back to Playwright")
            else:
                # Content too short — likely JS-rendered, fall through to Playwright
                print(f"    Content too short ({len(body_text)} chars) — falling back to Playwright")

    except requests.RequestException as e:
        print(f"    requests failed: {e} — falling back to Playwright")

    # Playwright fallback for JS-heavy sites
    # Use the shared module-level browser to avoid asyncio event loop conflicts
    # when sync_playwright() is called multiple times in the same process.
    print(f"    Launching Playwright for: {url}")
    try:
        browser = get_playwright_browser()
        page = browser.new_page()
        page.goto(url, timeout=20000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            # networkidle timeout is non-fatal — use whatever loaded
            pass
        html = page.content()

        # Check for Cloudflare challenge in rendered HTML
        if "Checking your browser" in html or "cf-ray" in html.lower():
            page.close()
            return None, True, "cloudflare", None, None

        if keep_page:
            # Caller is responsible for closing the page
            # Browser is kept alive (module-level shared instance)
            return html, True, None, page, (None, None)  # browser_handle is (None, None) — shared browser not closed by caller
        else:
            page.close()
            return html, True, None, None, None

    except ImportError:
        print("    Playwright not installed — cannot fall back")
        return None, False, "fetch_failed", None, None
    except Exception as e:
        print(f"    Playwright failed: {e}")
        return None, False, "fetch_failed", None, None


# ============================================================
# Headshot URL extraction
# ============================================================

def extract_headshot_url(html, member_name, city_url):
    """Extract best-candidate headshot URL for a council member from page HTML.

    Three strategies in order of confidence:

    Strategy 1 — Name proximity:
        Find all text nodes containing the member's last name. For each match,
        walk up to 4 parent levels looking for img tags. Filter by: valid
        extension, not matching EXCLUDE_PATTERNS in src or alt.

    Strategy 2 — Alt text match:
        Scan all img tags. If alt text contains the last name AND src has a
        valid extension AND src doesn't match exclusion patterns, return it.

    Strategy 3 — Wikipedia fallback:
        Attempt Wikipedia lookup: fetch https://en.wikipedia.org/wiki/First_Last.
        If found (200), extract infobox image (.infobox img). Add 2s delay
        before the request per Wikipedia rate-limit policy.

    Args:
        html: HTML string of the council page
        member_name: Full name of the council member (e.g., "Jess Talamantes")
        city_url: Base URL of the city council page (for resolving relative URLs)

    Returns:
        str: Absolute image URL, or None if no portrait found.
    """
    soup = BeautifulSoup(html, "html.parser")
    # Use last name for matching (handles "Mary Garcia" -> "garcia")
    name_parts = member_name.strip().split()
    last_name = name_parts[-1].lower() if name_parts else member_name.lower()

    def is_valid_img(src, alt=""):
        """Return True if img src looks like a portrait, not a logo/icon."""
        if not src:
            return False
        # Extract extension (handle query params like ?v=1)
        path_part = src.split("?")[0].split("#")[0]
        ext = ""
        if "." in path_part:
            ext = "." + path_part.rsplit(".", 1)[-1].lower()
        if ext not in PORTRAIT_EXTENSIONS:
            return False
        if EXCLUDE_PATTERNS.search(src):
            return False
        if alt and EXCLUDE_PATTERNS.search(alt):
            return False
        return True

    # Strategy 1: Name proximity — find text containing last name, walk up for img
    name_els = soup.find_all(string=re.compile(re.escape(last_name), re.I))
    for name_el in name_els:
        container = name_el.parent
        for _ in range(4):  # walk up to 4 parent levels
            if container is None:
                break
            imgs = container.find_all("img")
            for img in imgs:
                src = img.get("src", "")
                alt = img.get("alt", "")
                if is_valid_img(src, alt):
                    return urljoin(city_url, src)
            container = container.parent

    # Strategy 1b: CSS background-image near name text
    # Many CMS card galleries use div[style*="background-image"] instead of <img>
    # Also catches Avada theme's --awb-background-image-front CSS custom property
    def extract_urls_from_style(style_str):
        """Extract all image URLs from a style attribute, regardless of CSS property name.

        Handles standard background-image: url(...) and Avada theme's custom
        --awb-background-image-front:url(...) properties.

        Returns:
            list of URL strings found in the style attribute.
        """
        # Match any CSS property value containing url(...)
        return re.findall(r'url\(["\']?([^"\')\s]+)["\']?\)', style_str, re.I)

    for name_el in name_els:
        container = name_el.parent
        for _ in range(4):  # walk up to 4 parent levels
            if container is None:
                break
            # Check all elements with style attributes that contain url(
            styled_els = container.find_all(style=re.compile(r"url\(", re.I))
            for styled_el in styled_els:
                style = styled_el.get("style", "")
                for bg_url in extract_urls_from_style(style):
                    # Validate: must have portrait extension and not be excluded pattern
                    path_part = bg_url.split("?")[0].split("#")[0]
                    ext = ""
                    if "." in path_part:
                        ext = "." + path_part.rsplit(".", 1)[-1].lower()
                    if ext in PORTRAIT_EXTENSIONS and not EXCLUDE_PATTERNS.search(bg_url):
                        return urljoin(city_url, bg_url)
            container = container.parent

    # Strategy 2: Alt text match — scan all imgs on page
    for img in soup.find_all("img"):
        src = img.get("src", "")
        alt = img.get("alt", "").lower()
        if last_name in alt and is_valid_img(src, alt):
            return urljoin(city_url, src)

    # Strategy 2b: All elements with style attributes containing url(), match by URL containing last name
    for styled_el in soup.find_all(style=re.compile(r"url\(", re.I)):
        style = styled_el.get("style", "")
        for bg_url in extract_urls_from_style(style):
            if last_name in bg_url.lower():
                path_part = bg_url.split("?")[0].split("#")[0]
                ext = ""
                if "." in path_part:
                    ext = "." + path_part.rsplit(".", 1)[-1].lower()
                if ext in PORTRAIT_EXTENSIONS and not EXCLUDE_PATTERNS.search(bg_url):
                    return urljoin(city_url, bg_url)

    # Strategy 3: Wikipedia fallback — try member's Wikipedia article
    # Only attempt if name has at least 2 parts (First Last)
    if len(name_parts) >= 2:
        first_name = name_parts[0]
        wiki_name = "_".join(name_parts)
        wiki_url = f"https://en.wikipedia.org/wiki/{wiki_name}"
        try:
            # 2s delay before Wikipedia request (rate limit policy)
            time.sleep(2.0)
            wiki_resp = requests.get(wiki_url, headers=WIKIPEDIA_HEADERS, timeout=TIMEOUT)
            if wiki_resp.status_code == 200:
                wiki_soup = BeautifulSoup(wiki_resp.text, "html.parser")
                # Look for infobox portrait image
                infobox = wiki_soup.find(class_=re.compile(r"infobox", re.I))
                if infobox:
                    infobox_img = infobox.find("img")
                    if infobox_img:
                        src = infobox_img.get("src", "")
                        if src:
                            # Validate article is about a current politician, not a historical figure
                            # Check first paragraph for geographic/role relevance
                            first_para = wiki_soup.find("p", class_=False)
                            if first_para:
                                para_text = first_para.get_text().lower()
                                # Must mention California, council, mayor, or a city-related term
                                relevance_terms = [
                                    "california", "council", "mayor", "city of",
                                    "city council", "los angeles", "la county",
                                    "municipal", "alderman", "councilmember",
                                ]
                                # Extract city name from city_url for matching
                                city_domain = urlparse(city_url).hostname or ""
                                city_words = [
                                    w for w in re.split(r"[.\-_]", city_domain)
                                    if len(w) > 3 and w not in ("www", "city", "org", "gov", "com")
                                ]
                                relevance_terms.extend(city_words)

                                if not any(term in para_text for term in relevance_terms):
                                    # Article doesn't appear to be about a California politician
                                    # Skip to avoid false positives like historical figures
                                    pass  # fall through to return None
                                else:
                                    # Article is relevant — return the image
                                    if src.startswith("//"):
                                        src = "https:" + src
                                    return src
                            else:
                                # No first paragraph to validate — skip to avoid false positives
                                pass
        except requests.RequestException:
            pass  # Wikipedia lookup failure is non-fatal — return None

    return None


# ============================================================
# Playwright-based CSS background-image extraction
# ============================================================

def extract_headshot_url_playwright(page, member_name, city_url):
    """Extract headshot URL using Playwright's computed styles for CSS background-image.

    This catches images loaded via JavaScript or CSS that are invisible to
    BeautifulSoup's static HTML parsing.

    Args:
        page: Playwright page object (still open)
        member_name: Full name of the council member
        city_url: Base URL for resolving relative URLs

    Returns:
        str: Absolute image URL, or None if no portrait found.
    """
    last_name = member_name.strip().split()[-1].lower()

    try:
        # Use page.evaluate to find elements with computed background-image near the name
        result = page.evaluate("""(lastName) => {
            // Find all text nodes containing the last name
            const walker = document.createTreeWalker(
                document.body,
                NodeFilter.SHOW_TEXT,
                { acceptNode: (node) =>
                    node.textContent.toLowerCase().includes(lastName)
                    ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT
                }
            );

            const results = [];
            let textNode;
            while (textNode = walker.nextNode()) {
                // Walk up to 5 parent levels looking for background-image
                let el = textNode.parentElement;
                for (let i = 0; i < 5 && el; i++) {
                    // Check this element and its children for background-image
                    const candidates = [el, ...el.querySelectorAll('*')];
                    for (const candidate of candidates) {
                        const style = window.getComputedStyle(candidate);
                        const bg = style.backgroundImage;
                        if (bg && bg !== 'none' && bg.startsWith('url(')) {
                            const url = bg.slice(4, -1).replace(/['"]/g, '');
                            // Filter out gradients, SVGs, data URIs, small icons
                            if (url.startsWith('http') &&
                                !url.includes('gradient') &&
                                !url.includes('.svg') &&
                                !url.startsWith('data:') &&
                                (url.includes('.jpg') || url.includes('.jpeg') ||
                                 url.includes('.png') || url.includes('.webp'))) {
                                results.push(url);
                            }
                        }
                    }
                    el = el.parentElement;
                }
            }
            return results.length > 0 ? results[0] : null;
        }""", last_name)

        return result
    except Exception:
        return None


# ============================================================
# Storage path generation
# ============================================================

def make_storage_path(city_id, member_name, extension="jpg"):
    """Generate deterministic Supabase Storage path for a city council headshot.

    Convention: la_county/cities/{city_slug}/{name_slug}.{extension}
    Strips trailing '_city_council' from city_id to get city_slug.
    Slugifies member_name: lowercase, non-alphanumeric -> hyphens, strip edges.

    Args:
        city_id: City identifier from city_sources.json (e.g., "burbank_city_council")
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
# DB connection
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
# Politician ID lookup with fuzzy fallback
# ============================================================

def find_politician_id(cur, full_name):
    """Look up politician UUID by full_name with Levenshtein fallback.

    Two-tier lookup:
    1. Exact ILIKE match on full_name against essentials.politicians
    2. If no exact match: compare last names using Levenshtein distance.
       If exactly one match within threshold, return it. If multiple matches
       or no matches, return None (ambiguous or not found).

    Args:
        cur: psycopg2 RealDictCursor
        full_name: Full name string from roster (e.g., "Jess Talamantes")

    Returns:
        str: UUID string if found unambiguously, None otherwise.
    """
    # Tier 1: exact ILIKE match
    cur.execute("""
        SELECT p.id
        FROM essentials.politicians p
        WHERE p.full_name ILIKE %s
          AND p.is_active = true
        ORDER BY p.last_synced DESC
        LIMIT 1
    """, (full_name,))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    # Tier 2: fuzzy last-name match
    target_last = full_name.strip().split()[-1].lower()

    cur.execute("""
        SELECT p.id, p.full_name
        FROM essentials.politicians p
        WHERE p.is_active = true
          AND p.source = 'scraped'
        LIMIT 5000
    """)
    all_rows = cur.fetchall()

    matches = []
    for r in all_rows:
        db_last = r["full_name"].strip().split()[-1].lower()
        dist = Levenshtein.distance(db_last, target_last)
        if dist <= LEVENSHTEIN_THRESHOLD:
            matches.append(r)

    if len(matches) == 1:
        print(f"    Fuzzy match: '{full_name}' ~ '{matches[0]['full_name']}'")
        return str(matches[0]["id"])
    elif len(matches) > 1:
        # Ambiguous — too many similar last names, skip rather than guess wrong
        print(f"    Fuzzy match ambiguous for '{full_name}': {[m['full_name'] for m in matches[:3]]}...")
        return None

    return None


# ============================================================
# DB upsert for politician_images
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
# Image download
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
        from urllib.parse import urlparse
        parsed = urlparse(url)
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
# Per-city processing
# ============================================================

def process_city(conn, city_config, dry_run=False):
    """Process a single city: extract headshots for all roster members.

    Steps:
    1. Fetch council page HTML (with Cloudflare detection + Playwright fallback)
    2. Extract headshot URL per roster member using name-proximity heuristic
    3. Detect duplicate URLs (same URL for all members = likely logo, skip)
    4. For each member with a URL: find_politician_id, download, upload, upsert
    5. Per-city COMMIT (isolated from other cities)

    Args:
        conn: psycopg2 connection (autocommit=False)
        city_config: City config dict from city_sources.json
        dry_run: If True, skip upload and DB writes

    Returns:
        tuple: (success: bool, upload_count: int, failure_reason: str|None)
    """
    city_id = city_config["id"]
    city_name = city_config["name"]
    city_url = city_config.get("url", "")
    roster = city_config.get("roster", [])

    if not city_url:
        return False, 0, "no_url"

    if not roster:
        return False, 0, "empty_roster"

    print(f"\n  Fetching: {city_name} ({city_url})")

    # Step 1: Fetch council page (keep_page=True so Playwright page stays open for CSS extraction)
    html, used_playwright, blocked_reason, pw_page, pw_browser_handle = fetch_council_page(
        city_url, keep_page=True
    )

    if blocked_reason == "cloudflare":
        print(f"    Cloudflare blocked: {city_name}")
        return False, 0, "cloudflare"

    if html is None:
        print(f"    Fetch failed: {city_name} (reason={blocked_reason})")
        return False, 0, blocked_reason or "fetch_failed"

    print(f"    Fetched {'(Playwright)' if used_playwright else '(requests)'}: {len(html):,} chars")

    # Step 2: Extract headshot URL per roster member
    member_urls = {}
    for member in roster:
        member_name = member.get("name", "").strip()
        if not member_name:
            continue
        # Check for manual headshot_url override in roster entry
        manual_url = member.get("headshot_url", "").strip() if member.get("headshot_url") else ""
        if manual_url:
            headshot_url = manual_url
            print(f"    Override: {member_name} -> {manual_url}")
        else:
            headshot_url = extract_headshot_url(html, member_name, city_url)
        member_urls[member_name] = headshot_url
        if headshot_url and not manual_url:
            print(f"    Found: {member_name} -> {headshot_url}")
        elif not headshot_url:
            print(f"    None: {member_name}")

    # Step 2b: Playwright CSS extraction for members still without URL
    if pw_page is not None:
        for member in roster:
            member_name = member.get("name", "").strip()
            if not member_name:
                continue
            if member_urls.get(member_name) is None:
                pw_url = extract_headshot_url_playwright(pw_page, member_name, city_url)
                if pw_url:
                    member_urls[member_name] = pw_url
                    print(f"    Found (Playwright CSS): {member_name} -> {pw_url}")
        # Close the page (NOT the shared browser — it's reused across cities)
        try:
            pw_page.close()
        except Exception:
            pass

    # Step 3: Duplicate URL detection
    # If all non-None URLs are identical, likely extracted a logo for all members
    non_none_urls = [u for u in member_urls.values() if u is not None]
    if non_none_urls:
        unique_urls = set(non_none_urls)
        if len(unique_urls) == 1 and len(non_none_urls) > 1:
            only_url = next(iter(unique_urls))
            print(f"    WARN: All {len(non_none_urls)} members have same URL (likely logo): {only_url}")
            print(f"    Marking city for manual review — no uploads")
            # Clear all URLs for this city to prevent uploading the wrong image
            member_urls = {name: None for name in member_urls}
            return False, 0, "duplicate_url_detected"

    # Step 4: Process each member with a headshot URL
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    upload_count = 0
    first_download = True

    try:
        for member in roster:
            member_name = member.get("name", "").strip()
            if not member_name:
                continue

            headshot_url = member_urls.get(member_name)
            if not headshot_url:
                continue

            # Find politician in DB
            politician_id = find_politician_id(cur, member_name)
            if not politician_id:
                print(f"    WARN: {member_name} not found in DB — skipping")
                continue

            # Enforce download delay between images (not before first)
            if not first_download:
                time.sleep(DOWNLOAD_DELAY)
            first_download = False

            # Download image
            try:
                image_bytes, content_type = download_image(headshot_url)
                print(f"    Downloaded: {member_name} ({len(image_bytes):,} bytes, {content_type})")
            except requests.HTTPError as e:
                print(f"    ERROR: {member_name} — HTTP {e.response.status_code} downloading {headshot_url}")
                continue
            except requests.RequestException as e:
                print(f"    ERROR: {member_name} — {e}")
                continue

            # Determine file extension from content_type
            ext_map = {
                "image/jpeg": "jpg",
                "image/jpg": "jpg",
                "image/png": "png",
                "image/webp": "webp",
                "image/gif": "gif",
            }
            extension = ext_map.get(content_type, "jpg")

            # Determine storage path
            storage_path = make_storage_path(city_id, member_name, extension)

            if dry_run:
                print(f"    DRY-RUN: {member_name} — would upload to {storage_path}")
                upload_count += 1
                continue

            # Upload to Supabase Storage
            try:
                cdn_url = upload_photo_to_storage(image_bytes, storage_path, content_type)
                print(f"    Uploaded: {member_name} -> {cdn_url}")
            except Exception as e:
                print(f"    ERROR: {member_name} — upload failed: {e}")
                continue

            # Determine license (Wikipedia = CC BY-SA 4.0, others = scraped)
            if "wikimedia.org" in headshot_url or "wikipedia.org" in headshot_url:
                photo_license = "cc_by_sa_4.0"
            else:
                photo_license = "scraped_no_license"

            # Upsert into politician_images
            try:
                action = upsert_politician_image(cur, politician_id, cdn_url, photo_license)
                print(f"    DB {action}: {member_name} (license={photo_license})")
                upload_count += 1
            except Exception as e:
                print(f"    ERROR: {member_name} — DB upsert failed: {e}")
                continue

        # Per-city COMMIT — isolates each city's successes from failures in other cities
        if not dry_run:
            conn.commit()
            print(f"    Committed {upload_count} headshots for {city_name}")

        return True, upload_count, None

    except Exception as e:
        if not dry_run:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"    FATAL in {city_name}: {e}")
        traceback.print_exc()
        return False, 0, str(e)
    finally:
        cur.close()


# ============================================================
# Coverage validation
# ============================================================

def check_coverage(conn):
    """Validate headshot coverage via HEAD requests to Supabase CDN.

    Queries all politician_images.url for LOCAL/LOCAL_EXEC politicians in CA
    with Supabase CDN URLs, issues HEAD requests with 100ms inter-request delay,
    prints and returns coverage percentage.

    Required by PHOTO-03 success criterion 1 (HEAD request check, not null-count SQL).

    Args:
        conn: psycopg2 connection

    Returns:
        tuple: (ok_count: int, total_count: int, coverage_pct: float)
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT pi.url
        FROM essentials.politician_images pi
        JOIN essentials.politicians p ON p.id = pi.politician_id
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE pi.type = 'default'
          AND pi.url LIKE '%supabase%'
          AND d.district_type IN ('LOCAL', 'LOCAL_EXEC')
          AND d.state = 'CA'
          AND p.is_active = true
        LIMIT 600
    """)
    rows = cur.fetchall()
    cur.close()

    total = len(rows)
    ok = 0

    print(f"\nChecking coverage: {total} CDN URLs to validate...")
    for i, row in enumerate(rows):
        url = row["url"]
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                ok += 1
            else:
                print(f"  FAIL [{resp.status_code}]: {url}")
        except requests.RequestException as e:
            print(f"  ERROR: {url} — {e}")
        time.sleep(0.1)  # 100ms between HEAD requests

        if (i + 1) % 50 == 0:
            pct_so_far = (ok / (i + 1)) * 100
            print(f"  Progress: {i + 1}/{total} checked ({pct_so_far:.1f}% OK so far)")

    pct = (ok / total * 100) if total > 0 else 0.0
    print(f"\nCoverage: {ok}/{total} URLs return HTTP 200 ({pct:.1f}%)")
    if pct >= 80.0:
        print("PASS: 80%+ headshot coverage (PHOTO-03 requirement met)")
    else:
        print(f"WARN: Coverage {pct:.1f}% below 80% PHOTO-03 requirement")

    return ok, total, pct


# ============================================================
# Main entry point
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Batch headshot scraper for 89 LA County city councils",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scrape_city_headshots.py                   # process all pending cities
  python3 scrape_city_headshots.py --dry-run         # download only, no upload/DB writes
  python3 scrape_city_headshots.py --city burbank_city_council
  python3 scrape_city_headshots.py --resume          # skip to first non-scraped/non-blocked city
  python3 scrape_city_headshots.py --limit 10        # process at most 10 cities
  python3 scrape_city_headshots.py --check-coverage  # run coverage validation only
  python3 scrape_city_headshots.py --force-retry     # reset failed cities and re-attempt
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download images but skip Supabase upload and DB writes",
    )
    parser.add_argument(
        "--city",
        default=None,
        metavar="CITY_ID",
        help="Process a single city only (e.g., burbank_city_council)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip cities until the first non-scraped/non-blocked city",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N cities (useful for batched runs)",
    )
    parser.add_argument(
        "--check-coverage",
        action="store_true",
        help="Run HEAD-request coverage validation only (no scraping)",
    )
    parser.add_argument(
        "--force-retry",
        action="store_true",
        help="Reset all 'failed' cities to pending (re-attempt after URL fixes)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Phase 42 — Batch Headshot Scraper for 89 LA County Cities")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)
    if args.dry_run:
        print("  DRY-RUN MODE: downloads only, no uploads or DB writes")

    # Load environment
    load_env()
    if not args.dry_run:
        load_supabase_env()

    # Load city_sources.json
    config_path = Path(__file__).parent / "city_sources.json"
    if not config_path.exists():
        print(f"Error: city_sources.json not found at {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)
    cities = config.get("cities", [])
    print(f"\nLoaded {len(cities)} cities from {config_path}")

    # --force-retry: reset all "failed" cities to pending before processing
    if args.force_retry:
        retry_count = 0
        for city_config in cities:
            if city_config.get("headshot_status") == "failed":
                city_config["headshot_status"] = None
                # Clear old failure metadata
                city_config.pop("headshot_failure_reason", None)
                retry_count += 1
        print(f"Reset {retry_count} failed cities to pending (--force-retry)")

    # Connect to DB
    conn = get_connection()
    conn.autocommit = False

    # Coverage-only mode
    if args.check_coverage:
        ok, total, pct = check_coverage(conn)
        conn.close()
        sys.exit(0 if pct >= 80.0 else 1)

    # Counters
    processed = 0
    skipped = 0
    blocked = 0
    failed = 0
    total_uploaded = 0
    prev_city_time = None
    resume_reached = not args.resume  # if --resume, start as False until first pending city

    print("\n" + "=" * 60)
    print("Processing cities...")
    print("=" * 60)

    for city_config in cities:
        city_id = city_config["id"]
        city_name = city_config["name"]

        # --city: single city mode
        if args.city and city_id != args.city:
            continue

        # --limit: stop when limit reached
        if args.limit is not None and processed >= args.limit:
            print(f"\nReached --limit {args.limit} — stopping")
            break

        # Check current status
        current_status = city_config.get("headshot_status")

        # --resume: skip until first non-scraped/non-blocked city
        if args.resume and not resume_reached:
            if current_status in ("scraped", "blocked"):
                skipped += 1
                continue
            else:
                # Found first pending city — stop skipping
                resume_reached = True

        # Idempotent skip: already scraped or blocked
        if current_status in ("scraped", "blocked"):
            skipped += 1
            continue

        # Enforce inter-city rate limit (PIPE-02: 1.5s minimum between city page fetches)
        if prev_city_time is not None:
            elapsed = time.time() - prev_city_time
            if elapsed < INTER_CITY_DELAY:
                time.sleep(INTER_CITY_DELAY - elapsed)
        prev_city_time = time.time()

        # Process city
        try:
            success, upload_count, failure_reason = process_city(conn, city_config, dry_run=args.dry_run)

            if failure_reason == "cloudflare":
                city_config["headshot_status"] = "blocked"
                city_config["headshot_blocked_reason"] = "cloudflare"
                blocked += 1
                print(f"  BLOCKED: {city_name} (Cloudflare)")

            elif failure_reason == "duplicate_url_detected":
                city_config["headshot_status"] = "failed"
                city_config["headshot_failure_reason"] = "duplicate_url_detected"
                failed += 1
                print(f"  FAILED: {city_name} (duplicate image URL — needs manual review)")

            elif success:
                city_config["headshot_status"] = "scraped"
                city_config["headshot_scraped_at"] = datetime.now().isoformat()
                city_config["headshot_count"] = upload_count
                total_uploaded += upload_count
                processed += 1
                print(f"  OK: {city_name} ({upload_count} headshots)")

            else:
                city_config["headshot_status"] = "failed"
                city_config["headshot_failure_reason"] = failure_reason or "unknown"
                failed += 1
                print(f"  FAILED: {city_name} ({failure_reason})")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            city_config["headshot_status"] = "failed"
            city_config["headshot_failure_reason"] = str(e)
            failed += 1
            print(f"\n  ERROR in {city_name}: {e}")
            traceback.print_exc()

    # Write updated status back to city_sources.json
    config["cities"] = cities
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"\nUpdated {config_path} with headshot_status fields")

    conn.close()

    # Stop shared Playwright browser if it was used
    stop_playwright_browser()

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    if args.city:
        print(f"  Mode: single city ({args.city})")
    print(f"  Processed:      {processed}")
    print(f"  Skipped:        {skipped}")
    print(f"  Blocked (CF):   {blocked}")
    print(f"  Failed:         {failed}")
    if args.dry_run:
        print(f"  Would upload:   {total_uploaded}")
    else:
        print(f"  Total uploaded: {total_uploaded}")
    print(f"\nRun --check-coverage to validate CDN availability")

    if failed > 0:
        print(f"\nWARNING: {failed} cities failed — check city_sources.json for failure_reason")


if __name__ == "__main__":
    main()
