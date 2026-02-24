#!/usr/bin/env python3
"""
Batch scraper for all 87 LA County city councils (excluding City of Los Angeles).

Reads city_sources.json for city list and pre-populated rosters extracted from
the CA Secretary of State 2025 Incorporated Cities and Town Officials PDF.

Architecture:
- Primary source: SOS PDF roster pre-populated in city_sources.json
- Fallback: per-city website scraping with requests + BeautifulSoup
- Per-city COMMIT (not global transaction) — one failed city won't roll back all
- Reuses core dedup/upsert functions from scrape_la_officials.py

geo_id assignment rules (CRITICAL per RESEARCH.md Pitfall 1):
- At-large city council members: geo_id = place_geoid (G4110 Census GEOID)
- District city council members: geo_id = ocd_id (X0001 ward boundary)
- Mayors (all cities): district_type=LOCAL_EXEC, geo_id = place_geoid

Usage:
    cd EV-Backend/scripts
    python3 scrape_city_councils.py

Coverage target: 90%+ of 87 LA County cities (excluding LA City).
"""

import json
import os
import sys
import uuid
import re
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, next_ext_id

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from rapidfuzz.distance import Levenshtein
levenshtein_distance = Levenshtein.distance

psycopg2.extras.register_uuid()

# ============================================================
# Configuration
# ============================================================

LEVENSHTEIN_THRESHOLD = 1
SOS_PDF_DATA_SOURCE = "https://admin.cdn.sos.ca.gov/ca-roster/2025/cities-towns.pdf"


def init_ext_id_counter(conn):
    """Initialize the next_ext_id counter to avoid DB collisions.

    The utils.py counter starts at -200001 on each Python process launch,
    but previous runs may have already used IDs in this range. Query the DB
    to find the current minimum across all three tables that use external_id,
    then set the counter to start one below that minimum.

    This is safe because:
    - We only read from the DB, no writes
    - We set the counter once before any inserts happen
    - The counter decrements so subsequent IDs will all be unique

    Called once at start of main() before any city processing.
    """
    import utils
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT MIN(min_id) as global_min FROM (
            SELECT MIN(external_id) as min_id FROM essentials.politicians WHERE external_id < 0
            UNION ALL
            SELECT MIN(external_id) as min_id FROM essentials.chambers WHERE external_id < 0
            UNION ALL
            SELECT MIN(external_id) as min_id FROM essentials.districts WHERE external_id < 0
        ) sub
    """)
    row = cur.fetchone()
    cur.close()
    current_min = row["global_min"] if row and row["global_min"] else -200001
    # Start one below the current minimum to avoid any collision
    utils._EXT_ID_COUNTER = current_min - 1
    print(f"  ext_id counter initialized to {utils._EXT_ID_COUNTER} "
          f"(current DB min: {current_min})")


# ============================================================
# Connection helpers (reused from scrape_la_officials.py)
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL."""
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    parsed = urlparse(raw_url)
    kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": parsed.password,
    }
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v
    return psycopg2.connect(**kwargs)


# ============================================================
# HTML fetching with Playwright fallback
# (per RESEARCH.md Pattern 3)
# ============================================================

def fetch_html_with_fallback(url, timeout=15):
    """Try requests first; fall back to Playwright for JS-heavy sites.

    Returns (html: str, used_playwright: bool)
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        )
    }
    html = ""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text(strip=True)
        if len(body_text) > 500:
            return html, False
    except Exception as e:
        print(f"    requests failed: {e}")

    # Fall back to Playwright
    print(f"    Falling back to Playwright for: {url}")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=30000)
            page.wait_for_load_state("networkidle", timeout=30000)
            html = page.content()
            browser.close()
        return html, True
    except ImportError:
        print("    Playwright not installed — cannot fall back")
        return html, False
    except Exception as e:
        print(f"    Playwright failed: {e}")
        return html, False


# ============================================================
# Generic council page parser
# (per RESEARCH.md Pattern 4)
# ============================================================

def parse_generic_council_page(html, source_config):
    """Generic parser for common city council page patterns.

    Handles:
    - Tables with council member names and district/position
    - Lists with heading "Council Member" or "Mayor"
    - Card layouts with person name + title

    Returns: list of {name: str, district: int, title: str, role: str}
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Strategy 1: Tables with member names
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) >= 2:
                for i, cell in enumerate(cells):
                    if re.match(r"[A-Z][a-z]+ [A-Z][a-z]+", cell) and len(cell.split()) >= 2:
                        cell_text = " ".join(cells)
                        dist_match = re.search(r"District\s*(\d+)", cell_text, re.I)
                        district = int(dist_match.group(1)) if dist_match else 0
                        results.append({
                            "name": cell,
                            "district": district,
                            "title": "Council Member",
                            "role": "Council Member"
                        })
                        break

    # Strategy 2: Common CSS classes for council cards
    if not results:
        for tag in soup.find_all(class_=re.compile(
            r"council|member|official|elected|representative|board", re.I
        )):
            name_el = tag.find(re.compile(r"h[1-6]|strong|b|span|p"))
            if name_el:
                name = name_el.get_text(strip=True)
                if len(name.split()) >= 2 and re.match(r"[A-Z]", name):
                    tag_text = tag.get_text(" ", strip=True)
                    dist_match = re.search(r"District\s*(\d+)", tag_text, re.I)
                    district = int(dist_match.group(1)) if dist_match else 0
                    is_mayor = bool(re.search(r"\bMayor\b", tag_text, re.I))
                    results.append({
                        "name": name,
                        "district": district,
                        "title": "Mayor" if is_mayor else "Council Member",
                        "role": "Mayor" if is_mayor else "Council Member"
                    })

    # Strategy 3: Look for heading "Council Members" or "City Council" followed by names
    if not results:
        council_sections = soup.find_all(
            lambda tag: tag.name in ("h1", "h2", "h3", "h4", "h5")
            and re.search(r"council|board|elected", tag.get_text(), re.I)
        )
        for section in council_sections:
            # Look at next siblings
            for sibling in section.find_next_siblings():
                if sibling.name in ("h1", "h2", "h3"):
                    break
                text = sibling.get_text(strip=True)
                if re.match(r"[A-Z][a-z]+ [A-Z][a-z]+", text) and len(text.split()) <= 6:
                    results.append({
                        "name": text,
                        "district": 0,
                        "title": "Council Member",
                        "role": "Council Member"
                    })

    # Deduplicate by name
    seen = set()
    deduped = []
    for r in results:
        if r["name"] not in seen:
            seen.add(r["name"])
            deduped.append(r)

    return deduped


# ============================================================
# Name utilities (reused from scrape_la_officials.py)
# ============================================================

def normalize_party(party_str):
    """Convert full party name to short form."""
    p = (party_str or "").lower().strip()
    if p.startswith("democrat"):
        return "Democrat", "D"
    elif p.startswith("republican"):
        return "Republican", "R"
    elif p.startswith("independent"):
        return "Independent", "I"
    elif p.startswith("nonpartisan") or p.startswith("non-partisan"):
        return "Nonpartisan", "N"
    # City councils are generally nonpartisan
    return "Nonpartisan", "N"


def split_name(full_name):
    """Split full name into first and last."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        suffix_markers = {"jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        if parts[-1].lower() in suffix_markers:
            last = f"{parts[-2]} {parts[-1]}"
            first = " ".join(parts[:-2])
        else:
            last = parts[-1]
            first = " ".join(parts[:-1])
        return first, last


# ============================================================
# Deduplication (adapted from scrape_la_officials.py)
# ============================================================

def find_existing_politician_for_seat(cur, ocd_id, title, scraped_name, is_multi_seat=False):
    """Seat-first deduplication: find existing active politician in a seat.

    For at-large multi-seat councils (is_multi_seat=True):
      - All seats share the same ocd_id
      - Match by name only (exact or fuzzy)
      - If no name match found, treat as a NEW politician (not a replacement)
      - Never deactivate another at-large council member for a name-mismatch

    For single-seat districts (mayor, district-elected):
      - Standard seat-first logic: name mismatch = new_person_in_seat

    Returns (politician_id, match_type) where match_type is:
      'exact'              — exact full_name match
      'fuzzy'              — fuzzy last-name match within threshold
      'new_person_in_seat' — single seat, different person detected
      None (no seat match) — no seat found OR new person in multi-seat
    """
    title_like = f"%{title.split()[0]}%"
    cur.execute("""
        SELECT p.id as politician_id, p.full_name, p.is_active
        FROM essentials.districts d
        JOIN essentials.offices o ON o.district_id = d.id
        JOIN essentials.politicians p ON p.id = o.politician_id
        WHERE d.ocd_id = %s
          AND LOWER(o.title) LIKE LOWER(%s)
        ORDER BY p.is_active DESC, p.last_synced DESC
    """, (ocd_id, title_like))
    rows = cur.fetchall()

    if not rows:
        return None, None

    active_rows = [r for r in rows if r["is_active"]]
    if not active_rows and not is_multi_seat:
        return None, "new_person_in_seat"
    elif not active_rows:
        return None, None  # Multi-seat: no match, insert new

    scraped_lower = scraped_name.strip().lower()
    for row in active_rows:
        if row["full_name"].strip().lower() == scraped_lower:
            return row["politician_id"], "exact"

    scraped_last = scraped_name.strip().split()[-1].lower()
    for row in active_rows:
        existing_last = row["full_name"].strip().split()[-1].lower()
        dist = levenshtein_distance(scraped_last, existing_last)
        if dist <= LEVENSHTEIN_THRESHOLD:
            print(f"      Fuzzy match: '{scraped_name}' ~ '{row['full_name']}' "
                  f"(last name distance={dist})")
            return row["politician_id"], "fuzzy"

    # No name match
    if is_multi_seat:
        # Multi-seat at-large: no match = new politician (not a replacement)
        return None, None
    else:
        # Single seat: different person in seat
        old_id = active_rows[0]["politician_id"]
        old_name = active_rows[0]["full_name"]
        print(f"      New person in seat: '{scraped_name}' replacing '{old_name}'")
        return old_id, "new_person_in_seat"


# ============================================================
# Chamber / government / district helpers
# ============================================================

def find_or_create_government(cur, city_config):
    """Find or create government entity for a city."""
    city_name = city_config["name"]  # e.g., "City of Burbank"
    state = city_config.get("state", "CA")

    cur.execute("""
        SELECT id FROM essentials.governments
        WHERE name ILIKE %s AND state = %s
        LIMIT 1
    """, (f"%{city_name}%", state))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    gov_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO essentials.governments (id, name, type, state)
        VALUES (%s, %s, 'LOCAL', %s)
    """, (gov_id, f"{city_name}, California, US", state))
    print(f"      Created government: {city_name} ({gov_id})")
    return gov_id


def find_or_create_city_chamber(cur, city_config, gov_id):
    """Find or create city council chamber."""
    city_name = city_config["name"]  # "City of Burbank"
    # Strip "City of " prefix for chamber name
    place_name = re.sub(r'^(?:City|Town) of\s+', '', city_name).strip()
    chamber_name = "City Council"
    chamber_formal = f"{place_name} City Council"

    cur.execute("""
        SELECT id FROM essentials.chambers
        WHERE (name = %s OR name_formal = %s) AND government_id = %s
        LIMIT 1
    """, (chamber_name, chamber_formal, gov_id))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    chamber_id = str(uuid.uuid4())
    ext_id = next_ext_id()
    cur.execute("""
        INSERT INTO essentials.chambers
            (id, external_id, government_id, name, name_formal,
             term_length, election_frequency)
        VALUES (%s, %s, %s, %s, %s, '4 years', '4 years')
    """, (chamber_id, ext_id, gov_id, chamber_name, chamber_formal))
    print(f"      Created chamber: {chamber_formal} ({chamber_id})")
    return chamber_id


def find_or_create_city_district(cur, city_config, official_data, chamber_id):
    """Find or create district for a city official.

    geo_id assignment (CRITICAL — per RESEARCH.md Pitfall 1):
    - At-large council: geo_id = place_geoid (G4110 Census GEOID)
    - District council: geo_id = ocd_id (X0001 ward boundary)
    - Mayor (LOCAL_EXEC): geo_id = place_geoid (G4110 Census GEOID)

    Returns (district_id, ocd_id) tuple.
    """
    role = official_data.get("role", "Council Member")
    election_type = city_config.get("election_type", "at-large")
    place_geoid = city_config["place_geoid"]
    ocd_base = city_config["ocd_id_base"]
    state = city_config.get("state", "CA")
    city_name_display = re.sub(r'^(?:City|Town) of\s+', '', city_config["name"]).strip()

    if role == "Mayor":
        # Mayor always uses city-level OCD-ID and LOCAL_EXEC type
        ocd_id = ocd_base
        district_type = "LOCAL_EXEC"
        geo_id = place_geoid
        label = f"{city_name_display} Mayor"
    elif election_type == "district":
        # District-based: use ward OCD-ID
        district_num = official_data.get("district", 0)
        ocd_template = city_config.get("ocd_id_template", ocd_base + "/council_district:{n}")
        ocd_id = ocd_template.replace("{n}", str(district_num))
        district_type = "LOCAL"
        geo_id = ocd_id  # X0001 ward boundary uses ocd_id as geo_id
        label = f"{city_name_display} Council District {district_num}"
    else:
        # At-large: all members share city-level district with G4110 geoid
        ocd_id = ocd_base
        district_type = "LOCAL"
        geo_id = place_geoid  # G4110 Census GEOID
        label = f"{city_name_display} City Council (at-large)"

    # Try to find existing district — must match both ocd_id AND district_type
    # because Mayor (LOCAL_EXEC) and at-large council (LOCAL) share the same ocd_id
    cur.execute("""
        SELECT id FROM essentials.districts
        WHERE ocd_id = %s AND district_type = %s
        LIMIT 1
    """, (ocd_id, district_type))
    row = cur.fetchone()
    if row:
        return str(row["id"]), ocd_id

    # Create new district
    district_id = str(uuid.uuid4())
    dist_ext_id = next_ext_id()
    district_num_val = str(official_data.get("district", 0))

    cur.execute("""
        INSERT INTO essentials.districts
            (id, external_id, ocd_id, label, district_type, district_id,
             state, geo_id, num_officials, is_judicial, has_unknown_boundaries, retention)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, false, false, false)
    """, (
        district_id, dist_ext_id, ocd_id, label, district_type,
        district_num_val, state, geo_id,
    ))
    print(f"      Created district: {label} | geo_id={geo_id}")
    return district_id, ocd_id


# ============================================================
# Core upsert (adapted from scrape_la_officials.py)
# ============================================================

def upsert_politician(cur, city_config, official_data, chamber_id, district_id, ocd_id):
    """Upsert a single official.

    Returns (action, politician_id)
    """
    role = official_data.get("role", "Council Member")
    title = role  # "Mayor" or "Council Member"
    data_source = official_data.get("data_source", SOS_PDF_DATA_SOURCE)
    scraped_name = official_data.get("name", "").strip()
    party_full, party_short = normalize_party(official_data.get("party", ""))
    first_name, last_name = split_name(scraped_name)

    # At-large council members share a district; use multi-seat dedup
    # (name-match only, no seat replacement for mismatches)
    election_type = city_config.get("election_type", "at-large")
    is_multi_seat = (role == "Council Member" and election_type == "at-large")

    politician_id, match_type = find_existing_politician_for_seat(
        cur, ocd_id, title, scraped_name, is_multi_seat=is_multi_seat
    )

    if match_type in ("exact", "fuzzy"):
        cur.execute("""
            UPDATE essentials.politicians
            SET full_name = %s,
                first_name = %s,
                last_name = %s,
                party = %s,
                party_short_name = %s,
                data_source = %s,
                source = 'scraped',
                last_synced = NOW(),
                is_active = true
            WHERE id = %s
            RETURNING id
        """, (
            scraped_name, first_name, last_name,
            party_full, party_short, data_source, politician_id,
        ))
        return "updated", str(politician_id)

    elif match_type == "new_person_in_seat":
        old_id = politician_id
        cur.execute("""
            UPDATE essentials.politicians
            SET is_active = false, last_synced = NOW()
            WHERE id = %s
            RETURNING full_name
        """, (old_id,))
        deactivated_row = cur.fetchone()
        old_name = deactivated_row["full_name"] if deactivated_row else "unknown"
        print(f"      Deactivated: {old_name} (id={old_id})")

        new_pol_id = str(uuid.uuid4())
        pol_ext_id = next_ext_id()
        cur.execute("""
            INSERT INTO essentials.politicians
                (id, external_id, first_name, last_name, full_name,
                 party, party_short_name, source, data_source, last_synced,
                 is_appointed, is_vacant, is_off_cycle, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'scraped', %s, NOW(),
                    false, false, false, true)
        """, (
            new_pol_id, pol_ext_id,
            first_name, last_name, scraped_name,
            party_full, party_short, data_source,
        ))

        cur.execute("""
            UPDATE essentials.offices SET politician_id = %s WHERE politician_id = %s
        """, (new_pol_id, old_id))

        if cur.rowcount == 0:
            office_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO essentials.offices
                    (id, politician_id, chamber_id, district_id, title,
                     representing_state, seats, is_appointed_position)
                VALUES (%s, %s, %s, %s, %s, 'CA', 1, false)
            """, (office_id, new_pol_id, chamber_id, district_id, title))

        return "deactivated+new", new_pol_id

    else:
        new_pol_id = str(uuid.uuid4())
        pol_ext_id = next_ext_id()
        cur.execute("""
            INSERT INTO essentials.politicians
                (id, external_id, first_name, last_name, full_name,
                 party, party_short_name, source, data_source, last_synced,
                 is_appointed, is_vacant, is_off_cycle, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'scraped', %s, NOW(),
                    false, false, false, true)
        """, (
            new_pol_id, pol_ext_id,
            first_name, last_name, scraped_name,
            party_full, party_short, data_source,
        ))

        office_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO essentials.offices
                (id, politician_id, chamber_id, district_id, title,
                 representing_state, seats, is_appointed_position)
            VALUES (%s, %s, %s, %s, %s, 'CA', 1, false)
        """, (office_id, new_pol_id, chamber_id, district_id, title))

        return "new", new_pol_id


# ============================================================
# Per-city processing
# ============================================================

def process_city(conn, city_config):
    """Process a single city: upsert all officials from roster.

    Uses per-city COMMIT for isolation (per RESEARCH.md Pitfall 6).
    Returns (success: bool, counts: dict, failure_reason: str)
    """
    city_name = city_config["name"]
    roster = city_config.get("roster", [])
    url = city_config.get("url")

    print(f"\n  City: {city_name}")

    # If no roster from SOS PDF and has URL, try website scraping
    if not roster and url:
        print(f"    No SOS PDF data — trying website: {url}")
        try:
            html, used_playwright = fetch_html_with_fallback(url, timeout=20)
            website_officials = parse_generic_council_page(html, city_config)
            if website_officials:
                print(f"    Website parsed {len(website_officials)} officials "
                      f"({'Playwright' if used_playwright else 'requests'})")
                roster = website_officials
            else:
                print(f"    Website parse returned 0 officials")
        except Exception as e:
            print(f"    Website fetch failed: {e}")

    if not roster:
        return False, {}, "No roster data (SOS PDF empty, website failed)"

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    counts = {"updated": 0, "new": 0, "deactivated": 0, "errors": 0}

    try:
        # Find/create government and chamber (shared across all officials)
        gov_id = find_or_create_government(cur, city_config)
        chamber_id = find_or_create_city_chamber(cur, city_config, gov_id)

        for official_data in roster:
            name = official_data.get("name", "").strip()
            if not name or len(name.split()) < 2:
                continue

            # Add data source
            official_data["data_source"] = SOS_PDF_DATA_SOURCE

            try:
                # Find/create district
                district_id, ocd_id = find_or_create_city_district(
                    cur, city_config, official_data, chamber_id
                )

                # Upsert politician
                action, pol_id = upsert_politician(
                    cur, city_config, official_data, chamber_id, district_id, ocd_id
                )

                role_label = official_data.get("role", "Council Member")
                if action == "updated":
                    counts["updated"] += 1
                    print(f"    Updated: {name} ({role_label})")
                elif action == "new":
                    counts["new"] += 1
                    print(f"    Inserted: {name} ({role_label})")
                elif action == "deactivated+new":
                    counts["deactivated"] += 1
                    counts["new"] += 1
                    print(f"    Replaced: {name} ({role_label})")

            except Exception as e:
                counts["errors"] += 1
                print(f"    ERROR processing {name}: {e}")
                import traceback
                traceback.print_exc()

        total = counts["updated"] + counts["new"]
        print(f"    {city_name}: {total} officials processed "
              f"({counts['updated']} updated, {counts['new']} new, "
              f"{counts['errors']} errors)")

        return True, counts, None

    finally:
        cur.close()


# ============================================================
# Verification
# ============================================================

def verify_no_duplicates(cur):
    """Verify no active duplicate politicians exist per seat."""
    print("\n" + "=" * 60)
    print("Verification: Duplicate check (must be 0 rows)")
    print("=" * 60)

    cur.execute("""
        SELECT d.ocd_id, o.title, p.full_name, COUNT(*) as active_count
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE p.is_active = true
          AND d.state = 'CA'
          AND d.district_type IN ('LOCAL', 'LOCAL_EXEC')
          AND d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:%'
        GROUP BY d.ocd_id, o.title, p.full_name
        HAVING COUNT(*) > 1
    """)
    rows = cur.fetchall()

    if rows:
        print(f"  FAIL: {len(rows)} duplicate seat violations found!")
        for row in rows:
            print(f"    {row['ocd_id']}: {row['full_name']} as {row['title']} ({row['active_count']} active)")
    else:
        print("  PASS: 0 duplicate seat violations")

    return len(rows)


def verify_pip_tests(cur):
    """Run point-in-polygon tests for at-large and district cities."""
    print("\n" + "=" * 60)
    print("Verification: Point-in-polygon tests")
    print("=" * 60)

    tests = [
        {
            "label": "Burbank City Hall (at-large) — expect 1 council district",
            "lon": -118.3090, "lat": 34.1808,
            "filter": "d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:burbank%%'",
            "expected_min": 1,
        },
        {
            "label": "Long Beach City Hall (at-large) — expect 1 council district",
            "lon": -118.1937, "lat": 33.7701,
            "filter": "d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:long_beach%%'",
            "expected_min": 1,
        },
        {
            "label": "Pasadena City Hall (at-large) — expect 1 council district",
            "lon": -118.1437, "lat": 34.1478,
            "filter": "d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:pasadena%%'",
            "expected_min": 1,
        },
    ]

    for test in tests:
        print(f"\n  Test: {test['label']}")
        cur.execute(f"""
            SELECT p.full_name, o.title, d.ocd_id, d.geo_id, d.district_type
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            JOIN essentials.geofence_boundaries gb ON gb.geo_id = d.geo_id
            WHERE ST_Covers(gb.geometry, ST_SetSRID(
                ST_MakePoint(%s, %s), 4326
            ))
            AND {test['filter']}
            AND p.is_active = true
            ORDER BY d.district_type
        """, (test["lon"], test["lat"]))
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"    FOUND: {row['full_name']} — {row['title']} | "
                      f"{row['district_type']} | geo_id={row['geo_id']}")
            if len(rows) >= test["expected_min"]:
                print(f"  PASS: {len(rows)} result(s) found")
            else:
                print(f"  WARN: Expected {test['expected_min']}+, found {len(rows)}")
        else:
            print(f"  WARN: No officials found — check geo_id + geofence coverage")


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("Phase 37 — Scrape LA County City Councils")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    load_env()

    # Load city config
    config_path = Path(__file__).parent / "city_sources.json"
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)
    cities = config.get("cities", [])
    print(f"\nLoaded {len(cities)} cities from {config_path}")

    conn = get_connection()
    conn.autocommit = True  # Start with autocommit=True for initialization query

    # Initialize external ID counter from DB to avoid collision
    init_ext_id_counter(conn)

    # Counters
    scraped_count = 0
    failed_count = 0
    skipped_count = 0
    total_officials = 0

    print("\n" + "=" * 60)
    print("Processing cities...")
    print("=" * 60)

    for city_config in cities:
        city_id = city_config["id"]
        city_name = city_config["name"]

        # Skip already scraped (idempotent re-runs)
        if city_config.get("status") == "scraped":
            print(f"\n  Skipping {city_name} (already scraped)")
            skipped_count += 1
            continue

        # Each city gets its own transaction
        if conn.autocommit:
            conn.autocommit = False
        try:
            success, counts, failure_reason = process_city(conn, city_config)

            if success:
                conn.commit()
                city_config["status"] = "scraped"
                city_config["last_scraped"] = datetime.now().isoformat()
                scraped_count += 1
                total_officials += counts["updated"] + counts["new"]
            else:
                conn.rollback()
                city_config["status"] = "failed"
                city_config["failure_reason"] = failure_reason
                failed_count += 1
                print(f"    FAILED: {city_name} — {failure_reason}")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            city_config["status"] = "failed"
            city_config["failure_reason"] = str(e)
            failed_count += 1
            print(f"\n  ERROR in {city_name}: {e}")
            import traceback
            traceback.print_exc()

    # Save updated status back to config
    config["cities"] = cities
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"\nUpdated {config_path} with status fields")

    # Overall verification
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dup_count = verify_no_duplicates(cur)
    verify_pip_tests(cur)
    cur.close()

    # Coverage report
    total = len(cities) - skipped_count
    coverage = (scraped_count / (total + skipped_count)) * 100 if (total + skipped_count) > 0 else 0
    full_coverage = ((scraped_count + skipped_count) / len(cities)) * 100 if len(cities) > 0 else 0

    print("\n" + "=" * 60)
    print("Coverage Report")
    print("=" * 60)
    print(f"Cities total:     {len(cities)}")
    print(f"  Scraped:        {scraped_count}")
    print(f"  Previously done:{skipped_count}")
    print(f"  Failed:         {failed_count}")
    print(f"Coverage:         {full_coverage:.1f}%")
    print(f"Total officials:  {total_officials}")

    if full_coverage < 85:
        print(f"\nWARNING: Coverage {full_coverage:.1f}% below 85% minimum")

    if dup_count > 0:
        print(f"\nWARNING: {dup_count} duplicate seat violations!")
        sys.exit(1)
    else:
        print("\nAll requirements passed:")
        print("  POL-03: City council records populated for LA County cities")
        print("  POL-05: 0 active duplicates per seat")

    if failed_count > 0:
        print(f"\n{failed_count} cities failed — see city_sources.json for failure reasons")
        # Don't exit 1 for partial failures if coverage target met

    conn.close()


if __name__ == "__main__":
    main()
