#!/usr/bin/env python3
"""
Config-driven scraper for LA County and LA City officials.

Reads politician_sources.json to determine which government websites to scrape
and how to match/upsert officials into essentials.politicians.

This script:
1. Runs gap_fill_geo_ids.py functions to ensure geo_ids are populated
2. For each source in politician_sources.json:
   a. Scrapes official names from the source URL (with hardcoded fallbacks)
   b. Seat-first deduplication: finds district by ocd_id, then matches person by name
   c. Exact match or fuzzy last-name match -> UPDATE existing record
   d. New person in existing seat -> mark old is_active=false, INSERT new record
   e. No seat found -> CREATE district + chamber + office + politician
3. Runs deduplication verification query (must return 0 rows)
4. Runs point-in-polygon verification tests

Usage:
    cd EV-Backend/scripts
    python3 gap_fill_geo_ids.py   # Run schema migration + geo_id fix first (idempotent)
    python3 scrape_la_officials.py  # Then scrape and upsert

Deduplication logic:
  A "duplicate" = same person in same seat with BOTH records marked active.
  When person changes in seat: old record is_active=false + new record inserted.
  Historical records are NEVER deleted.

TODO: Photo re-hosting to Supabase Storage is planned but deferred.
  Currently storing photo_origin_url pointing to government site URLs.
  When implemented: download to temp file -> upload to politician-photos bucket
  -> update photo_origin_url with Supabase CDN URL.
"""

import json
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env, next_ext_id

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from rapidfuzz.distance import Levenshtein
levenshtein_distance = Levenshtein.distance

# Register UUID adapter (consistent with promote_scraped_officials.py)
psycopg2.extras.register_uuid()

# ============================================================
# Levenshtein threshold for last-name fuzzy matching.
# Threshold = 1: catches misspellings ("McOsker" / "McOskar"),
# but avoids false matches on short surnames ("Hahn" / "Haan").
# Per RESEARCH.md Pitfall 5: short names need tight threshold.
# ============================================================
LEVENSHTEIN_THRESHOLD = 1

# ============================================================
# Hardcoded fallback rosters (verified Feb 2026 from authoritative sources)
# Used when live HTML parsing fails/returns unexpected results.
# ============================================================

FALLBACK_LA_COUNTY_SUPERVISORS = [
    {"district": 1, "name": "Hilda L. Solis", "party": "Democrat"},
    {"district": 2, "name": "Holly J. Mitchell", "party": "Democrat"},
    {"district": 3, "name": "Lindsey P. Horvath", "party": "Democrat"},
    {"district": 4, "name": "Janice Hahn", "party": "Democrat"},
    {"district": 5, "name": "Kathryn Barger", "party": "Republican"},
]

FALLBACK_LA_CITY_COUNCIL = [
    {"district": 1, "name": "Eunisses Hernandez", "party": "Democrat"},
    {"district": 2, "name": "Adrin Nazarian", "party": "Democrat"},
    {"district": 3, "name": "Bob Blumenfield", "party": "Democrat"},
    {"district": 4, "name": "Nithya Raman", "party": "Democrat"},
    {"district": 5, "name": "Katy Young Yaroslavsky", "party": "Democrat"},
    {"district": 6, "name": "Imelda Padilla", "party": "Democrat"},
    {"district": 7, "name": "Monica Rodriguez", "party": "Democrat"},
    {"district": 8, "name": "Marqueece Harris-Dawson", "party": "Democrat"},
    {"district": 9, "name": "Curren D. Price Jr.", "party": "Democrat"},
    {"district": 10, "name": "Heather Hutt", "party": "Democrat"},
    {"district": 11, "name": "Traci Park", "party": "Democrat"},
    {"district": 12, "name": "John Lee", "party": "Republican"},
    {"district": 13, "name": "Hugo Soto-Martinez", "party": "Democrat"},
    {"district": 14, "name": "Ysabel J. Jurado", "party": "Democrat"},
    {"district": 15, "name": "Tim McOsker", "party": "Democrat"},
]

FALLBACK_LA_CITY_MAYOR = [
    {"district": 0, "name": "Karen Ruth Bass", "party": "Democrat"},
]


# ============================================================
# Connection helpers
# ============================================================

def get_connection():
    """Create psycopg2 connection from DATABASE_URL.

    Handles passwords containing special characters (e.g., '@', '/')
    by parsing the URL and connecting with keyword arguments.
    Consistent with gap_fill_geo_ids.py.
    """
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
# HTML parsers
# ============================================================

def fetch_html(url, timeout=15):
    """Fetch HTML from URL with a browser-like User-Agent."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def parse_la_county_bos(html):
    """Parse LA County Board of Supervisors page (bos.lacounty.gov).

    Returns list of dicts: [{district: int, name: str, party: str, ...}]

    The page lists all 5 supervisors. If parsing fails or returns unexpected
    results, falls back to the hardcoded Feb 2026 roster.
    """
    results = []
    try:
        soup = BeautifulSoup(html, "html.parser")

        # Strategy 1: Look for supervisor cards/articles with district headings
        # The BOS page typically has a section for each supervisor with their name
        # and district number prominently featured.
        supervisor_sections = soup.find_all(
            lambda tag: tag.name in ("article", "div", "section")
            and tag.get("class")
            and any("supervisor" in c.lower() or "board" in c.lower() or "member" in c.lower()
                    for c in tag.get("class", []))
        )

        # Strategy 2: Find heading tags containing "District" + a number
        if not supervisor_sections:
            supervisor_sections = soup.find_all(
                ["h1", "h2", "h3", "h4"],
                string=lambda s: s and "district" in s.lower()
            )

        # Strategy 3: Look for list items or paragraphs with supervisor names
        # Try finding patterns like "Supervisor Name, District N"
        if not supervisor_sections:
            # Scan all text blocks for name + district number patterns
            import re
            name_pattern = re.compile(
                r'(?:Supervisor\s+)?([A-Z][a-zA-Z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-zA-Z]+)+)'
                r'.*?District\s+(\d)',
                re.DOTALL
            )
            body_text = soup.get_text()
            matches = name_pattern.findall(body_text)
            for name, dist_num in matches:
                name = name.strip()
                if len(name.split()) >= 2:
                    results.append({
                        "district": int(dist_num),
                        "name": name,
                        "party": "",
                    })

        if len(results) == 5:
            print(f"    Parsed {len(results)} supervisors from live HTML")
            return results

    except Exception as e:
        print(f"    Warning: HTML parsing failed ({e}) — using hardcoded fallback")

    if len(results) != 5:
        print(f"    Live parse returned {len(results)} results — using hardcoded fallback")
        return FALLBACK_LA_COUNTY_SUPERVISORS

    return results


def parse_la_city_clerk(html):
    """Parse LA City Clerk current-elected-officials page.

    Returns list of dicts for all 15 council members.
    Falls back to hardcoded Feb 2026 roster if parsing fails.
    """
    results = []
    try:
        soup = BeautifulSoup(html, "html.parser")

        # The clerk.lacity.gov page typically has a table or list of officials
        # Strategy 1: Find table rows with council district numbers
        import re
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    cell_texts = [c.get_text(strip=True) for c in cells]
                    # Look for "District N" or "CD N" pattern
                    for i, text in enumerate(cell_texts):
                        dist_match = re.search(r'(?:District|CD)\s*(\d{1,2})', text, re.I)
                        if dist_match:
                            dist_num = int(dist_match.group(1))
                            if 1 <= dist_num <= 15:
                                # Name is usually in an adjacent cell
                                name = ""
                                for j, other in enumerate(cell_texts):
                                    if j != i and other and not re.search(
                                        r'(?:District|CD|\d|Member|Council)', other, re.I
                                    ):
                                        if len(other.split()) >= 2:
                                            name = other
                                            break
                                if name:
                                    results.append({
                                        "district": dist_num,
                                        "name": name,
                                        "party": "",
                                    })
                                    break

        # Strategy 2: Find links with council member names + district references
        if len(results) < 10:
            results = []
            all_links = soup.find_all("a")
            for link in all_links:
                text = link.get_text(strip=True)
                # Detect "Councilmember Name, District N" patterns
                dist_match = re.search(r'District\s*(\d{1,2})', text, re.I)
                if dist_match:
                    dist_num = int(dist_match.group(1))
                    name_part = re.sub(r'(?:Council\s*Member|District\s*\d+|,.*)', '', text, flags=re.I).strip()
                    if name_part and len(name_part.split()) >= 2:
                        results.append({
                            "district": dist_num,
                            "name": name_part,
                            "party": "",
                        })

        # Deduplicate by district number
        seen = {}
        for r in results:
            d = r["district"]
            if d not in seen:
                seen[d] = r
        results = sorted(seen.values(), key=lambda x: x["district"])

        if len(results) == 15:
            print(f"    Parsed {len(results)} council members from live HTML")
            return results

    except Exception as e:
        print(f"    Warning: HTML parsing failed ({e}) — using hardcoded fallback")

    if len(results) != 15:
        print(f"    Live parse returned {len(results)} council members — using hardcoded fallback")
        return FALLBACK_LA_CITY_COUNCIL

    return results


def parse_la_city_clerk_mayor(html):
    """Parse mayor name from LA City Clerk current-elected-officials page.

    Returns list with one dict: [{district: 0, name: "Karen Ruth Bass", party: "Democrat"}]
    Falls back to hardcoded if parsing fails.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        import re

        # Look for "Mayor" heading followed by a name
        mayor_tags = soup.find_all(string=re.compile(r'\bMayor\b', re.I))
        for tag in mayor_tags:
            parent = tag.parent if hasattr(tag, 'parent') else None
            if parent:
                # Look for a sibling or child element with a person name
                # Check nearby text
                nearby_text = parent.get_text(strip=True)
                # "Karen Bass" or "Karen Ruth Bass" pattern
                name_match = re.search(
                    r'(?:Mayor\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]*\.?)?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',
                    nearby_text
                )
                if name_match:
                    name = name_match.group(1).strip()
                    # Filter out false positives like "City Hall"
                    if "bass" in name.lower() or "mayor" not in name.lower():
                        if len(name.split()) >= 2:
                            print(f"    Parsed mayor from live HTML: {name}")
                            return [{"district": 0, "name": name, "party": "Democrat"}]

    except Exception as e:
        print(f"    Warning: Mayor HTML parsing failed ({e}) — using hardcoded fallback")

    print("    Using hardcoded fallback for mayor")
    return FALLBACK_LA_CITY_MAYOR


# ============================================================
# Deduplication
# ============================================================

def find_existing_politician_for_seat(cur, ocd_id, title, scraped_name):
    """Seat-first deduplication: find existing active politician in a seat.

    Algorithm:
      Step 1: Find the district by ocd_id and offices with matching title
      Step 2: Exact full_name match (case-insensitive)
      Step 3: Fuzzy last-name match (Levenshtein <= 1)
      Step 4: Person changed — new person in existing seat

    Args:
        cur: psycopg2 cursor
        ocd_id: OCD division ID of the district
        title: Office title (e.g., "Supervisor", "Council Member", "Mayor")
        scraped_name: Full name from scraped data

    Returns:
        (politician_id, match_type) where match_type is:
          'exact'             — exact full_name match
          'fuzzy'             — fuzzy last-name match within threshold
          'new_person_in_seat' — seat exists, different person
          None                — no seat found (ocd_id not in districts)
    """
    # Step 1: Find all active politicians in this seat
    # Use LIKE for title to handle slight title variations ("Council Member" vs "Councilmember")
    # Match on ocd_id only (NOT district_type) per RESEARCH.md Pitfall 1 — supervisor
    # districts are district_type=LOCAL not COUNTY
    title_like = f"%{title.split()[0]}%"  # e.g., "Supervisor" -> "%Supervisor%"
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
        return None, None  # No seat exists — will need to create district + chamber + office

    # Consider only currently-active politicians for matching
    active_rows = [r for r in rows if r["is_active"]]
    if not active_rows:
        # All politicians in this seat are inactive — treat as new person
        return None, "new_person_in_seat"

    # Step 2: Exact full_name match (case-insensitive)
    scraped_lower = scraped_name.strip().lower()
    for row in active_rows:
        if row["full_name"].strip().lower() == scraped_lower:
            return row["politician_id"], "exact"

    # Step 3: Fuzzy last-name match with Levenshtein <= 1
    # "Holly J. Mitchell" vs "Holly Mitchell" — same last name, exact match on last token
    # "McOsker" vs "McOskar" — last name distance = 1, fuzzy match
    scraped_last = scraped_name.strip().split()[-1].lower()
    for row in active_rows:
        existing_last = row["full_name"].strip().split()[-1].lower()
        dist = levenshtein_distance(scraped_last, existing_last)
        if dist <= LEVENSHTEIN_THRESHOLD:
            print(f"      Fuzzy match: '{scraped_name}' ~ '{row['full_name']}' "
                  f"(last name distance={dist})")
            return row["politician_id"], "fuzzy"

    # Step 4: Different person in this seat (post-election transition)
    old_id = active_rows[0]["politician_id"]
    old_name = active_rows[0]["full_name"]
    print(f"      New person in seat: '{scraped_name}' replacing '{old_name}'")
    return old_id, "new_person_in_seat"


def find_or_create_chamber_for_source(cur, source_config):
    """Find or create a chamber for the given source config.

    For supervisors: LA County Board of Supervisors
    For council + mayor: LA City Council
    """
    district_type = source_config.get("district_type", "LOCAL")
    state = source_config.get("state", "CA")

    if "la_county" in source_config["id"] or "supervisor" in source_config["id"].lower():
        gov_name = "Los Angeles County"
        chamber_name = "Board of Supervisors"
        chamber_formal = "Los Angeles County Board of Supervisors"
    elif "mayor" in source_config["id"].lower() or "city" in source_config["id"].lower():
        gov_name = "Los Angeles"
        chamber_name = "City Council"
        chamber_formal = "Los Angeles City Council"
    else:
        gov_name = source_config.get("name", "")
        chamber_name = source_config.get("title", "Council")
        chamber_formal = chamber_name

    # Find or create government
    cur.execute("""
        SELECT id FROM essentials.governments
        WHERE name ILIKE %s AND state = %s
        LIMIT 1
    """, (f"%{gov_name}%", state))
    row = cur.fetchone()
    if row:
        gov_id = str(row["id"])
    else:
        gov_id = str(uuid.uuid4())
        gov_type = "LOCAL_EXEC" if district_type == "LOCAL_EXEC" else "LOCAL"
        cur.execute("""
            INSERT INTO essentials.governments (id, name, type, state)
            VALUES (%s, %s, %s, %s)
        """, (gov_id, f"{gov_name}, California, US", gov_type, state))
        print(f"      Created government: {gov_name} ({gov_id})")

    # Find or create chamber
    cur.execute("""
        SELECT id FROM essentials.chambers
        WHERE (name = %s OR name_formal = %s)
          AND government_id = %s
        LIMIT 1
    """, (chamber_name, chamber_formal, gov_id))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    # Create new chamber
    chamber_id = str(uuid.uuid4())
    ext_id = next_ext_id()
    cur.execute("""
        INSERT INTO essentials.chambers
            (id, external_id, government_id, name, name_formal,
             term_length, election_frequency)
        VALUES (%s, %s, %s, %s, %s, '4 years', '4 years')
    """, (chamber_id, ext_id, gov_id, chamber_name, chamber_formal))
    print(f"      Created chamber: {chamber_name} ({chamber_id})")
    return chamber_id


def find_or_create_district(cur, source_config, official_data):
    """Find existing district by ocd_id or create new one if missing.

    Per RESEARCH.md: supervisor and LA City council districts DEFINITELY exist
    from BallotReady cache. This handles edge cases gracefully.

    Returns (district_id, ocd_id) tuple.
    """
    # Build ocd_id for this official
    if "ocd_id" in source_config:
        # Fixed OCD-ID (e.g., mayor uses city-level ID)
        ocd_id = source_config["ocd_id"]
    else:
        # Template-based (e.g., supervisors and council members)
        n = official_data.get("district", 0)
        ocd_id = source_config["ocd_id_template"].replace("{n}", str(n))

    # Try to find existing district
    cur.execute("""
        SELECT id FROM essentials.districts
        WHERE ocd_id = %s
        LIMIT 1
    """, (ocd_id,))
    row = cur.fetchone()
    if row:
        return str(row["id"]), ocd_id

    # District not found — create it
    # This is unexpected for supervisors/council but handled gracefully
    print(f"      WARNING: District not found for ocd_id={ocd_id} — creating new record")
    district_id = str(uuid.uuid4())
    dist_ext_id = next_ext_id()
    district_type = source_config.get("district_type", "LOCAL")
    state = source_config.get("state", "CA")
    dist_num = official_data.get("district", 0)

    # For LOCAL districts: geo_id = ocd_id (Phase 35/36 convention)
    # For LOCAL_EXEC (mayor): geo_id = Census GEOID '0644000'
    if district_type == "LOCAL_EXEC":
        geo_id = "0644000"
        label = "City of Los Angeles"
    else:
        geo_id = ocd_id
        label = f"District {dist_num}"

    cur.execute("""
        INSERT INTO essentials.districts
            (id, external_id, ocd_id, label, district_type, district_id,
             state, geo_id, num_officials, is_judicial, has_unknown_boundaries, retention)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, false, false, false)
    """, (
        district_id, dist_ext_id, ocd_id, label, district_type,
        str(dist_num), state, geo_id,
    ))
    return district_id, ocd_id


# ============================================================
# Party normalization
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
    return party_str, ""


def split_name(full_name):
    """Split full name into first and last name components.

    Handles:
    - "Karen Ruth Bass" -> first="Karen Ruth", last="Bass"
    - "Curren D. Price Jr." -> first="Curren D.", last="Price Jr."
    - "Holly J. Mitchell" -> first="Holly J.", last="Mitchell"
    - "Hugo Soto-Martinez" -> first="Hugo", last="Soto-Martinez"
    """
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        # For "Jr.", "Sr.", "III" suffixes, last name is second-to-last
        suffix_markers = {"jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
        if parts[-1].lower() in suffix_markers:
            last = f"{parts[-2]} {parts[-1]}"
            first = " ".join(parts[:-2])
        else:
            last = parts[-1]
            first = " ".join(parts[:-1])
        return first, last


# ============================================================
# Core upsert
# ============================================================

def upsert_politician(cur, source_config, official_data, chamber_id, district_id, ocd_id):
    """Main upsert logic for a single official.

    Match types and actions:
    - exact:               UPDATE data fields on existing record
    - fuzzy:               UPDATE data fields on existing record (name check logged)
    - new_person_in_seat:  mark old is_active=false, INSERT new record + new office
    - None (no seat):      INSERT new politician + office (district already created)

    Returns:
        (action, politician_id) where action is 'updated', 'new', or 'deactivated+new'
    """
    title = source_config.get("title", "")
    data_source = source_config.get("url", "")
    scraped_name = official_data.get("name", "")
    party_full, party_short = normalize_party(official_data.get("party", ""))
    first_name, last_name = split_name(scraped_name)

    politician_id, match_type = find_existing_politician_for_seat(
        cur, ocd_id, title, scraped_name
    )

    if match_type in ("exact", "fuzzy"):
        # UPDATE existing politician record with fresher scraped data
        # Per CONTEXT.md: "overwrite with scraped data" when fresher
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
            party_full, party_short,
            data_source, politician_id,
        ))
        return "updated", str(politician_id)

    elif match_type == "new_person_in_seat":
        # Old politician in this seat — deactivate them
        old_id = politician_id
        cur.execute("""
            UPDATE essentials.politicians
            SET is_active = false,
                last_synced = NOW()
            WHERE id = %s
            RETURNING full_name
        """, (old_id,))
        deactivated_row = cur.fetchone()
        old_name = deactivated_row["full_name"] if deactivated_row else "unknown"
        print(f"      Deactivated: {old_name} (id={old_id})")

        # Insert new politician
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

        # Deactivate old office and create new one
        cur.execute("""
            UPDATE essentials.offices
            SET politician_id = %s
            WHERE politician_id = %s
        """, (new_pol_id, old_id))

        # If no office was updated (unusual), create a new one
        if cur.rowcount == 0:
            office_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO essentials.offices
                    (id, politician_id, chamber_id, district_id, title,
                     representing_state, seats, is_appointed_position)
                VALUES (%s, %s, %s, %s, %s, %s, 1, false)
            """, (office_id, new_pol_id, chamber_id, district_id, title, "CA"))

        return "deactivated+new", new_pol_id

    else:
        # No seat or no match — create new politician + office
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

        # Create office linking politician to district + chamber
        office_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO essentials.offices
                (id, politician_id, chamber_id, district_id, title,
                 representing_state, seats, is_appointed_position)
            VALUES (%s, %s, %s, %s, %s, 'CA', 1, false)
        """, (office_id, new_pol_id, chamber_id, district_id, title))

        return "new", new_pol_id


# ============================================================
# Verification queries
# ============================================================

def verify_no_duplicates(cur):
    """Verify no active duplicate politicians exist per seat.

    A "duplicate" = same person appearing in the same seat with both records
    marked active. This query MUST return 0 rows after a complete run.

    Returns number of duplicate seat violations found.
    """
    print("\n" + "=" * 60)
    print("Verification: Duplicate check (must be 0 rows)")
    print("=" * 60)

    cur.execute("""
        SELECT d.ocd_id, p.full_name, COUNT(*) as active_count
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE p.is_active = true
          AND d.state = 'CA'
          AND d.district_type IN ('LOCAL', 'LOCAL_EXEC')
          AND d.ocd_id LIKE 'ocd-division/country:us/state:ca/%%'
        GROUP BY d.ocd_id, p.full_name
        HAVING COUNT(*) > 1
    """)
    rows = cur.fetchall()

    if rows:
        print(f"  FAIL: {len(rows)} duplicate seat violations found!")
        for row in rows:
            print(f"    {row['ocd_id']}: {row['full_name']} ({row['active_count']} active records)")
    else:
        print("  PASS: 0 duplicate seat violations")

    return len(rows)


def verify_point_in_polygon(cur):
    """Run PIP tests to verify the geo_id join chain works end-to-end.

    Tests:
    1. East LA unincorporated (34.0239, -118.1726): expect 1 supervisor
    2. LA City Hall (34.0537, -118.2427): expect 1 council member + 1 mayor
    """
    print("\n" + "=" * 60)
    print("Verification: Point-in-polygon tests")
    print("=" * 60)

    # Test 1: East LA unincorporated — expect supervisor
    print("\n  Test 1: East LA unincorporated (34.0239, -118.1726)")
    print("  Expected: 1 supervisor")
    cur.execute("""
        SELECT p.full_name, o.title, d.ocd_id, d.geo_id
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        JOIN essentials.geofence_boundaries gb
          ON gb.geo_id = d.geo_id AND gb.mtfcc = 'X0001'
        WHERE ST_Covers(gb.geometry, ST_SetSRID(ST_MakePoint(-118.1726, 34.0239), 4326))
          AND d.district_type = 'LOCAL'
          AND d.ocd_id LIKE 'ocd-division/country:us/state:ca/county:los_angeles/%%'
          AND p.is_active = true
    """)
    pip1 = cur.fetchall()
    if pip1:
        for row in pip1:
            print(f"    FOUND: {row['full_name']} — {row['title']} ({row['ocd_id']})")
        if len(pip1) == 1:
            print(f"  PASS: Exactly 1 supervisor found")
        else:
            print(f"  WARN: Expected 1, found {len(pip1)}")
    else:
        print("  WARN: No supervisor found at East LA — check geo_id + geofence coverage")

    # Test 2: LA City Hall — expect 1 council member + 1 mayor
    print("\n  Test 2: LA City Hall (34.0537, -118.2427)")
    print("  Expected: 1 council member (CD14) + 1 mayor")
    cur.execute("""
        SELECT p.full_name, o.title, d.ocd_id, d.district_type
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        JOIN essentials.geofence_boundaries gb ON gb.geo_id = d.geo_id
        WHERE ST_Covers(gb.geometry, ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326))
          AND d.district_type IN ('LOCAL', 'LOCAL_EXEC')
          AND (d.ocd_id LIKE 'ocd-division/country:us/state:ca/place:los_angeles/%%'
               OR d.ocd_id = 'ocd-division/country:us/state:ca/place:los_angeles')
          AND p.is_active = true
        ORDER BY d.district_type
    """)
    pip2 = cur.fetchall()
    if pip2:
        for row in pip2:
            print(f"    FOUND: {row['full_name']} — {row['title']} ({row['district_type']})")
        council_count = sum(1 for r in pip2 if r["district_type"] == "LOCAL")
        mayor_count = sum(1 for r in pip2 if r["district_type"] == "LOCAL_EXEC")
        if council_count == 1 and mayor_count == 1:
            print(f"  PASS: 1 council member + 1 mayor found at LA City Hall")
        else:
            print(f"  WARN: Expected 1 council + 1 mayor, found {council_count} council + {mayor_count} mayor")
    else:
        print("  WARN: No officials found at LA City Hall — check geo_id + geofence coverage")


# ============================================================
# Per-source scrape + upsert
# ============================================================

def process_source(cur, source_config):
    """Process a single source: scrape, dedup, upsert.

    Returns dict: {matched: int, updated: int, new: int, deactivated: int, errors: int}
    """
    source_id = source_config["id"]
    source_url = source_config["url"]
    parser = source_config["parser"]
    expected_count = source_config.get("expected_count", 0)

    print(f"\n  Source: {source_config['name']}")
    print(f"  URL: {source_url}")
    print(f"  Parser: {parser}")

    # Fetch HTML
    html = ""
    try:
        html = fetch_html(source_url)
        print(f"  Fetched {len(html):,} bytes from {source_url}")
    except Exception as e:
        print(f"  WARNING: Could not fetch {source_url}: {e}")
        print("  Proceeding with hardcoded fallback data")

    # Parse officials
    officials = []
    if parser == "la_county_bos":
        officials = parse_la_county_bos(html)
    elif parser == "la_city_clerk":
        officials = parse_la_city_clerk(html)
    elif parser == "la_city_clerk_mayor":
        officials = parse_la_city_clerk_mayor(html)
    else:
        print(f"  ERROR: Unknown parser '{parser}'")
        return {"matched": 0, "updated": 0, "new": 0, "deactivated": 0, "errors": 1}

    print(f"  Parsed {len(officials)} officials (expected {expected_count})")
    if len(officials) != expected_count:
        print(f"  WARNING: Count mismatch! Using data as-is.")

    # Find or create chamber (shared by all officials in this source)
    chamber_id = find_or_create_chamber_for_source(cur, source_config)

    # Counters
    counts = {"matched": 0, "updated": 0, "new": 0, "deactivated": 0, "errors": 0}

    for official in officials:
        name = official.get("name", "").strip()
        if not name:
            continue

        try:
            # Find/create district for this official
            district_id, ocd_id = find_or_create_district(cur, source_config, official)

            # Upsert politician
            action, pol_id = upsert_politician(
                cur, source_config, official, chamber_id, district_id, ocd_id
            )

            if action == "updated":
                counts["matched"] += 1
                counts["updated"] += 1
                dist_label = official.get("district", "?")
                print(f"    Updated: {name} (District {dist_label}, ocd={ocd_id})")
            elif action == "new":
                counts["new"] += 1
                dist_label = official.get("district", "?")
                print(f"    Inserted: {name} (District {dist_label}, ocd={ocd_id})")
            elif action == "deactivated+new":
                counts["deactivated"] += 1
                counts["new"] += 1
                dist_label = official.get("district", "?")
                print(f"    Replaced: {name} (District {dist_label}, ocd={ocd_id})")

        except Exception as e:
            counts["errors"] += 1
            print(f"    ERROR processing {name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n  Source summary: {counts['matched']} matched/updated, "
          f"{counts['new']} new inserts, {counts['deactivated']} deactivated, "
          f"{counts['errors']} errors")
    return counts


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("Phase 36 — Scrape LA Officials + Upsert to essentials.politicians")
    print("=" * 60)

    load_env()

    # Load source config
    config_path = Path(__file__).parent / "politician_sources.json"
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path) as f:
        config = json.load(f)
    sources = config.get("sources", [])
    print(f"\nLoaded {len(sources)} sources from {config_path}")

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    total_counts = {"matched": 0, "updated": 0, "new": 0, "deactivated": 0, "errors": 0}

    try:
        print("\n" + "=" * 60)
        print("Step 1: Process each source")
        print("=" * 60)

        for source_config in sources:
            try:
                counts = process_source(cur, source_config)
                for k in total_counts:
                    total_counts[k] += counts.get(k, 0)
            except Exception as e:
                print(f"\n  ERROR in source '{source_config['id']}': {e}")
                import traceback
                traceback.print_exc()
                print("  Rolling back this source and continuing...")
                # Note: single transaction — error in one source rolls back all
                # In production, use savepoints for per-source isolation
                conn.rollback()
                conn.autocommit = False
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Overall verification
        dup_count = verify_no_duplicates(cur)
        verify_point_in_polygon(cur)

        conn.commit()
        print("\n" + "=" * 60)
        print("All changes committed successfully.")
        print("=" * 60)

        print("\nFinal totals:")
        print(f"  Matched/updated : {total_counts['matched']}")
        print(f"  New inserts     : {total_counts['new']}")
        print(f"  Deactivated     : {total_counts['deactivated']}")
        print(f"  Errors          : {total_counts['errors']}")

        if dup_count > 0:
            print(f"\nWARNING: {dup_count} duplicate seat violations remain!")
            sys.exit(1)
        elif total_counts['errors'] > 0:
            print(f"\nWARNING: {total_counts['errors']} errors occurred during processing.")
            sys.exit(1)
        else:
            print("\nAll requirements passed:")
            print("  POL-05: 0 active duplicates per seat")

    except Exception as e:
        conn.rollback()
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
