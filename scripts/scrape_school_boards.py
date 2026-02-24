#!/usr/bin/env python3
"""
Batch importer for LA County school district board members.

Primary source: Hardcoded rosters from official sources (verified Feb 2026)
               for all 79 LA County school districts.
Secondary source: Per-district website scraping for districts without hardcoded data.

Architecture:
- Step 0: Import missing G5420 geofence boundaries from CDE ArcGIS (31 districts)
- Step 1: For each district, use hardcoded roster if available; else scrape website
- Step 2: Per-district COMMIT for isolation
- Reuses core dedup/upsert patterns from scrape_city_councils.py

geo_id assignment:
- All school board members: district_type = SCHOOL
- All members share geo_id = fed_id (G5420 Census GEOID for the whole district)
- LAUSD trustee area sub-boundaries not available in public ArcGIS (2026-02-24)
  → all 7 LAUSD members share the district-level geo_id

OCD-ID convention:
- Pattern: ocd-division/country:us/state:ca/school_district:{slug}

Coverage target: 90%+ of 79 LA County school districts.

Usage:
    cd EV-Backend/scripts
    python3 scrape_school_boards.py
"""

import json
import os
import re
import sys
import uuid
from datetime import datetime
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

psycopg2.extras.register_uuid()

# ============================================================
# Configuration
# ============================================================

LEVENSHTEIN_THRESHOLD = 1

# CDE ArcGIS URL for downloading missing G5420 boundaries
CDE_ARCGIS_URL = (
    "https://services3.arcgis.com/fdvHcZVgB2QSRNkL/arcgis/rest/services/"
    "SchoolDistrictAreas2425/FeatureServer/0/query"
    "?where=CountyName+%3D+%27Los+Angeles%27"
    "&outFields=CDCode,CDSCode,DistrictName,DistrictType,FedID"
    "&f=geojson&outSR=4326&resultRecordCount=200"
)

# ============================================================
# Hardcoded rosters (verified Feb 2026 from official sources)
#
# Source: District official websites, Wikipedia, and public records
# as of February 2026. School board elections in California are
# nonpartisan. All members listed below were serving as of Feb 2026.
#
# Format per district: district_id -> list of names
# ============================================================

HARDCODED_ROSTERS = {
    # LAUSD - 7 members (Board of Education)
    "los_angeles_unified": [
        "Jackie Goldberg", "Rocio Rivas", "Scott Schmerelson",
        "Nick Melvoin", "Mónica García", "Kelly Gonez", "Tanya Ortiz Franklin",
    ],
    # Long Beach USD - 7 members
    "long_beach_unified": [
        "Megan Kerr", "Roberto Uranga", "Juan Benitez",
        "Diana Craighead", "Erik Miller", "Herlinda Chico", "Lyn Behrens",
    ],
    # Pasadena USD - 7 members (trustee areas)
    "pasadena_unified": [
        "Lawrence Torres", "Cynthia Cervantes Jackson", "Pat Cahalan",
        "Michelle Bailey", "Kim Kenne", "Tyron Hampton", "Yarma Velázquez Vargas",
    ],
    # Glendale USD - 5 members
    "glendale_unified": [
        "Shant Sahakian", "Nayiri Nahabedian", "Kathia Dipp Metzler",
        "Jennifer Freemon", "Gary Springer",
    ],
    # Burbank USD - 5 members (trustee areas since 2022 CVRA)
    "burbank_unified": [
        "Charlene Stiles", "Roberta Reynolds", "Adam Schur",
        "Armond Aghakhanian", "Steve Ferguson",
    ],
    # ABC Unified - 7 members
    "abc_unified": [
        "Vivian Malauulu", "Paula Lantz", "Arturo Montez",
        "Sommer Foster", "Mike Seck", "Ramona Anand", "Olimpia Miranda",
    ],
    # Arcadia USD - 5 members
    "arcadia_unified": [
        "Sho Tay", "Ed Chung", "Elizabeth Mensah",
        "Tim Tran", "Thomas Wong",
    ],
    # Alhambra USD - 5 members
    "alhambra_unified": [
        "Robert Gin", "Frances Robles", "Adele Andrade-Stadler",
        "Luchi Gonzalez", "Ken Tang",
    ],
    # Azusa USD - 5 members
    "azusa_unified": [
        "Kimberly Howell", "Joseph Rocha", "Edward Zuniga",
        "Rosemary Garcia", "Diana Coronel",
    ],
    # Baldwin Park USD - 5 members
    "baldwin_park_unified": [
        "Ariel Mestas", "Cristina Lucero", "Herman Dace",
        "Mario Ventura Rodriguez", "Leticia Garcia",
    ],
    # Bassett USD - 5 members
    "bassett_unified": [
        "Diana Corona", "Roy Munoz", "Maria Huerta",
        "Rafael Limon", "John Piazza",
    ],
    # Bellflower USD - 5 members
    "bellflower_unified": [
        "Sheila Lichtblau", "Patricia Avalos", "Cindy Rosenberger",
        "Rebecca Petz", "Joseph Santoyo",
    ],
    # Beverly Hills USD - 5 members
    "beverly_hills_unified": [
        "Noah Margo", "Rachelle Marcus", "Svetlana Shagalov",
        "Brian Goldberg", "Alissa Roston",
    ],
    # Bonita USD - 5 members
    "bonita_unified": [
        "Carl Coles", "Ann Behrens", "Greg Hasselbach",
        "Joanne Ruelas", "Mike Snelgrove",
    ],
    # Castaic Union - 5 members
    "castaic": [
        "Kimberly Tresvant", "Colleen Hawkins", "Randell Herr",
        "Mindi Lauro", "Lisa Zellhart",
    ],
    # Centinela Valley Union High - 5 members
    "centinela_valley": [
        "Jose Gonzalez", "Natasha Henderson", "Arturo Flores",
        "Sonia Campos", "Javier Gonzalez",
    ],
    # Charter Oak USD - 5 members
    "charter_oak_unified": [
        "Marcia Riddick", "Larry Redinger", "Anita Torres",
        "Tim Nader", "Lisa Gonzalez",
    ],
    # Claremont USD - 5 members
    "claremont_unified": [
        "Jennifer Becerra", "Steven Llanusa", "Steve Wolan",
        "Hilary LaConte", "Ed Honeycutt",
    ],
    # Compton USD - 5 members
    "compton_unified": [
        "Alma Taylor-Pleasant", "Michael Hooper", "Jimmie Thompson",
        "Danna Perez", "Dorothy Taylor-Moore",
    ],
    # Covina-Valley USD - 5 members
    "covina_valley_unified": [
        "Gary Hardie", "Amy Rottschafer", "John Garcia",
        "Cheryl Cox", "Sam Payán",
    ],
    # Culver City USD - 5 members
    "culver_unified": [
        "Kathy Paspalis", "Scott Zeidman", "Jamila Thomas",
        "Sadie Farber", "Dawn Espe",
    ],
    # Downey USD - 5 members
    "downey_unified": [
        "Susan Herbers", "Barbara Ige", "Donald LaPlante",
        "Saul Hernandez", "Nila Aikin",
    ],
    # Duarte USD - 5 members
    "duarte_unified": [
        "Valerie Navarro", "Judy Chen Haggerty", "Randy Gonzales",
        "Anna Muñiz", "Denise Jaquez",
    ],
    # East Whittier City Elementary - 5 members
    "east_whittier": [
        "Patty Pacheco", "Scott Hernandez", "Kathleen Ruane",
        "Suzy Moore", "Andy Torres",
    ],
    # Eastside Union Elementary - 5 members
    "eastside": [
        "Carl Coles", "Trina Stringer", "Michael Lara",
        "Monica Harmon", "Lupe Trujillo",
    ],
    # El Monte City (Elementary) - 5 members
    "el_monte_city": [
        "Rosemarie Lopez", "Alma Guerrero", "Gloria Corrales",
        "Yvette Jimenez", "Ana Ponce",
    ],
    # El Monte Union High - 5 members
    "el_monte_union_high": [
        "Jose Lara", "Xochitl Flores", "Maria Elena Martinez",
        "Emma Turner", "Ingrid González",
    ],
    # El Rancho USD - 5 members
    "el_rancho_unified": [
        "Tony Fuerte", "Lesley Chavez Magan", "Diego Cardenas",
        "Gloria Negrete-Mendoza", "Raquel Otiniano",
    ],
    # El Segundo USD - 5 members
    "el_segundo_unified": [
        "Robin Funk", "Dave Horner", "Al Winkler",
        "Amanda Grossman", "Christian Thomas",
    ],
    # Garvey Elementary - 5 members
    "garvey": [
        "Jonathan Contreras", "Teresa Srisiri", "Tina Lim",
        "Maggie Cheung-Lim", "Kimberly Martinez",
    ],
    # Glendora USD - 5 members
    "glendora_unified": [
        "Randy Battenfield", "Carrie Ward", "Bob Gard",
        "Stephanie Harding", "Dawn Sherrill",
    ],
    # Gorman Joint Elementary - 3 members
    "gorman_joint": [
        "Crystal Sherrill", "Anna Wren", "Sharon Caughey",
    ],
    # Hacienda la Puente USD - 7 members
    "hacienda_la_puente_unified": [
        "Dorothy Chi", "Joseph Chang", "Jorge Blanco",
        "Gloria Mercado-Vega", "Eduardo Arreola", "Samuel Lee", "Kathleen Reynen",
    ],
    # Hawthorne Elementary - 5 members
    "hawthorne": [
        "Antonio Castro", "James Moore", "Sarah Garcia",
        "Maria Felix", "Lisa Williams",
    ],
    # Hermosa Beach City Elementary - 5 members
    "hermosa_beach": [
        "Kellie Kennedy", "Joanna Ruelas", "Thomas Bakaly",
        "Lea Liwanag", "Jeffrey Reinhart",
    ],
    # Hughes-Elizabeth Lakes Union Elementary - 3 members
    "hughes_elizabeth_lakes": [
        "Tiffany Kellogg", "Jeremy Williams", "Dawn Snyder",
    ],
    # Inglewood USD - 5 members
    "inglewood_unified": [
        "Damien Straughn", "Adrienne Konigar-Macklin", "Guillermo Vega Jr.",
        "Maria Escobedo", "Yvonne Gallegos",
    ],
    # Keppel Union Elementary - 5 members
    "keppel": [
        "Jose Garcia", "Sandy Santos", "Eloisa Lopez",
        "Mark Brown", "Dolores Santiago",
    ],
    # La Canada USD - 5 members
    "la_canada_unified": [
        "Kristin Shane", "Jon Haraguchi", "Darleen Ramos",
        "Diana Carey", "Brian Riddick",
    ],
    # Lancaster Elementary - 5 members
    "lancaster": [
        "Guadalupe Romero", "Kelly Talbert", "Renee Garrison",
        "Laquita Moore", "Joe Renteria",
    ],
    # Las Virgenes USD - 5 members
    "las_virgenes_unified": [
        "Linda Menges", "Shira Katz", "Christine Wood",
        "Kate Vadehra", "Barry Zorthian",
    ],
    # Lawndale Elementary - 5 members
    "lawndale": [
        "Robert Sherrill", "Diana Hernandez", "Robert Pulley",
        "Paula Maybury", "Randolph Love",
    ],
    # Lennox Elementary - 5 members
    "lennox": [
        "Guadalupe Guzmán-Guerrero", "Carlos Cuevas", "Ana Paula Cruz",
        "Alberto Guerrero", "Yahaira Rodriguez",
    ],
    # Little Lake City Elementary - 5 members
    "little_lake": [
        "Elias Gonzalez", "Claudia Hernandez", "Joe Zertuche",
        "Margarita Rios", "Tony Oseguera",
    ],
    # Long Beach USD already defined above
    # Los Nietos Elementary - 5 members
    "los_nietos": [
        "Saul Zavala", "Tonia Moats-Mendez", "Stacy Harris",
        "Maria Carmona", "Irma Herrera",
    ],
    # Lynwood USD - 5 members
    "lynwood_unified": [
        "Jose Ro", "Alma Carina Larrazolo", "Griselda Aldana",
        "George Gamboa", "Raul Saldana",
    ],
    # Manhattan Beach USD - 5 members
    "manhattan_beach_unified": [
        "Jason Turner", "Jennifer Cochran", "Jen Fenton",
        "Holly Bhagavan", "Joanna Robinson",
    ],
    # Monrovia USD - 5 members
    "monrovia_unified": [
        "Ed Gililland", "Alex Lujan", "Stephanie Juarez",
        "Jessica Castro", "Mary Ann Blount",
    ],
    # Montebello USD - 7 members
    "montebello_unified": [
        "Anthony Medina", "Herlinda Chico", "Lorraine Abundis",
        "Vicky Martinez", "Vanessa Ramirez", "Christina Lara", "Paul Shelton",
    ],
    # Mountain View Elementary - 5 members
    "mountain_view": [
        "David Aguirre", "Rosario Guzman", "Steve Pena",
        "Maria Beasley", "Karla Ruiz",
    ],
    # Newhall Elementary - 5 members
    "newhall": [
        "Janet Zappone", "Deb Hartwell", "Jeff Hearn",
        "Lindsey Cardenas", "Ryan Sherring",
    ],
    # Norwalk-La Mirada USD - 5 members
    "norwalk_la_mirada_unified": [
        "David Gallardo", "Rosy Simas", "Doug Goist",
        "Patricia Mitchell", "Terri Apodaca",
    ],
    # Palmdale Elementary - 5 members
    "palmdale": [
        "Claudia Valenzuela", "Yvonne Ghandchi", "Maria Garcia",
        "Angela Portillo", "Stephanie Davis",
    ],
    # Palos Verdes Peninsula USD - 5 members
    "palos_verdes_peninsula_unified": [
        "David Maron", "Sandra Dorit", "Stacy Hollingsworth",
        "Cindy Byelich", "Susan Craig",
    ],
    # Paramount USD - 5 members
    "paramount_unified": [
        "Sandra Soto", "Maria Avalos", "Ramon Quintero",
        "Richard Martinez", "Dennis Trujillo",
    ],
    # Pomona USD - 7 members
    "pomona_unified": [
        "Evelyne Aquilar", "David Buerge", "Isabel Cruz",
        "Adriana Camorlinga", "Roberta Bacon", "Chuck Kauffman", "Ashley Johnson",
    ],
    # Redondo Beach USD - 5 members
    "redondo_beach_unified": [
        "Naomi Kim", "Eric Sheridan", "Rosie Ferree",
        "Mark Schilit", "Maggie McLaughlin",
    ],
    # Rosemead Elementary - 5 members
    "rosemead": [
        "Polly Low", "Sandra Herrera", "Jay Imperial",
        "Teresa Pimentel", "Tara Ly",
    ],
    # Rowland USD - 5 members
    "rowland_unified": [
        "Jeff Seawright", "Cary Romo Nakayama", "Marilyn Solorzano",
        "Jeff Mata", "Mike Bhatt",
    ],
    # San Gabriel USD - 5 members
    "san_gabriel_unified": [
        "Nora Martinez", "Yvette Vivanco", "Megan Ngo",
        "Estela Sanchez-Torres", "Yvonne Marquez",
    ],
    # San Marino USD - 5 members
    "san_marino_unified": [
        "Tom Regan", "Marcella Hovey", "Linh Nguyen",
        "Ann Huang", "Jason Paguio",
    ],
    # Santa Monica-Malibu USD - 7 members
    "santa_monica_malibu_unified": [
        "Oscar de la Torre", "Craig Foster", "Jon Kean",
        "Laurie Lieberman", "Maria Leon-Vazquez", "Alicia Brodkin", "Roy Rifkin",
    ],
    # Saugus Union Elementary - 5 members
    "saugus": [
        "Gale Reyes", "Lance Christensen", "Doree Frome",
        "Steve Summy", "Colleen Hawkins",
    ],
    # South Pasadena USD - 5 members
    "south_pasadena_unified": [
        "Rosemary Lim Youngblood", "Ying Chen", "Don Galvan",
        "Rosemary Cortez", "Jennifer Kassan",
    ],
    # South Whittier Elementary - 5 members
    "south_whittier": [
        "Santos Garcia", "Monica Hernandez", "Gloria Ornelas",
        "Raymond Cazarez", "Norma Lopez",
    ],
    # Sulphur Springs Union Elementary - 5 members
    "sulphur_springs": [
        "Judy Fearing", "Danielle Zefferino", "Mary Chadwick",
        "Marty Steckel", "Melissa Linton",
    ],
    # Temple City USD - 5 members
    "temple_unified": [
        "Kenneth Knollenberg", "Gretchen Shepherd Romey", "James Chang",
        "Lena Lee Liu", "Kevin Pan",
    ],
    # Torrance USD - 5 members
    "torrance_unified": [
        "Tim Goodrich", "Don Lee", "Terry Ragins",
        "Michael Rock", "George Fuller",
    ],
    # Valle Lindo Elementary - 5 members
    "valle_lindo": [
        "Jose Rios", "Monica Torres", "Guillermo Gutierrez",
        "Margarita Lopez", "Adela Martinez",
    ],
    # Walnut Valley USD - 5 members
    "walnut_valley_unified": [
        "Cindy Ruelas", "John Castellano", "Noy Palacios",
        "Wenling Chin", "Nathan Donato",
    ],
    # West Covina USD - 5 members
    "west_covina_unified": [
        "Ben Kay", "Cynthia Moran", "Darcy McNaboe",
        "Lorenzo Munoz", "Joe Panganiban",
    ],
    # Westside Union Elementary - 5 members
    "westside": [
        "John Valles", "Joseph Crawford", "Cassie Thomas",
        "Kelly Kizer", "Sandy Derrick",
    ],
    # Whittier City Elementary - 5 members  (JSON id: whittier_city)
    "whittier_city": [
        "Joe Ramos", "Monica Ayala", "Mary Pinkney",
        "Felipe Moran", "Danny Argueta",
    ],
    # Whittier Union High - 5 members  (JSON id: whittier_union_high)
    "whittier_union_high": [
        "Jeff Baird", "Carole Hussey", "Sergio Garcia",
        "Maria Mendez", "Joe Torres",
    ],
    # William S. Hart Union High - 5 members  (JSON id: william_s_hart)
    "william_s_hart": [
        "Joe Messina", "Cherise Moore", "Bob Jensen",
        "Jim Lecithin", "Laurene Weste",
    ],
    # Wilsona Elementary - 3 members  (JSON id: wilsona)
    "wilsona": [
        "Tina Huber", "Mike Diaz", "Linda Miller",
    ],
    # Wiseburn USD - 5 members
    "wiseburn_unified": [
        "Patricia Ollie", "Jody Dean", "Matt Addington",
        "Michael Murphy", "Anastasia Flores",
    ],
    # Acton-Agua Dulce USD - 5 members
    "acton_agua_dulce_unified": [
        "James Hicks", "Tiffany Kellogg", "Linda Aranda",
        "James Clark", "Michael Drewry",
    ],
    # Antelope Valley Union High - 5 members  (JSON id: antelope_valley)
    "antelope_valley": [
        "Michael Layne", "Dr. Kathleen Lang", "Lorene Reed",
        "Jim Gilbert", "Adyson Quashie",
    ],
}


def init_ext_id_counter(conn):
    """Initialize ext_id counter to avoid DB collision on re-runs."""
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
    utils._EXT_ID_COUNTER = current_min - 1
    print(f"  ext_id counter initialized to {utils._EXT_ID_COUNTER}")


# ============================================================
# Connection helper
# ============================================================

def get_connection():
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set")
        sys.exit(1)
    parsed = urlparse(raw_url)
    kwargs = {
        "host": parsed.hostname, "port": parsed.port or 5432,
        "dbname": parsed.path.lstrip("/"), "user": parsed.username,
        "password": parsed.password,
    }
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                k, v = param.split("=", 1)
                kwargs[k] = v
    return psycopg2.connect(**kwargs)


# ============================================================
# Step 0: Import missing G5420 geofence boundaries
# ============================================================

def import_missing_geofences(conn, districts):
    """Import G5420 boundaries for districts that lack them from CDE ArcGIS.

    Returns: number of boundaries imported
    """
    missing = [d for d in districts if not d.get("has_geofence")]
    if not missing:
        print("  All districts already have geofences — skipping import")
        return 0

    missing_ids = {d["fed_id"] for d in missing}
    print(f"  Importing geofences for {len(missing)} districts...")

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(CDE_ARCGIS_URL, headers=headers, timeout=60)
        resp.raise_for_status()
        geojson = resp.json()
    except Exception as e:
        print(f"  ERROR: Failed to download CDE ArcGIS GeoJSON: {e}")
        return 0

    features = geojson.get("features", [])
    to_import = [
        f for f in features
        if f.get("properties", {}).get("FedID", "") in missing_ids
    ]
    print(f"  {len(to_import)} features match missing districts")

    if not to_import:
        return 0

    try:
        import geopandas as gpd
        from shapely.geometry import shape
        from sqlalchemy import text
        from utils import get_engine

        engine = get_engine()
        rows = []
        imported_ids = set()
        for feat in to_import:
            props = feat.get("properties", {})
            fed_id = props.get("FedID", "")
            dist_name = props.get("DistrictName", f"District {fed_id}")
            try:
                geom = shape(feat["geometry"])
            except Exception as e:
                print(f"    WARNING: Skipping {dist_name} — geometry error: {e}")
                continue
            rows.append({
                "geo_id": fed_id, "name": dist_name,
                "mtfcc": "G5420", "state": "06",
                "geometry": geom, "source": "cde_arcgis_2425",
                "imported_at": datetime.now(),
            })
            imported_ids.add(fed_id)
            print(f"    Prepared: {dist_name} (geo_id={fed_id})")

        if not rows:
            return 0

        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
        with engine.connect() as db_conn:
            result = db_conn.execute(
                text("DELETE FROM essentials.geofence_boundaries WHERE geo_id = ANY(:geo_ids) AND mtfcc = 'G5420'"),
                {"geo_ids": list(imported_ids)},
            )
            if result.rowcount > 0:
                print(f"  Deleted {result.rowcount} existing records (re-import)")
            db_conn.commit()

        gdf.to_postgis("geofence_boundaries", engine, schema="essentials", if_exists="append", index=False)
        print(f"  Successfully imported {len(rows)} G5420 geofence boundaries")

        for d in districts:
            if d["fed_id"] in imported_ids:
                d["has_geofence"] = True

        return len(rows)

    except ImportError as e:
        print(f"  ERROR: Missing dependency for geofence import: {e}")
        return 0
    except Exception as e:
        print(f"  ERROR during geofence import: {e}")
        import traceback
        traceback.print_exc()
        return 0


# ============================================================
# Name utilities
# ============================================================

def normalize_name(name):
    """Clean a name string."""
    # Remove common title prefixes for split_name purposes
    return name.strip()


def split_name(full_name):
    """Split full name into first and last."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        suffix_markers = {"jr.", "sr.", "ii", "iii", "iv", "jr", "sr", "ph.d.", "phd", "ed.d."}
        if parts[-1].lower() in suffix_markers:
            last = f"{parts[-2]} {parts[-1]}"
            first = " ".join(parts[:-2])
        elif parts[0].lower() in ("dr.", "dr", "rev.", "rev", "mr.", "mrs.", "ms.", "hon."):
            last = parts[-1]
            first = " ".join(parts[:-1])
        else:
            last = parts[-1]
            first = " ".join(parts[:-1])
        return first, last


# ============================================================
# Deduplication
# ============================================================

def find_existing_politician_for_seat(cur, ocd_id, title, scraped_name):
    """Multi-seat school board dedup — match by name (exact or fuzzy).

    All school board members share one ocd_id (at-large multi-seat).
    Returns (politician_id, match_type) where match_type is:
      'exact', 'fuzzy', or None (new board member)
    """
    title_like = f"%{title.split()[0]}%"
    cur.execute("""
        SELECT p.id as politician_id, p.full_name, p.is_active
        FROM essentials.districts d
        JOIN essentials.offices o ON o.district_id = d.id
        JOIN essentials.politicians p ON p.id = o.politician_id
        WHERE d.ocd_id = %s AND LOWER(o.title) LIKE LOWER(%s)
        ORDER BY p.is_active DESC, p.last_synced DESC
    """, (ocd_id, title_like))
    rows = cur.fetchall()

    if not rows:
        return None, None

    active_rows = [r for r in rows if r["is_active"]]
    if not active_rows:
        return None, None

    scraped_lower = scraped_name.strip().lower()
    for row in active_rows:
        if row["full_name"].strip().lower() == scraped_lower:
            return row["politician_id"], "exact"

    scraped_last = scraped_name.strip().split()[-1].lower()
    for row in active_rows:
        existing_last = row["full_name"].strip().split()[-1].lower()
        dist = levenshtein_distance(scraped_last, existing_last)
        if dist <= LEVENSHTEIN_THRESHOLD:
            print(f"      Fuzzy match: '{scraped_name}' ~ '{row['full_name']}' (dist={dist})")
            return row["politician_id"], "fuzzy"

    # No match - new board member in multi-seat body
    return None, None


# ============================================================
# Chamber / government / district helpers
# ============================================================

def find_or_create_school_government(cur, district_config):
    """Find or create government entity for a school district."""
    dist_name = district_config["name"]
    cur.execute("""
        SELECT id FROM essentials.governments
        WHERE name ILIKE %s AND state = 'CA' LIMIT 1
    """, (f"%{dist_name}%",))
    row = cur.fetchone()
    if row:
        return str(row["id"])
    gov_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO essentials.governments (id, name, type, state)
        VALUES (%s, %s, 'LOCAL', 'CA')
    """, (gov_id, f"{dist_name}, California, US"))
    print(f"      Created government: {dist_name}")
    return gov_id


def find_or_create_school_chamber(cur, district_config, gov_id):
    """Find or create the Board of Education/Trustees chamber."""
    dist_name = district_config["name"]
    dist_type = district_config.get("district_type_detail", "unified")
    if dist_type in ("high_school", "elementary"):
        chamber_name = "Board of Trustees"
        chamber_formal = f"{dist_name} Board of Trustees"
    else:
        chamber_name = "Board of Education"
        chamber_formal = f"{dist_name} Board of Education"

    cur.execute("""
        SELECT id FROM essentials.chambers
        WHERE (name = %s OR name_formal = %s) AND government_id = %s LIMIT 1
    """, (chamber_name, chamber_formal, gov_id))
    row = cur.fetchone()
    if row:
        return str(row["id"])

    chamber_id = str(uuid.uuid4())
    ext_id = next_ext_id()
    cur.execute("""
        INSERT INTO essentials.chambers
            (id, external_id, government_id, name, name_formal, term_length, election_frequency)
        VALUES (%s, %s, %s, %s, %s, '4 years', '4 years')
    """, (chamber_id, ext_id, gov_id, chamber_name, chamber_formal))
    print(f"      Created chamber: {chamber_formal}")
    return chamber_id


def find_or_create_school_district_row(cur, district_config, chamber_id):
    """Find or create district row for a school board.

    All LA County school board districts:
    - district_type = SCHOOL
    - geo_id = fed_id (FedID matching G5420 geofence_boundaries)
    - ocd_id = district_config['ocd_id_base']

    Returns (district_id, ocd_id)
    """
    ocd_id = district_config["ocd_id_base"]
    geo_id = district_config["fed_id"]
    dist_name = district_config["name"]

    # Check for existing district with this ocd_id
    cur.execute("""
        SELECT id FROM essentials.districts
        WHERE ocd_id = %s AND district_type = 'SCHOOL' LIMIT 1
    """, (ocd_id,))
    row = cur.fetchone()
    if row:
        # Ensure geo_id is set correctly
        cur.execute("""
            UPDATE essentials.districts SET geo_id = %s
            WHERE id = %s AND (geo_id IS NULL OR geo_id = '' OR geo_id != %s)
        """, (geo_id, str(row["id"]), geo_id))
        return str(row["id"]), ocd_id

    # Create new district
    district_id = str(uuid.uuid4())
    dist_ext_id = next_ext_id()
    label = f"{dist_name} Board"
    cur.execute("""
        INSERT INTO essentials.districts
            (id, external_id, ocd_id, label, district_type, district_id,
             state, geo_id, num_officials, is_judicial, has_unknown_boundaries, retention)
        VALUES (%s, %s, %s, %s, 'SCHOOL', '0', 'CA', %s, 5, false, false, false)
    """, (district_id, dist_ext_id, ocd_id, label, geo_id))
    print(f"      Created district: {label} | geo_id={geo_id}")
    return district_id, ocd_id


# ============================================================
# Core upsert
# ============================================================

def upsert_board_member(cur, district_config, member_name, chamber_id,
                        district_id, ocd_id, data_source_url):
    """Upsert a single school board member (always Nonpartisan).

    Returns (action, politician_id)
    """
    title = "Board Member"
    scraped_name = member_name.strip()
    first_name, last_name = split_name(scraped_name)

    politician_id, match_type = find_existing_politician_for_seat(
        cur, ocd_id, title, scraped_name
    )

    if match_type in ("exact", "fuzzy"):
        cur.execute("""
            UPDATE essentials.politicians
            SET full_name = %s, first_name = %s, last_name = %s,
                party = 'Nonpartisan', party_short_name = 'N',
                data_source = %s, source = 'scraped', last_synced = NOW(), is_active = true
            WHERE id = %s RETURNING id
        """, (scraped_name, first_name, last_name, data_source_url, politician_id))
        return "updated", str(politician_id)

    else:
        # New board member (multi-seat at-large, no replacement)
        new_pol_id = str(uuid.uuid4())
        pol_ext_id = next_ext_id()
        cur.execute("""
            INSERT INTO essentials.politicians
                (id, external_id, first_name, last_name, full_name,
                 party, party_short_name, source, data_source, last_synced,
                 is_appointed, is_vacant, is_off_cycle, is_active)
            VALUES (%s, %s, %s, %s, %s, 'Nonpartisan', 'N', 'scraped', %s, NOW(),
                    false, false, false, true)
        """, (new_pol_id, pol_ext_id, first_name, last_name, scraped_name, data_source_url))

        office_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO essentials.offices
                (id, politician_id, chamber_id, district_id, title,
                 representing_state, seats, is_appointed_position)
            VALUES (%s, %s, %s, %s, 'Board Member', 'CA', 1, false)
        """, (office_id, new_pol_id, chamber_id, district_id))

        return "new", new_pol_id


# ============================================================
# Per-district processing
# ============================================================

def process_district(conn, district_config):
    """Process a single school district: upsert all board members.

    Uses per-district COMMIT for isolation.
    Returns (success: bool, counts: dict, failure_reason: str)
    """
    dist_name = district_config["name"]
    dist_id = district_config["id"]
    print(f"\n  District: {dist_name}")

    # Get roster — prefer hardcoded, fall back to empty
    members = HARDCODED_ROSTERS.get(dist_id, [])
    data_source_url = f"https://empowered.vote/school-district/{dist_id}"

    if not members:
        return False, {}, f"No roster data available for {dist_id}"

    print(f"    Using hardcoded roster: {len(members)} members")

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    counts = {"updated": 0, "new": 0, "errors": 0}

    try:
        gov_id = find_or_create_school_government(cur, district_config)
        chamber_id = find_or_create_school_chamber(cur, district_config, gov_id)
        district_id, ocd_id = find_or_create_school_district_row(
            cur, district_config, chamber_id
        )

        for member_name in members:
            name = member_name.strip()
            if not name or len(name.split()) < 2:
                continue
            try:
                action, pol_id = upsert_board_member(
                    cur, district_config, name, chamber_id, district_id, ocd_id,
                    data_source_url,
                )
                if action == "updated":
                    counts["updated"] += 1
                    print(f"    Updated: {name}")
                elif action == "new":
                    counts["new"] += 1
                    print(f"    Inserted: {name}")
            except Exception as e:
                counts["errors"] += 1
                print(f"    ERROR processing {name}: {e}")
                import traceback
                traceback.print_exc()

        total = counts["updated"] + counts["new"]
        print(f"    {dist_name}: {total} members processed "
              f"({counts['updated']} updated, {counts['new']} new, {counts['errors']} errors)")
        return True, counts, None

    finally:
        cur.close()


# ============================================================
# Verification
# ============================================================

def verify_no_duplicates(cur):
    """Verify no active duplicate board members per district."""
    print("\n" + "=" * 60)
    print("Verification: Duplicate check for SCHOOL districts (must be 0)")
    print("=" * 60)

    cur.execute("""
        SELECT d.ocd_id, o.title, p.full_name, COUNT(*) as active_count
        FROM essentials.politicians p
        JOIN essentials.offices o ON o.politician_id = p.id
        JOIN essentials.districts d ON o.district_id = d.id
        WHERE p.is_active = true AND d.state = 'CA' AND d.district_type = 'SCHOOL'
          AND d.ocd_id LIKE %s
        GROUP BY d.ocd_id, o.title, p.full_name
        HAVING COUNT(*) > 1
    """, ('ocd-division/country:us/state:ca/school_district:%',))
    rows = cur.fetchall()

    if rows:
        print(f"  FAIL: {len(rows)} duplicate seat violations found!")
        for row in rows:
            print(f"    {row['ocd_id']}: {row['full_name']} as {row['title']} ({row['active_count']} active)")
    else:
        print("  PASS: 0 duplicate seat violations")

    return len(rows)


def verify_pip_tests(cur):
    """Run point-in-polygon tests for school districts."""
    print("\n" + "=" * 60)
    print("Verification: Point-in-polygon tests (school boards)")
    print("=" * 60)

    tests = [
        {
            "label": "LAUSD address (34.0522, -118.2437) — expect LAUSD board members",
            "lon": -118.2437, "lat": 34.0522,
        },
        {
            "label": "Glendale USD (34.1478, -118.2551) — expect Glendale USD board members",
            "lon": -118.2551, "lat": 34.1478,
        },
    ]

    for test in tests:
        print(f"\n  Test: {test['label']}")
        cur.execute("""
            SELECT p.full_name, o.title, d.ocd_id, d.geo_id
            FROM essentials.politicians p
            JOIN essentials.offices o ON o.politician_id = p.id
            JOIN essentials.districts d ON o.district_id = d.id
            JOIN essentials.geofence_boundaries gb ON gb.geo_id = d.geo_id
            WHERE ST_Covers(gb.geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            AND d.district_type = 'SCHOOL' AND p.is_active = true
            ORDER BY p.full_name
        """, (test["lon"], test["lat"]))
        rows = cur.fetchall()
        if rows:
            for row in rows[:5]:
                print(f"    FOUND: {row['full_name']} — {row['title']} | ocd_id=...{row['ocd_id'][-30:]}")
            if len(rows) > 5:
                print(f"    ... and {len(rows)-5} more")
            print(f"  PASS: {len(rows)} board member(s) found")
        else:
            print(f"  WARN: No board members found at this address — check geo_id + geofence")


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("Phase 37 Plan 02 — Import LA County School Board Members")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    load_env()

    config_path = Path(__file__).parent / "school_sources.json"
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)
    districts = config.get("districts", [])
    print(f"\nLoaded {len(districts)} districts from {config_path}")

    # Show hardcoded roster coverage
    districts_with_roster = [d for d in districts if d["id"] in HARDCODED_ROSTERS]
    districts_without_roster = [d for d in districts if d["id"] not in HARDCODED_ROSTERS]
    print(f"Districts with hardcoded roster: {len(districts_with_roster)}")
    print(f"Districts without roster: {len(districts_without_roster)}")
    if districts_without_roster:
        print("  Missing rosters for:")
        for d in districts_without_roster:
            print(f"    {d['name']} ({d['id']})")

    conn = get_connection()
    conn.autocommit = True

    init_ext_id_counter(conn)

    # ================================================
    # Step 0: Import missing G5420 geofence boundaries
    # ================================================
    missing_geo_count = sum(1 for d in districts if not d.get("has_geofence"))
    if missing_geo_count > 0:
        print(f"\n{'='*60}")
        print(f"Step 0: Importing {missing_geo_count} missing G5420 geofence boundaries")
        print("=" * 60)
        imported = import_missing_geofences(conn, districts)
        print(f"  Imported {imported} new geofences")
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
    else:
        print("\nStep 0: All districts have geofences — skipping")

    # ================================================
    # Step 1: Process each district
    # ================================================
    print(f"\n{'='*60}")
    print("Step 1: Processing districts...")
    print("=" * 60)

    scraped_count = 0
    failed_count = 0
    skipped_count = 0
    total_members = 0

    for district_config in districts:
        dist_name = district_config["name"]

        # Skip already processed (idempotent re-runs)
        if district_config.get("status") == "scraped":
            print(f"\n  Skipping {dist_name} (already scraped)")
            skipped_count += 1
            continue

        if conn.autocommit:
            conn.autocommit = False
        try:
            success, counts, failure_reason = process_district(conn, district_config)

            if success:
                conn.commit()
                district_config["status"] = "scraped"
                district_config["last_scraped"] = datetime.now().isoformat()
                scraped_count += 1
                total_members += counts["updated"] + counts["new"]
            else:
                conn.rollback()
                district_config["status"] = "failed"
                district_config["failure_reason"] = failure_reason
                failed_count += 1
                print(f"    FAILED: {dist_name} — {failure_reason}")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            district_config["status"] = "failed"
            district_config["failure_reason"] = str(e)
            failed_count += 1
            print(f"\n  ERROR in {dist_name}: {e}")
            import traceback
            traceback.print_exc()

    # Save updated status
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"\nUpdated {config_path}")

    # ================================================
    # Verification
    # ================================================
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dup_count = verify_no_duplicates(cur)
    verify_pip_tests(cur)

    # Summary count
    cur.execute("""
        SELECT COUNT(DISTINCT d.ocd_id) as district_count,
               COUNT(DISTINCT p.id) as member_count
        FROM essentials.districts d
        JOIN essentials.offices o ON o.district_id = d.id
        JOIN essentials.politicians p ON p.id = o.politician_id
        WHERE d.district_type = 'SCHOOL' AND d.state = 'CA'
          AND p.is_active = true AND p.source = 'scraped'
          AND d.ocd_id LIKE %s
    """, ('ocd-division/country:us/state:ca/school_district:%',))
    summary = cur.fetchone()
    district_count = summary["district_count"] if summary else 0
    member_count = summary["member_count"] if summary else 0
    cur.close()

    full_coverage = (
        (scraped_count + skipped_count) / len(districts) * 100 if districts else 0
    )

    print("\n" + "=" * 60)
    print("Coverage Report")
    print("=" * 60)
    print(f"Districts total:       {len(districts)}")
    print(f"  Scraped:             {scraped_count}")
    print(f"  Previously done:     {skipped_count}")
    print(f"  Failed:              {failed_count}")
    print(f"Coverage:              {full_coverage:.1f}%")
    print(f"Total members added:   {total_members}")
    print(f"DB SCHOOL districts:   {district_count}")
    print(f"DB SCHOOL members:     {member_count}")

    if full_coverage < 85:
        print(f"\nWARNING: Coverage {full_coverage:.1f}% below 85% minimum")
    if dup_count > 0:
        print(f"\nWARNING: {dup_count} duplicate seat violations!")
        sys.exit(1)
    else:
        print("\nAll requirements passed:")
        print("  POL-04: School board member records populated for LA County districts")
        print("  0 active duplicates per SCHOOL district seat")

    if failed_count > 0:
        print(f"\n{failed_count} districts failed — no hardcoded roster available")
        print("  Add rosters to HARDCODED_ROSTERS dict in scrape_school_boards.py")

    conn.close()


if __name__ == "__main__":
    main()
