#!/usr/bin/env python3
"""
Quick-8: Import Monroe County Council district boundaries into geofence_boundaries.
Source: Monroe County Election Map ArcGIS FeatureServer (layer 13)

Run: DATABASE_URL=... python3 import_monroe_council_districts.py
"""

import os
import sys
import json
import psycopg2
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("Error: DATABASE_URL not set")
    sys.exit(1)

GEOJSON_URL = (
    "https://services1.arcgis.com/nYfGJ9xFTKW6VPqW/arcgis/rest/services/"
    "Monroe_County_Election_Map_Current_WFL1/FeatureServer/13/query"
    "?where=1%3D1&outFields=CountyCouncil,Council,Rep&f=geojson&outSR=4326"
)

# Map district number -> geo_id (matching essentials.districts)
DISTRICT_GEO_IDS = {
    "1": "1810500001",
    "2": "1810500002",
    "3": "1810500003",
    "4": "1810500004",
}

SOURCE = "monroe_county_arcgis_council_districts_2022"

def fetch_districts():
    import urllib.request
    with urllib.request.urlopen(GEOJSON_URL, timeout=30) as resp:
        return json.loads(resp.read())

def import_districts(geojson):
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    imported = 0
    skipped = 0

    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        district_num = str(props.get("CountyCouncil", "")).strip()
        rep_name = props.get("Rep", "")
        geometry = json.dumps(feature.get("geometry", {}))

        geo_id = DISTRICT_GEO_IDS.get(district_num)
        if not geo_id:
            print(f"  SKIP: unknown district number '{district_num}'")
            skipped += 1
            continue

        name = f"Monroe County Council District {district_num}"
        print(f"  Importing: {name} (geo_id={geo_id}, rep={rep_name})...")

        cur.execute("""
            INSERT INTO essentials.geofence_boundaries
                (geo_id, name, state, mtfcc, geometry, source, imported_at)
            VALUES (
                %s, %s, 'IN', 'X0001',
                ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
                %s, NOW()
            )
            ON CONFLICT (geo_id, mtfcc) DO UPDATE SET
                name = EXCLUDED.name,
                geometry = EXCLUDED.geometry,
                source = EXCLUDED.source,
                imported_at = NOW()
        """, (geo_id, name, geometry, SOURCE))

        print(f"    OK: {cur.statusmessage}")
        imported += 1

    conn.commit()
    cur.close()
    conn.close()
    return imported, skipped

def verify():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT geo_id, name, mtfcc, source
        FROM essentials.geofence_boundaries
        WHERE geo_id IN ('1810500001','1810500002','1810500003','1810500004')
        ORDER BY geo_id
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    print("=" * 60)
    print("Monroe County Council District Boundary Importer")
    print(f"Source: {SOURCE}")
    print("=" * 60)

    print("\nFetching district boundaries from ArcGIS...")
    geojson = fetch_districts()
    print(f"  Got {len(geojson.get('features', []))} features")

    print("\nImporting to essentials.geofence_boundaries...")
    imported, skipped = import_districts(geojson)

    print(f"\nImport complete: {imported} imported, {skipped} skipped")

    print("\nVerifying...")
    rows = verify()
    for row in rows:
        print(f"  geo_id={row[0]}, name={row[1]}, mtfcc={row[2]}, source={row[3]}")

    if len(rows) == 4:
        print("\nSUCCESS: All 4 district boundaries are present in DB.")
    else:
        print(f"\nWARNING: Expected 4, found {len(rows)} rows.")
        sys.exit(1)
