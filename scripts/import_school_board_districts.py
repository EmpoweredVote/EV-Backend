#!/usr/bin/env python3
"""
Import school board sub-district boundaries from Monroe County GIS ArcGIS service.

These polygons enable precise point-in-polygon matching for school board members,
replacing the prefix-matching workaround that returned all 7 members for any address.

Source: Monroe County GIS (MoCo-GIS)
  Feature Service: MCCSC_School_Board_2024
  URL: https://services1.arcgis.com/nYfGJ9xFTKW6VPqW/arcgis/rest/services/MCCSC_School_Board_2024/FeatureServer

Requires: pip install geopandas psycopg2-binary sqlalchemy shapely
Usage: DATABASE_URL="postgresql://..." python3 import_school_board_districts.py
  Or: automatically reads from ../.env.local
"""

import os
import sys
import json
import geopandas as gpd
from shapely.geometry import shape
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

# Map ArcGIS district numbers to BallotReady geo_ids in essentials.districts
DISTRICT_GEO_IDS = {
    1: "180063000001",  # MCCSC District 1
    2: "180063000002",  # MCCSC District 2
    3: "180063000003",  # MCCSC District 3
    4: "180063000004",  # MCCSC District 4
    5: "180063000005",  # MCCSC District 5
    6: "180063000006",  # MCCSC District 6
    7: "180063000007",  # MCCSC District 7
    0: "1809480",       # RBBCSC (Richland-Bean Blossom) - all seats share this geo_id
}

GEOJSON_PATH = Path(__file__).parent / "mccsc_school_board_districts.geojson"
ARCGIS_URL = (
    "https://services1.arcgis.com/nYfGJ9xFTKW6VPqW/arcgis/rest/services/"
    "MCCSC_School_Board_2024/FeatureServer/1/query"
    "?where=1%3D1&outFields=District,DistrictName,Board_Members"
    "&f=geojson&outSR=4326"
)


def load_env():
    """Load DATABASE_URL from .env.local if not already set"""
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
    """Create SQLAlchemy engine, URL-encoding the password if needed"""
    from urllib.parse import urlparse, quote_plus, urlunparse
    raw_url = os.getenv("DATABASE_URL")
    parsed = urlparse(raw_url)
    if parsed.password:
        encoded_pw = quote_plus(parsed.password)
        netloc = f"{parsed.username}:{encoded_pw}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        safe_url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))
    else:
        safe_url = raw_url
    return create_engine(safe_url)


def download_geojson():
    """Download GeoJSON from ArcGIS if not already cached locally"""
    if GEOJSON_PATH.exists():
        print(f"  Using cached GeoJSON: {GEOJSON_PATH.name}")
        return

    import requests
    print(f"  Downloading from ArcGIS feature service...")
    resp = requests.get(ARCGIS_URL)
    resp.raise_for_status()
    with open(GEOJSON_PATH, 'w') as f:
        f.write(resp.text)
    print(f"  Downloaded: {GEOJSON_PATH.name}")


def import_districts(engine):
    """Import school board district polygons into geofence_boundaries"""
    with open(GEOJSON_PATH) as f:
        data = json.load(f)

    features = data["features"]
    print(f"  Found {len(features)} features in GeoJSON")

    rows = []
    for feat in features:
        props = feat["properties"]
        district_num = props["District"]
        geo_id = DISTRICT_GEO_IDS.get(district_num)

        if geo_id is None:
            print(f"  WARNING: No geo_id mapping for district {district_num}, skipping")
            continue

        geom = shape(feat["geometry"])
        name = props["DistrictName"]

        rows.append({
            "geo_id": geo_id,
            "name": name,
            "mtfcc": "G5420",
            "state": "18",
            "geometry": geom,
            "source": "moco_gis_arcgis_2024",
            "imported_at": datetime.now(),
        })
        print(f"  District {district_num} ({name}) -> geo_id {geo_id}")

    gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

    # Delete existing sub-district boundaries (if re-running)
    with engine.connect() as conn:
        geo_ids = [r["geo_id"] for r in rows]
        result = conn.execute(
            text("""
                DELETE FROM essentials.geofence_boundaries
                WHERE geo_id = ANY(:geo_ids) AND mtfcc = 'G5420'
            """),
            {"geo_ids": geo_ids}
        )
        if result.rowcount > 0:
            print(f"  Deleted {result.rowcount} existing records (re-import)")
        conn.commit()

    # Import new boundaries
    gdf.to_postgis(
        "geofence_boundaries",
        engine,
        schema="essentials",
        if_exists="append",
        index=False,
    )
    print(f"  Imported {len(gdf)} school board district boundaries")
    return len(gdf)


def verify(engine):
    """Verify import by testing point-in-polygon for downtown Bloomington"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT geo_id, name, mtfcc
            FROM essentials.geofence_boundaries
            WHERE mtfcc = 'G5420'
              AND ST_Contains(
                  geometry,
                  ST_SetSRID(ST_MakePoint(-86.5342, 39.1699), 4326)
              )
            ORDER BY geo_id
        """))
        rows = result.fetchall()

    print(f"\n  Point-in-polygon test (downtown Bloomington 39.1699, -86.5342):")
    print(f"  Found {len(rows)} G5420 boundaries:")
    for row in rows:
        print(f"    geo_id={row[0]}  name={row[1]}  mtfcc={row[2]}")

    # Check: should have the parent Census district + exactly 1 sub-district
    sub_districts = [r for r in rows if len(r[0]) > 7]
    if len(sub_districts) == 1:
        print(f"  SUCCESS: Point falls in exactly 1 sub-district: {sub_districts[0][0]}")
    elif len(sub_districts) == 0:
        print(f"  WARNING: Point doesn't fall in any sub-district")
    else:
        print(f"  WARNING: Point falls in {len(sub_districts)} sub-districts (expected 1)")


def main():
    print("=" * 60)
    print("Import School Board Sub-District Boundaries")
    print("Source: Monroe County GIS (MCCSC_School_Board_2024)")
    print("=" * 60)

    load_env()
    engine = get_engine()

    download_geojson()
    count = import_districts(engine)
    verify(engine)

    print(f"\nDone! Imported {count} school board district boundaries.")
    print("You can now remove the G5420 prefix matching in geofence_lookup.go")


if __name__ == "__main__":
    main()
