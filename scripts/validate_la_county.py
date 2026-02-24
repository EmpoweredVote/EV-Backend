#!/usr/bin/env python3
"""
LA County validation script — Phase 38 final pipeline gate.

Performs three checks in sequence:
  1. VACUUM ANALYZE essentials.geofence_boundaries (VAL-02)
  2. GiST index verification via EXPLAIN ANALYZE (VAL-03)
  3. Point-in-polygon tier checks for 16 representative addresses (VAL-01 + VAL-04)

Usage:
    cd EV-Backend/scripts
    python3 validate_la_county.py

Exit codes:
    0 — all addresses pass
    1 — one or more addresses fail (requires investigation)
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from utils import load_env

import psycopg2
import psycopg2.extras
from urllib.parse import urlparse


# ============================================================
# Database connection
# ============================================================

def get_connection():
    """Open a psycopg2 connection using DATABASE_URL.

    Mirrors the pattern used in scrape_la_officials.py.
    Uses direct connection (port 5432) — pooler port 6543 breaks VACUUM.
    """
    raw_url = os.getenv("DATABASE_URL")
    if not raw_url:
        print("Error: DATABASE_URL not set. Call load_env() first.")
        sys.exit(1)
    parsed = urlparse(raw_url)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )


# ============================================================
# Test address definitions
# (label, lat, lng, expected_tiers, notes)
#
# expected_tiers: set of tier names that MUST be present.
# Extra tiers (e.g. "judicial") are OK — superset check.
#
# Federal tier = NATIONAL_LOWER only (Congressional district from G5200).
# NATIONAL_UPPER (US Senate) and NATIONAL_EXEC (President/VP) are
# returned by fetchStatewideFromDB in production, not from geofences.
# Do NOT include "federal" for US Senate / President in expected_tiers.
#
# Known acceptable gaps — do NOT mark as failures:
#   - Santa Clarita: gap city (no ArcGIS council boundary found, Phase 35)
#   - Unincorporated areas: no city tier is correct behaviour
#   - LAUSD addresses: returning all 7 board members is correct (Phase 37-02)
# ============================================================

TEST_ADDRESSES = [
    # ---- Required VAL-01 addresses (3) ----
    (
        "Pasadena City Hall (incorporated)",
        34.1478, -118.1445,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "6-tier: mid-size city with wards (Supervisor D5 Barger)",
    ),
    (
        "East LA (unincorporated)",
        34.0239, -118.1726,
        {"federal", "state_senate", "state_assembly", "county", "school"},
        "5-tier: no city — unincorporated (Supervisor D1 Solis)",
    ),
    (
        "Pasadena/Arcadia boundary edge",
        34.1161, -118.1003,
        {"federal", "state_senate", "state_assembly"},
        "Boundary edge: at least federal+state — must not return empty",
    ),
    # ---- Additional VAL-04 addresses (13) ----
    (
        "LA City Hall (Downtown)",
        34.0537, -118.2427,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "LA City council + mayor + LAUSD (7 board members expected)",
    ),
    (
        "Long Beach City Hall",
        33.7701, -118.1937,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "Long Beach council wards (imported Phase 35)",
    ),
    (
        "Glendale City Hall",
        34.1425, -118.2551,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "Glendale USD school board check",
    ),
    (
        "Compton City Hall",
        33.8958, -118.2201,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "South county: Supervisor D2 (Mitchell)",
    ),
    (
        "Lancaster City Hall",
        34.6987, -118.1365,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "North county: Supervisor D5 (Barger)",
    ),
    (
        "Malibu City Hall",
        34.0319, -118.6896,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "West county: small city",
    ),
    (
        "Inglewood City Hall",
        33.9617, -118.3531,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "Inglewood (geometry_dissolved — Phase 35)",
    ),
    (
        "Torrance City Hall",
        33.8361, -118.3406,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "Torrance council wards (imported Phase 35)",
    ),
    (
        "West Covina City Hall",
        34.0686, -117.9390,
        {"federal", "state_senate", "state_assembly", "county", "city", "school"},
        "East county: West Covina council wards (Phase 35)",
    ),
    (
        "Willowbrook (unincorporated)",
        33.9158, -118.2226,
        {"federal", "state_senate", "state_assembly", "county", "school"},
        "Unincorporated: no city tier expected",
    ),
    (
        "Santa Clarita City Hall",
        34.3917, -118.5426,
        {"federal", "state_senate", "state_assembly", "county", "school"},
        "Gap city: no council boundaries (Phase 35) — city tier absent expected",
    ),
    (
        "Altadena (unincorporated)",
        34.1900, -118.1320,
        {"federal", "state_senate", "state_assembly", "county", "school"},
        "Unincorporated near Pasadena — no city tier expected",
    ),
    (
        "Marina del Rey (unincorporated)",
        33.9804, -118.4517,
        {"federal", "state_senate", "state_assembly", "county", "school"},
        "Unincorporated coastal — no city tier expected",
    ),
]


# ============================================================
# Tier classification
#
# Maps (district_type, ocd_id, office_title) -> tier name.
#
# KEY NUANCE — LA County supervisors (Phase 35-01 decision):
#   district_type = 'LOCAL', mtfcc = 'X0001'
#   They are NOT classified as COUNTY via district_type.
#   Detect them by ocd_id LIKE '%county:los_angeles/council_district%'
#   OR by office_title LIKE '%Supervisor%'.
# ============================================================

def classify_tier(district_type: str, ocd_id: str, office_title: str) -> str:
    """Return a tier name for the given district metadata.

    Returns one of: federal, state_senate, state_assembly, state,
                    county, city, school, judicial, unknown
    """
    dt = district_type or ""
    ocd = ocd_id or ""
    title = office_title or ""

    if dt in ("NATIONAL_LOWER", "NATIONAL_UPPER", "NATIONAL_EXEC"):
        return "federal"
    if dt == "STATE_UPPER":
        return "state_senate"
    if dt == "STATE_LOWER":
        return "state_assembly"
    if dt == "STATE_EXEC":
        return "state"
    if dt == "COUNTY":
        return "county"
    if dt == "SCHOOL":
        return "school"
    if dt == "JUDICIAL":
        return "judicial"
    if dt == "LOCAL_EXEC":
        # Mayor / county exec
        if "county:los_angeles" in ocd and "council_district" in ocd:
            return "county"
        return "city"
    if dt == "LOCAL":
        # LA County supervisors have district_type=LOCAL (Phase 35-01 decision).
        # Detect by ocd_id pattern or office title.
        if "county:los_angeles/council_district" in ocd:
            return "county"
        if "Supervisor" in title:
            return "county"
        return "city"
    return "unknown"


# ============================================================
# PIP query (mirrors geofence_lookup.go FindGeoIDsByPoint)
# ============================================================

PIP_QUERY = """
    SELECT DISTINCT
        p.full_name,
        p.party,
        o.title AS office_title,
        d.district_type,
        d.geo_id,
        gb.mtfcc,
        d.ocd_id
    FROM essentials.politicians p
    JOIN essentials.offices o ON o.politician_id = p.id
    JOIN essentials.districts d ON o.district_id = d.id
    JOIN essentials.geofence_boundaries gb ON gb.geo_id = d.geo_id
    WHERE ST_Covers(
        gb.geometry,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    )
      AND p.is_active = true
    ORDER BY d.district_type, p.full_name
"""


def check_tiers(cur, lat: float, lng: float) -> dict:
    """Run PIP query and return tier -> [(full_name, office_title, party), ...].

    Note: PostGIS ST_MakePoint takes (x=lng, y=lat) — NOT (lat, lng).
    """
    cur.execute(PIP_QUERY, (lng, lat))
    rows = cur.fetchall()

    tiers: dict = {}
    for row in rows:
        full_name, party, office_title, district_type, geo_id, mtfcc, ocd_id = row
        tier = classify_tier(district_type, ocd_id or "", office_title or "")
        if tier not in tiers:
            tiers[tier] = []
        tiers[tier].append((full_name, office_title, party))

    return tiers


# ============================================================
# Step 1 — VACUUM ANALYZE (VAL-02)
# ============================================================

def run_vacuum_analyze(conn) -> None:
    """Run VACUUM ANALYZE on geofence_boundaries.

    CRITICAL: Must use autocommit=True — VACUUM cannot run inside a transaction.
    (RESEARCH.md Pitfall 1)
    """
    print("\n" + "=" * 60)
    print("STEP 1: VACUUM ANALYZE (VAL-02)")
    print("=" * 60)

    conn.autocommit = True
    cur = conn.cursor()
    try:
        print("Running VACUUM ANALYZE essentials.geofence_boundaries ...")
        t0 = time.time()
        cur.execute("VACUUM ANALYZE essentials.geofence_boundaries;")
        elapsed = time.time() - t0
        print(f"VACUUM ANALYZE complete in {elapsed:.1f}s")
    finally:
        conn.autocommit = False
        cur.close()


# ============================================================
# Step 2 — GiST index verification (VAL-03)
# ============================================================

def verify_gist_index(conn) -> bool:
    """Check that a GiST index exists and EXPLAIN ANALYZE does not show Seq Scan.

    Returns True if index check passes, False if Seq Scan detected.
    """
    print("\n" + "=" * 60)
    print("STEP 2: GiST INDEX VERIFICATION (VAL-03)")
    print("=" * 60)

    cur = conn.cursor()

    # Check pg_indexes for existing GiST index
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'geofence_boundaries'
          AND schemaname = 'essentials'
          AND indexdef ILIKE '%gist%'
    """)
    idx_rows = cur.fetchall()

    if not idx_rows:
        print("WARNING: No GiST index found on essentials.geofence_boundaries.geometry")
        print("Creating GiST index now ...")
        conn.autocommit = True
        cur.execute("CREATE INDEX ON essentials.geofence_boundaries USING gist (geometry);")
        conn.autocommit = False
        print("GiST index created. Re-running VACUUM ANALYZE to update statistics ...")
        conn.autocommit = True
        cur.execute("VACUUM ANALYZE essentials.geofence_boundaries;")
        conn.autocommit = False
        print("VACUUM ANALYZE complete.")
        # Re-query index list
        cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'geofence_boundaries'
              AND schemaname = 'essentials'
              AND indexdef ILIKE '%gist%'
        """)
        idx_rows = cur.fetchall()

    print(f"Found {len(idx_rows)} GiST index(es):")
    for name, defn in idx_rows:
        print(f"  {name}: {defn}")

    # Run EXPLAIN ANALYZE using LA City Hall coordinates
    print("\nRunning EXPLAIN ANALYZE with LA City Hall coordinates (-118.2427, 34.0537) ...")
    cur.execute("""
        EXPLAIN ANALYZE
        SELECT geo_id, mtfcc
        FROM essentials.geofence_boundaries
        WHERE ST_Covers(
            geometry,
            ST_SetSRID(ST_MakePoint(-118.2427, 34.0537), 4326)
        )
    """)
    plan_rows = cur.fetchall()
    plan_text = "\n".join(row[0] for row in plan_rows)

    print("\nEXPLAIN ANALYZE output:")
    print(plan_text)

    if "Seq Scan on geofence_boundaries" in plan_text:
        print("\nFAIL: Seq Scan detected on geofence_boundaries — GiST index not used.")
        print("      This may indicate stale statistics. VACUUM ANALYZE should have fixed this.")
        cur.close()
        return False
    else:
        print("\nPASS: No Seq Scan on geofence_boundaries (GiST index confirmed).")
        cur.close()
        return True


# ============================================================
# Step 3 — Per-address PIP validation (VAL-01 + VAL-04)
# ============================================================

def validate_addresses(conn) -> list:
    """Run PIP checks for all TEST_ADDRESSES.

    Returns list of result dicts for summary report.
    """
    print("\n" + "=" * 60)
    print("STEP 3: ADDRESS VALIDATION (VAL-01 + VAL-04)")
    print("=" * 60)

    cur = conn.cursor()
    results = []

    for idx, (label, lat, lng, expected_tiers, notes) in enumerate(TEST_ADDRESSES, 1):
        print(f"\n[{idx:02d}/{len(TEST_ADDRESSES)}] {label}")
        print(f"      Coords: ({lat}, {lng})")
        print(f"      Notes:  {notes}")

        found_tiers = check_tiers(cur, lat, lng)
        found_tier_names = set(found_tiers.keys()) - {"unknown", "judicial"}

        # Print officials grouped by tier
        if found_tiers:
            for tier in sorted(found_tiers.keys()):
                officials = found_tiers[tier]
                print(f"      [{tier.upper()}]")
                for name, title, party in officials:
                    party_str = f" ({party})" if party else ""
                    print(f"        - {name}{party_str} | {title}")
        else:
            print("      (no officials found via geofence)")

        # Evaluate pass/fail
        missing_tiers = expected_tiers - found_tier_names
        extra_tiers = found_tier_names - expected_tiers

        if missing_tiers:
            status = "FAIL"
            detail = f"Missing: {', '.join(sorted(missing_tiers))}"
        else:
            status = "PASS"
            detail = f"{len(expected_tiers)}/{len(expected_tiers)} required tiers found"

        if extra_tiers and extra_tiers != {"judicial"}:
            print(f"      INFO: Extra tiers found (OK): {', '.join(sorted(extra_tiers))}")

        # Per-tier PASS/FAIL output
        print(f"\n      Tier results:")
        all_tiers = sorted(expected_tiers | found_tier_names - {"judicial"})
        for tier in all_tiers:
            if tier == "judicial":
                continue
            is_expected = tier in expected_tiers
            is_found = tier in found_tier_names
            if is_expected and is_found:
                tier_status = "PASS"
            elif is_expected and not is_found:
                tier_status = "FAIL (expected but missing)"
            elif not is_expected and is_found:
                tier_status = "INFO (found but not required)"
            else:
                tier_status = "N/A"
            if tier_status != "N/A":
                print(f"        {tier:<16} {tier_status}")

        print(f"\n      RESULT: {status} — {detail}")

        results.append({
            "label": label,
            "lat": lat,
            "lng": lng,
            "expected_tiers": expected_tiers,
            "found_tiers": found_tier_names,
            "missing_tiers": missing_tiers,
            "status": status,
            "notes": notes,
        })

    cur.close()
    return results


# ============================================================
# Step 4 — Summary report
# ============================================================

def print_summary(results: list, index_pass: bool) -> bool:
    """Print final summary table.

    Returns True if all checks pass, False otherwise.
    """
    print("\n" + "=" * 70)
    print("=== VALIDATION SUMMARY ===")
    print("=" * 70)

    # GiST index row
    gist_label = "GiST Index Check (EXPLAIN ANALYZE)"
    gist_status = "PASS" if index_pass else "FAIL"
    print(f"{'Check':<45} {'Result':<6}  {'Notes'}")
    print("-" * 70)
    print(f"{'VACUUM ANALYZE complete':<45} {'PASS':<6}  VAL-02")
    print(f"{gist_label:<45} {gist_status:<6}  VAL-03")
    print("-" * 70)

    passed = 0
    failed = 0
    for r in results:
        found_count = len(r["found_tiers"])
        exp_count = len(r["expected_tiers"])
        tier_summary = f"{found_count} tiers found"
        if r["missing_tiers"]:
            missing_str = ", ".join(sorted(r["missing_tiers"]))
            tier_summary += f" | Missing: {missing_str}"
        label = r["label"][:44]
        print(f"{label:<45} {r['status']:<6}  {tier_summary}")
        if r["status"] == "PASS":
            passed += 1
        else:
            failed += 1

    total = len(results)
    print("-" * 70)

    overall_ok = failed == 0 and index_pass
    overall_label = "PASS" if overall_ok else "FAIL"

    print(f"\nOVERALL: {passed}/{total} addresses PASS | GiST: {'PASS' if index_pass else 'FAIL'} | OVERALL: {overall_label}")

    if failed > 0:
        print(f"\nAddresses needing investigation ({failed}):")
        for r in results:
            if r["status"] == "FAIL":
                missing = ", ".join(sorted(r["missing_tiers"]))
                print(f"  - {r['label']}: missing tiers = {missing}")

    print("=" * 70)
    return overall_ok


# ============================================================
# Main entry point
# ============================================================

def main():
    print("=" * 60)
    print("LA County Validation Script — Phase 38")
    print(f"Addresses to test: {len(TEST_ADDRESSES)}")
    print("=" * 60)

    load_env()
    conn = get_connection()

    try:
        # Step 1: VACUUM ANALYZE
        run_vacuum_analyze(conn)

        # Step 2: GiST index verification
        index_pass = verify_gist_index(conn)

        # Step 3: Address validation
        results = validate_addresses(conn)

    finally:
        conn.close()

    # Step 4: Summary report
    overall_ok = print_summary(results, index_pass)

    if overall_ok:
        print("\nValidation complete. All checks passed.")
        sys.exit(0)
    else:
        print("\nValidation complete. One or more checks FAILED — see summary above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
