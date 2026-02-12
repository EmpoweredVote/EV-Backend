#!/usr/bin/env python3
"""
Bulk Import LA County Politicians

This script triggers the bulk import API to populate politician data for all LA County ZIP codes.
"""

import os
import sys
import json
import requests
import time

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:5050")
DELAY_MS = int(os.getenv("DELAY_MS", "3000"))  # 3 seconds between imports
SESSION_COOKIE = os.getenv("SESSION_COOKIE", "")

# Comprehensive list of LA County ZIP codes
LA_COUNTY_ZIPS = [
    # Central/Downtown LA (900xx)
    "90001", "90002", "90003", "90004", "90005", "90006", "90007", "90008", "90009", "90010",
    "90011", "90012", "90013", "90014", "90015", "90016", "90017", "90018", "90019", "90020",
    "90021", "90022", "90023", "90024", "90025", "90026", "90027", "90028", "90029", "90030",
    "90031", "90032", "90033", "90034", "90035", "90036", "90037", "90038", "90039", "90040",
    "90041", "90042", "90043", "90044", "90045", "90046", "90047", "90048", "90049", "90050",
    "90051", "90052", "90053", "90054", "90055", "90056", "90057", "90058", "90059", "90060",
    "90061", "90062", "90063", "90064", "90065", "90066", "90067", "90068", "90069", "90070",
    "90071", "90072", "90073", "90074", "90075", "90076", "90077", "90078", "90079", "90080",
    "90081", "90082", "90083", "90084", "90086", "90087", "90088", "90089",

    # West LA/Santa Monica/Culver City (902xx)
    "90201", "90202", "90209", "90210", "90211", "90212", "90213", "90220", "90221", "90222",
    "90230", "90231", "90232", "90233", "90239", "90240", "90241", "90242", "90245", "90247",
    "90248", "90249", "90250", "90251", "90254", "90255", "90260", "90261", "90262", "90263",
    "90264", "90265", "90266", "90267", "90270", "90272", "90274", "90275", "90277", "90278",
    "90280", "90290", "90291", "90292", "90293", "90294", "90295", "90296",

    # South Bay/Torrance/Long Beach (905xx-908xx)
    "90501", "90502", "90503", "90504", "90505", "90506", "90507", "90508", "90509", "90510",
    "90601", "90602", "90603", "90604", "90605", "90606", "90607", "90608", "90609", "90610",
    "90620", "90621", "90622", "90623", "90624", "90630", "90631", "90632", "90633", "90637",
    "90638", "90639", "90640", "90650", "90651", "90652", "90660", "90661", "90662", "90665",
    "90670", "90671", "90680", "90701", "90702", "90703", "90704", "90706", "90707", "90710",
    "90711", "90712", "90713", "90714", "90715", "90716", "90717", "90720", "90721", "90723",
    "90731", "90732", "90733", "90734", "90740", "90742", "90743", "90744", "90745", "90746",
    "90747", "90748", "90749", "90755", "90802", "90803", "90804", "90805", "90806", "90807",
    "90808", "90809", "90810", "90813", "90814", "90815", "90822", "90831", "90832", "90833",
    "90834", "90835", "90840", "90842", "90844", "90846", "90847", "90848", "90853", "90888",
    "90895", "90899",

    # San Fernando Valley/Pasadena/San Gabriel Valley (910xx-918xx)
    "91001", "91003", "91006", "91007", "91008", "91009", "91010", "91011", "91012", "91016",
    "91017", "91020", "91021", "91023", "91024", "91025", "91030", "91031", "91040", "91041",
    "91042", "91043", "91046", "91050", "91051", "91066", "91077", "91101", "91102", "91103",
    "91104", "91105", "91106", "91107", "91108", "91109", "91110", "91114", "91115", "91116",
    "91117", "91118", "91121", "91123", "91124", "91125", "91126", "91129", "91182", "91184",
    "91185", "91188", "91189", "91199", "91201", "91202", "91203", "91204", "91205", "91206",
    "91207", "91208", "91209", "91210", "91214", "91221", "91222", "91224", "91225", "91226",
    "91301", "91302", "91303", "91304", "91305", "91306", "91307", "91308", "91309", "91310",
    "91311", "91313", "91316", "91319", "91320", "91321", "91322", "91324", "91325", "91326",
    "91327", "91328", "91329", "91330", "91331", "91333", "91334", "91335", "91337", "91340",
    "91341", "91342", "91343", "91344", "91345", "91346", "91350", "91351", "91352", "91353",
    "91354", "91355", "91356", "91357", "91358", "91359", "91360", "91361", "91362", "91363",
    "91364", "91365", "91367", "91371", "91372", "91376", "91380", "91381", "91382", "91383",
    "91384", "91385", "91386", "91387", "91390", "91392", "91393", "91394", "91395", "91396",
    "91401", "91402", "91403", "91404", "91405", "91406", "91407", "91408", "91409", "91410",
    "91411", "91412", "91413", "91416", "91423", "91426", "91436", "91470", "91482", "91495",
    "91496", "91499", "91501", "91502", "91503", "91504", "91505", "91506", "91507", "91508",
    "91510", "91521", "91522", "91523", "91526", "91601", "91602", "91603", "91604", "91605",
    "91606", "91607", "91608", "91609", "91610", "91611", "91612", "91614", "91615", "91616",
    "91617", "91618",

    # East LA/Pomona/Inland Empire border (917xx-918xx)
    "91701", "91702", "91706", "91708", "91709", "91710", "91711", "91714", "91715", "91716",
    "91722", "91723", "91724", "91731", "91732", "91733", "91734", "91735", "91740", "91741",
    "91744", "91745", "91746", "91747", "91748", "91749", "91750", "91754", "91755", "91756",
    "91765", "91766", "91767", "91768", "91769", "91770", "91771", "91772", "91773", "91775",
    "91776", "91778", "91780", "91788", "91789", "91790", "91791", "91792", "91793", "91795",
    "91801", "91802", "91803", "91804", "91841", "91896", "91899",
]


def main():
    print("=" * 50)
    print("LA County Politician Bulk Import")
    print("=" * 50)
    print()
    print(f"API URL: {API_URL}")
    print(f"Delay between ZIPs: {DELAY_MS}ms")
    print()

    # Check for session cookie
    if not SESSION_COOKIE:
        print("ERROR: SESSION_COOKIE environment variable is required")
        print()
        print("Usage:")
        print("  1. Log in to your app as an admin user")
        print("  2. Get your session cookie from browser dev tools")
        print("  3. Run: SESSION_COOKIE='session_id=your-cookie-here' python3 bulk_import_la_county.py")
        print()
        sys.exit(1)

    print(f"Total ZIP codes to import: {len(LA_COUNTY_ZIPS)}")
    print()
    estimated_minutes = (len(LA_COUNTY_ZIPS) * DELAY_MS / 1000) / 60
    print(f"Estimated time: ~{estimated_minutes:.0f} minutes")
    print()
    print("Starting bulk import...")
    print()

    # Make API request
    try:
        response = requests.post(
            f"{API_URL}/essentials/admin/import",
            headers={
                "Content-Type": "application/json",
                "Cookie": SESSION_COOKIE,
            },
            json={
                "zips": LA_COUNTY_ZIPS,
                "delay_between_ms": DELAY_MS,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to start import job: {e}")
        sys.exit(1)

    job_id = data.get("job_id")
    status = data.get("status")

    if not job_id:
        print("ERROR: No job_id in response")
        print(f"Response: {data}")
        sys.exit(1)

    print("✓ Import job started successfully!")
    print()
    print(f"Job ID: {job_id}")
    print(f"Status: {status}")
    print()
    print("Monitor progress with:")
    print(f"  curl -H 'Cookie: {SESSION_COOKIE}' {API_URL}/essentials/admin/import/{job_id}")
    print()
    print("Or list all jobs:")
    print(f"  curl -H 'Cookie: {SESSION_COOKIE}' {API_URL}/essentials/admin/import")
    print()

    # Optional: Poll for completion
    poll = input("Monitor progress automatically? (y/n): ").strip().lower()
    if poll == "y":
        print()
        print("Monitoring job progress (Ctrl+C to stop)...")
        print()
        try:
            while True:
                time.sleep(10)  # Check every 10 seconds
                resp = requests.get(
                    f"{API_URL}/essentials/admin/import/{job_id}",
                    headers={"Cookie": SESSION_COOKIE},
                    timeout=10,
                )
                resp.raise_for_status()
                job = resp.json()

                status = job.get("status", "unknown")
                completed = job.get("completed", 0)
                failed = job.get("failed", 0)
                total = job.get("total_zips", 0)
                current = job.get("current_zip", "")

                progress_pct = (completed / total * 100) if total > 0 else 0

                print(f"[{status}] {completed}/{total} ({progress_pct:.1f}%) | Failed: {failed} | Current: {current}")

                if status in ["completed", "completed_with_errors", "failed"]:
                    print()
                    print(f"✓ Import job {status}!")
                    print(f"  Completed: {completed}")
                    print(f"  Failed: {failed}")
                    if job.get("failed_zips"):
                        print(f"  Failed ZIPs: {', '.join(job['failed_zips'][:10])}{'...' if len(job['failed_zips']) > 10 else ''}")
                    break
        except KeyboardInterrupt:
            print()
            print("Stopped monitoring (job continues running in background)")
        except requests.exceptions.RequestException as e:
            print(f"ERROR: Failed to fetch job status: {e}")


if __name__ == "__main__":
    main()
