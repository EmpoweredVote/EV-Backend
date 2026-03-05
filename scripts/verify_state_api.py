#!/usr/bin/env python3
"""
verify_state_api.py

Verifies that all 4 state legislative API endpoints return valid, non-empty
data for known Indiana and California legislators via the running Go API server.

Tests the full HTTP stack: HTTP request → Chi router → GORM → PostgreSQL.

Usage:
    python verify_state_api.py
    python verify_state_api.py --api-url http://localhost:5050
    python verify_state_api.py --api-url https://api.empowered.vote --verbose

Flags:
    --api-url URL   Base URL of the Go API server (default: http://localhost:5050)
    --verbose       Print full JSON responses for each endpoint

Exit codes:
    0   All checks passed
    1   One or more checks failed (or server unreachable)
"""

import argparse
import json
import sys

import requests


# ---------------------------------------------------------------------------
# Test legislators (UUIDs from Phase 60/61 validation)
# ---------------------------------------------------------------------------
TEST_LEGISLATORS = [
    {
        "state": "Indiana",
        "name": "Rodric Bray",
        "id": "97c61094-b962-48b2-b6ef-de96b5f9bb7a",
    },
    {
        "state": "California",
        "name": "Lisa Calderon",
        "id": "0afa998d-94e9-4af4-ba00-256c38869398",
    },
]

# ---------------------------------------------------------------------------
# Endpoint definitions
# endpoint: path suffix (after /essentials/politician/{id})
# type: "array" or "object"
# required_key: for arrays — spot-check that first item has this key
# ---------------------------------------------------------------------------
ENDPOINTS = [
    {
        "path": "committees",
        "label": "/committees",
        "type": "array",
        "required_key": "name",
    },
    {
        "path": "bills",
        "label": "/bills",
        "type": "array",
        "required_key": "title",
    },
    {
        "path": "votes",
        "label": "/votes",
        "type": "array",
        "required_key": "result",
    },
    {
        "path": "legislative-summary",
        "label": "/legislative-summary",
        "type": "object",
        "required_key": None,
    },
]


def check_endpoint(api_url: str, politician_id: str, endpoint: dict, verbose: bool) -> tuple:
    """
    Hit a single endpoint and validate the response.

    Returns (passed: bool, detail: str).
    """
    url = f"{api_url}/essentials/politician/{politician_id}/{endpoint['path']}"
    try:
        response = requests.get(url, timeout=15)
    except requests.exceptions.ConnectionError:
        return False, "connection refused"
    except requests.exceptions.Timeout:
        return False, "request timed out"

    if response.status_code != 200:
        return False, f"HTTP {response.status_code}"

    try:
        data = response.json()
    except ValueError:
        return False, "response is not valid JSON"

    if verbose:
        print(f"      Response: {json.dumps(data)[:200]}")

    if endpoint["type"] == "array":
        if not isinstance(data, list):
            return False, f"expected JSON array, got {type(data).__name__}"
        if len(data) == 0:
            return False, "empty array (no data)"
        required_key = endpoint.get("required_key")
        if required_key and required_key not in data[0]:
            return False, f"first item missing expected key '{required_key}'"
        count = len(data)
        return True, f"{count} {'item' if count == 1 else 'items'}"

    elif endpoint["type"] == "object":
        if not isinstance(data, dict):
            return False, f"expected JSON object, got {type(data).__name__}"
        if not data:
            return False, "empty object (no data)"
        return True, "summary present"

    return False, "unknown endpoint type"


def run_verification(api_url: str, verbose: bool) -> int:
    """
    Run all checks. Returns 0 on full pass, 1 on any failure.
    """
    # Verify server is reachable before running individual checks
    try:
        requests.get(f"{api_url}/", timeout=5)
    except requests.exceptions.ConnectionError:
        print(
            f"ERROR: Cannot reach API at {api_url}. "
            "Is the Go server running? (cd EV-Backend && go run .)"
        )
        return 1
    except requests.exceptions.Timeout:
        print(f"ERROR: API at {api_url} did not respond within 5 seconds.")
        return 1
    except Exception:
        # Some backends return non-200 on /, which is fine — server is up
        pass

    print("State Legislative API Verification")
    print("===================================")

    total_checks = 0
    passed_checks = 0
    any_failure = False

    for legislator in TEST_LEGISLATORS:
        print(f"\n{legislator['state']} ({legislator['name']}):")

        for endpoint in ENDPOINTS:
            total_checks += 1
            label = endpoint["label"]

            try:
                passed, detail = check_endpoint(api_url, legislator["id"], endpoint, verbose)
            except Exception as exc:
                passed = False
                detail = f"unexpected error: {exc}"

            status = "PASS" if passed else "FAIL"
            # Pad label to fixed width for alignment
            padded = f"  {label}".ljust(28)
            print(f"{padded} {status} ({detail})")

            if passed:
                passed_checks += 1
            else:
                any_failure = True

    print(f"\nResult: {passed_checks}/{total_checks} checks passed")

    return 1 if any_failure else 0


def main():
    parser = argparse.ArgumentParser(
        description="Verify state legislative API endpoints for Indiana and California legislators"
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:5050",
        metavar="URL",
        help="Base URL of the Go API server (default: http://localhost:5050)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full JSON responses for each endpoint",
    )
    args = parser.parse_args()

    # Strip trailing slash for clean URL construction
    api_url = args.api_url.rstrip("/")

    sys.exit(run_verification(api_url, args.verbose))


if __name__ == "__main__":
    main()
