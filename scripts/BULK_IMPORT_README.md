# Bulk Import Politicians by ZIP Code

This directory contains scripts to bulk import politician data for entire counties or regions using the `/essentials/admin/import` API endpoint.

## Quick Start: LA County Import

### Option 1: Python Script (Recommended)

```bash
cd EV-Backend/scripts

# Set your admin session cookie
export SESSION_COOKIE="session_id=your-cookie-here"

# Optional: customize settings
export API_URL="http://localhost:5050"  # or https://api.empowered.vote
export DELAY_MS=3000  # milliseconds between ZIP imports (default: 3000)

# Run the import
python3 bulk_import_la_county.py
```

### Option 2: Bash Script

```bash
cd EV-Backend/scripts

# Set your admin session cookie
SESSION_COOKIE="session_id=your-cookie-here" ./bulk_import_la_county.sh
```

### Option 3: Direct curl

```bash
curl -X POST http://localhost:5050/essentials/admin/import \
  -H 'Content-Type: application/json' \
  -H 'Cookie: session_id=your-session-cookie' \
  -d '{
    "zips": ["90001", "90002", ...],
    "delay_between_ms": 3000
  }'
```

## Getting Your Session Cookie

1. **Log in** to your app as an admin user
2. **Open browser DevTools** (F12 or Cmd+Opt+I)
3. Go to **Application/Storage > Cookies**
4. Find the `session_id` cookie
5. **Copy the value** (everything after `session_id=`)

Example cookie format: `session_id=abc123def456...`

## What Gets Imported

### LA County Coverage

- **Total ZIP codes:** ~400
- **Estimated time:** ~20 hours at 3-second delay (to avoid BallotReady API rate limits)
- **Geographic coverage:**
  - Downtown/Central LA (90001-90089)
  - West LA/Santa Monica (90201-90296)
  - South Bay/Torrance/Long Beach (90501-90899)
  - San Fernando Valley (91301-91618)
  - Pasadena/San Gabriel Valley (91001-91226)
  - East LA/Pomona (91701-91899)

### Data Imported Per ZIP

For each ZIP code, the system:
1. Calls BallotReady API to fetch all officials (federal, state, local)
2. Upserts politicians, offices, chambers, districts
3. Maps politicians to ZIP codes
4. Updates cache timestamps

This ensures any address in LA County will return instant results from your local database instead of hitting the BallotReady API.

## Monitoring Progress

### Check Job Status

```bash
# Get specific job status
curl -H 'Cookie: session_id=your-cookie' \
  http://localhost:5050/essentials/admin/import/JOB_ID

# List all import jobs
curl -H 'Cookie: session_id=your-cookie' \
  http://localhost:5050/essentials/admin/import
```

### Job Response Format

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "running",  // or "completed", "completed_with_errors", "failed"
  "total_zips": 400,
  "completed": 150,
  "failed": 2,
  "current_zip": "90210",
  "failed_zips": ["90050", "90051"],
  "delay_between_ms": 3000,
  "started_at": "2025-01-15T10:30:00Z",
  "completed_at": null  // timestamp when job finishes
}
```

### Backend Logs

The backend logs each ZIP as it processes:

```
[BulkImport] job=550e8400 processing ZIP 90001 (1/400)
[BulkImport] job=550e8400 ZIP 90001 failed: rate limit exceeded
[BulkImport] job=550e8400 processing ZIP 90002 (2/400)
[BulkImport] job=550e8400 finished — completed=398 failed=2
```

## Rate Limiting

BallotReady API has rate limits. The default 3-second delay is conservative:

- **3000ms (3 sec):** Safe, ~20 hours for 400 ZIPs
- **2000ms (2 sec):** Moderate, ~13 hours
- **1000ms (1 sec):** Aggressive, ~7 hours (may hit rate limits)

If you see frequent failures, increase the delay:

```bash
export DELAY_MS=5000  # 5 seconds between ZIPs
```

## Retry Failed ZIPs

If some ZIPs fail, you can retry just those:

```bash
# Extract failed ZIPs from job response
curl -H 'Cookie: session_id=xxx' http://localhost:5050/essentials/admin/import/JOB_ID \
  | jq -r '.failed_zips[]'

# Create a new import job with only failed ZIPs
curl -X POST http://localhost:5050/essentials/admin/import \
  -H 'Content-Type: application/json' \
  -H 'Cookie: session_id=xxx' \
  -d '{
    "zips": ["90050", "90051"],
    "delay_between_ms": 5000
  }'
```

## Importing Other Counties

To import a different county:

1. **Find ZIP codes** for that county (use USPS ZIP database or Census data)
2. **Create a new script** or modify the existing one with the ZIP list
3. **Run the import** with appropriate delay

Example for Orange County, CA:

```python
ORANGE_COUNTY_ZIPS = [
    "92602", "92603", "92604", "92606", "92610", "92612", "92614", "92617",
    "92618", "92620", "92624", "92625", "92626", "92627", "92628", "92629",
    # ... add all Orange County ZIPs
]
```

## Verifying Import Success

After the import completes:

### 1. Check Database

```sql
-- Count politicians per ZIP
SELECT zip, COUNT(*) as politician_count
FROM essentials.zip_politicians
WHERE zip LIKE '90%' OR zip LIKE '91%'
GROUP BY zip
ORDER BY politician_count DESC
LIMIT 20;

-- Check cache freshness
SELECT zip, last_fetched, extract(epoch from (now() - last_fetched)) / 3600 as hours_ago
FROM essentials.zip_caches
WHERE zip LIKE '90%' OR zip LIKE '91%'
ORDER BY last_fetched DESC
LIMIT 20;
```

### 2. Test API

```bash
# Should return instant results (X-Data-Status: fresh)
curl http://localhost:5050/essentials/politicians/90210

# Check response headers
curl -i http://localhost:5050/essentials/politicians/90001 | grep X-Data-Status
```

Expected: `X-Data-Status: fresh` (served from cache, not BallotReady API)

### 3. Test Address Search

```bash
curl -X POST http://localhost:5050/essentials/politicians/search \
  -H 'Content-Type: application/json' \
  -d '{"query": "1200 Getty Center Dr, Los Angeles, CA 90049"}'
```

Expected: Response headers should show `X-Data-Status: fresh-local` if geofence data is imported.

## Troubleshooting

### "Invalid session cookie" / 401 Unauthorized

- Make sure you're logged in as an **admin user**
- Check that `session_id` cookie hasn't expired
- Re-login and get a fresh cookie

### "Rate limit exceeded" errors

- Increase `DELAY_MS` to 4000 or 5000
- Retry failed ZIPs after waiting 1 hour

### Import job stuck at "running"

- Check backend logs for errors
- Backend might have crashed/restarted - job state is in-memory only
- Re-run the import (it will skip already-cached ZIPs if they're still fresh)

### Some ZIPs return no politicians

- Not all ZIP codes have local officials (military bases, PO boxes, etc.)
- Federal officials should still appear
- Check BallotReady API directly to confirm data availability

## Next Steps

After bulk importing LA County:

1. **Import geofence boundaries** (see `EV-Backend/scripts/import_shapefiles.py`)
   - This enables instant address → politician lookups without any API calls
   - ~10-15 minute one-time import

2. **Set up monitoring** to track cache freshness
   - ZIPs expire after 90 days
   - Can automate re-warming via cron job

3. **Expand to other counties** as needed
   - San Diego County, Orange County, etc.
   - Follow same bulk import pattern

## Cost Considerations

BallotReady API calls may have usage limits or costs depending on your plan.

**Optimization strategies:**
- Import during off-peak hours
- Use slower delay (3-5 seconds) to stay well under rate limits
- Only re-import when caches expire (90 days for ZIPs)
- Use geofence-based lookups to avoid API calls entirely for cached areas

---

For more information, see:
- `EV-Backend/GEOFENCE_SETUP.md` - Full geofence system documentation
- `EV-Backend/internal/essentials/admin.go` - Bulk import implementation
- `EV-Backend/scripts/README.md` - Geofence boundary import guide
