-- Normalize congressional district labels in essentials.districts
-- Issue: Cicero-sourced labels like "Indiana 9th Congress" and "Indiana - Class 3"
--   are surfaced in essentials sort keys and CompassV2 senator subtitles,
--   making the UI look unprofessional.
--
-- Target formats:
--   NATIONAL_LOWER: "District N" (e.g., "District 9")
--   NATIONAL_UPPER: Full state name only (e.g., "Indiana")
--
-- This script is idempotent and safe to re-run.
-- Run via: psql $DATABASE_URL -f normalize_congressional_district_labels.sql
--   or paste into Supabase SQL Editor.

BEGIN;

-- Step 1: Normalize U.S. House districts to "District N"
UPDATE essentials.districts
SET label = 'District ' || district_id
WHERE district_type = 'NATIONAL_LOWER'
  AND district_id ~ '^\d+$'
  AND district_id != '';

-- Step 2: Normalize U.S. Senate districts to full state name only
-- Maps the 2-letter state abbreviation to the full name,
-- stripping any class designation or ordinal suffix currently in the label.
UPDATE essentials.districts
SET label = CASE state
  WHEN 'AL' THEN 'Alabama'
  WHEN 'AK' THEN 'Alaska'
  WHEN 'AZ' THEN 'Arizona'
  WHEN 'AR' THEN 'Arkansas'
  WHEN 'CA' THEN 'California'
  WHEN 'CO' THEN 'Colorado'
  WHEN 'CT' THEN 'Connecticut'
  WHEN 'DE' THEN 'Delaware'
  WHEN 'FL' THEN 'Florida'
  WHEN 'GA' THEN 'Georgia'
  WHEN 'HI' THEN 'Hawaii'
  WHEN 'ID' THEN 'Idaho'
  WHEN 'IL' THEN 'Illinois'
  WHEN 'IN' THEN 'Indiana'
  WHEN 'IA' THEN 'Iowa'
  WHEN 'KS' THEN 'Kansas'
  WHEN 'KY' THEN 'Kentucky'
  WHEN 'LA' THEN 'Louisiana'
  WHEN 'ME' THEN 'Maine'
  WHEN 'MD' THEN 'Maryland'
  WHEN 'MA' THEN 'Massachusetts'
  WHEN 'MI' THEN 'Michigan'
  WHEN 'MN' THEN 'Minnesota'
  WHEN 'MS' THEN 'Mississippi'
  WHEN 'MO' THEN 'Missouri'
  WHEN 'MT' THEN 'Montana'
  WHEN 'NE' THEN 'Nebraska'
  WHEN 'NV' THEN 'Nevada'
  WHEN 'NH' THEN 'New Hampshire'
  WHEN 'NJ' THEN 'New Jersey'
  WHEN 'NM' THEN 'New Mexico'
  WHEN 'NY' THEN 'New York'
  WHEN 'NC' THEN 'North Carolina'
  WHEN 'ND' THEN 'North Dakota'
  WHEN 'OH' THEN 'Ohio'
  WHEN 'OK' THEN 'Oklahoma'
  WHEN 'OR' THEN 'Oregon'
  WHEN 'PA' THEN 'Pennsylvania'
  WHEN 'RI' THEN 'Rhode Island'
  WHEN 'SC' THEN 'South Carolina'
  WHEN 'SD' THEN 'South Dakota'
  WHEN 'TN' THEN 'Tennessee'
  WHEN 'TX' THEN 'Texas'
  WHEN 'UT' THEN 'Utah'
  WHEN 'VT' THEN 'Vermont'
  WHEN 'VA' THEN 'Virginia'
  WHEN 'WA' THEN 'Washington'
  WHEN 'WV' THEN 'West Virginia'
  WHEN 'WI' THEN 'Wisconsin'
  WHEN 'WY' THEN 'Wyoming'
  WHEN 'DC' THEN 'District of Columbia'
  ELSE label  -- leave unknown states unchanged
END
WHERE district_type = 'NATIONAL_UPPER'
  AND state IS NOT NULL
  AND state != '';

COMMIT;

-- Verification queries (run after migration):
--
-- 1. House: should show "District 9" for Erin Houchin's Indiana district
-- SELECT label, district_id, state, district_type
-- FROM essentials.districts
-- WHERE district_type = 'NATIONAL_LOWER' AND state = 'IN';
--   Expected: label = "District 9" (and other numbered districts)
--
-- 2. Senate: should show "Indiana" (not "Indiana - Class 1" or "Indiana 1st Congress")
-- SELECT label, district_id, state, district_type
-- FROM essentials.districts
-- WHERE district_type = 'NATIONAL_UPPER' AND state = 'IN';
--   Expected: label = "Indiana"
--
-- 3. Confirm no NATIONAL_LOWER rows still have old ordinal Congress format
-- SELECT COUNT(*) FROM essentials.districts
-- WHERE district_type = 'NATIONAL_LOWER'
--   AND label ~ '(1st|2nd|3rd|[0-9]th) Congress';
--   Expected: 0
--
-- 4. Confirm no NATIONAL_UPPER rows still have class designations
-- SELECT COUNT(*) FROM essentials.districts
-- WHERE district_type = 'NATIONAL_UPPER'
--   AND label ~ '(Class|class|\d+(st|nd|rd|th))';
--   Expected: 0
