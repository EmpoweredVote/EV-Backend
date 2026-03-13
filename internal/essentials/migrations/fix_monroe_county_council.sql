-- Fix Monroe County Council data
-- Issue: Monroe County Council members invisible in search results due to:
--   1. Liz Feitl linked to wrong district (Assessor instead of At Large)
--   2. Duplicate politician records for districts 1-4 from quick-8 migration
--   3. Missing zip_politicians entries for Liz Feitl
--
-- This script is idempotent and safe to re-run.
-- Run via: psql $DATABASE_URL -f fix_monroe_county_council.sql
--   or paste into Supabase SQL Editor.

BEGIN;

-- Step 1: Delete Liz Feitl's incorrectly-linked office (pointed to Assessor district)
-- Must happen BEFORE reassigning Munson's office due to unique constraint on politician_id
DELETE FROM essentials.offices
WHERE id = '2e2414f4-3468-4a85-a5ed-60b778b4355e';

-- Step 2: Reassign Cheryl Munson's vacant at-large office to Liz Feitl
UPDATE essentials.offices
SET politician_id = 'b3830ff1-3b9b-463a-bb7d-311bf1bf0168',
    is_vacant = false,
    vacant_since = NULL
WHERE id = 'bbb57efc-6b2e-4d95-a749-e6233241a1ce';

-- Step 3: Delete duplicate migration offices and politicians for districts 1-4
DELETE FROM essentials.offices WHERE politician_id IN (
  SELECT id FROM essentials.politicians WHERE slug IN (
    'peter-iversen-monroe-county-council-d1',
    'kate-wiltz-monroe-county-council-d2',
    'marty-hawk-monroe-county-council-d3',
    'jennifer-crossley-monroe-county-council-d4'
  )
);
DELETE FROM essentials.politicians WHERE slug IN (
  'peter-iversen-monroe-county-council-d1',
  'kate-wiltz-monroe-county-council-d2',
  'marty-hawk-monroe-county-council-d3',
  'jennifer-crossley-monroe-county-council-d4'
);

-- Step 4: Populate zip_politicians for Liz Feitl
-- Uses same ZIP codes as other at-large Monroe County officials (geo_id 18105, district_type COUNTY)
INSERT INTO essentials.zip_politicians (zip, politician_id)
SELECT zp.zip, 'b3830ff1-3b9b-463a-bb7d-311bf1bf0168'
FROM essentials.zip_politicians zp
JOIN essentials.politicians p ON zp.politician_id = p.id
JOIN essentials.offices o ON o.politician_id = p.id
JOIN essentials.districts d ON o.district_id = d.id
WHERE d.geo_id = '18105' AND d.district_type = 'COUNTY'
  AND p.id != 'b3830ff1-3b9b-463a-bb7d-311bf1bf0168'
GROUP BY zp.zip
ON CONFLICT DO NOTHING;

-- Step 5: Mark Cheryl Munson as inactive (no longer holds office)
UPDATE essentials.politicians
SET is_active = false
WHERE id = '4d893b8e-2134-4177-99d9-ef72d859f0d2';

COMMIT;

-- Verification queries (run after migration):
--
-- 1. Verify Liz Feitl's office:
-- SELECT p.full_name, o.title, d.label, d.district_type
-- FROM essentials.offices o
-- JOIN essentials.politicians p ON o.politician_id = p.id
-- JOIN essentials.districts d ON o.district_id = d.id
-- WHERE p.id = 'b3830ff1-3b9b-463a-bb7d-311bf1bf0168';
--
-- 2. Verify no duplicates:
-- SELECT slug FROM essentials.politicians WHERE slug LIKE '%monroe-county-council-d%';
--   Expected: 0 rows
--
-- 3. Verify zip_politicians:
-- SELECT COUNT(*) FROM essentials.zip_politicians WHERE politician_id = 'b3830ff1-3b9b-463a-bb7d-311bf1bf0168';
--   Expected: non-zero count
--
-- 4. Verify Cheryl Munson inactive:
-- SELECT full_name, is_active FROM essentials.politicians WHERE id = '4d893b8e-2134-4177-99d9-ef72d859f0d2';
--   Expected: is_active = false
