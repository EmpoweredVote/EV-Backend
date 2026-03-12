-- Quick-8: Monroe County Council membership update
-- Run once: psql "$DATABASE_URL" -f internal/essentials/data/migrate_quick8_council.sql
-- Idempotent — safe to re-run.

-- 1. Deactivate Cheryl Munson's Monroe County Council office
UPDATE essentials.offices o
SET is_vacant = true, vacant_since = CURRENT_DATE
WHERE o.politician_id = (
  SELECT p.id FROM essentials.politicians p
  WHERE lower(p.first_name) = 'cheryl' AND lower(p.last_name) = 'munson'
  LIMIT 1
)
AND o.chamber_id IN (
  SELECT id FROM essentials.chambers WHERE name LIKE 'Monroe County Council%'
);

-- 2. Add Liz Feitl as Monroe County Council at-large member

INSERT INTO essentials.politicians (first_name, last_name, full_name, slug, party, is_appointed)
SELECT 'Liz', 'Feitl', 'Liz Feitl', 'liz-feitl-monroe-county-council', '', false
WHERE NOT EXISTS (
  SELECT 1 FROM essentials.politicians WHERE slug = 'liz-feitl-monroe-county-council'
);

INSERT INTO essentials.offices (politician_id, chamber_id, district_id, title, normalized_position_name, seats, is_vacant)
SELECT
  p.id,
  c.id,
  d.id,
  'Monroe County Council Member',
  'County Council Member',
  1,
  false
FROM essentials.politicians p
CROSS JOIN essentials.chambers c
CROSS JOIN essentials.districts d
WHERE p.slug = 'liz-feitl-monroe-county-council'
  AND c.name LIKE 'Monroe County Council%'
  AND d.geo_id = '18105'
  AND d.district_type IN ('COUNTY', 'LOCAL')
  AND NOT EXISTS (
    SELECT 1 FROM essentials.offices WHERE politician_id = p.id
  )
LIMIT 1;

-- 3. District 1: Peter Iversen

INSERT INTO essentials.politicians (first_name, last_name, full_name, slug, party, is_appointed)
SELECT 'Peter', 'Iversen', 'Peter Iversen', 'peter-iversen-monroe-county-council-d1', '', false
WHERE NOT EXISTS (
  SELECT 1 FROM essentials.politicians WHERE slug = 'peter-iversen-monroe-county-council-d1'
);

INSERT INTO essentials.offices (politician_id, chamber_id, district_id, title, normalized_position_name, seats, is_vacant)
SELECT p.id, c.id, d.id, 'Monroe County Council Member - District 1', 'County Council Member', 1, false
FROM essentials.politicians p
CROSS JOIN essentials.chambers c
CROSS JOIN essentials.districts d
WHERE p.slug = 'peter-iversen-monroe-county-council-d1'
  AND c.name LIKE 'Monroe County Council%'
  AND d.geo_id = '1810500001'
  AND NOT EXISTS (
    SELECT 1 FROM essentials.offices WHERE politician_id = p.id
  )
LIMIT 1;

-- 4. District 2: Kate Wiltz

INSERT INTO essentials.politicians (first_name, last_name, full_name, slug, party, is_appointed)
SELECT 'Kate', 'Wiltz', 'Kate Wiltz', 'kate-wiltz-monroe-county-council-d2', '', false
WHERE NOT EXISTS (
  SELECT 1 FROM essentials.politicians WHERE slug = 'kate-wiltz-monroe-county-council-d2'
);

INSERT INTO essentials.offices (politician_id, chamber_id, district_id, title, normalized_position_name, seats, is_vacant)
SELECT p.id, c.id, d.id, 'Monroe County Council Member - District 2', 'County Council Member', 1, false
FROM essentials.politicians p
CROSS JOIN essentials.chambers c
CROSS JOIN essentials.districts d
WHERE p.slug = 'kate-wiltz-monroe-county-council-d2'
  AND c.name LIKE 'Monroe County Council%'
  AND d.geo_id = '1810500002'
  AND NOT EXISTS (
    SELECT 1 FROM essentials.offices WHERE politician_id = p.id
  )
LIMIT 1;

-- 5. District 3: Marty Hawk

INSERT INTO essentials.politicians (first_name, last_name, full_name, slug, party, is_appointed)
SELECT 'Marty', 'Hawk', 'Marty Hawk', 'marty-hawk-monroe-county-council-d3', '', false
WHERE NOT EXISTS (
  SELECT 1 FROM essentials.politicians WHERE slug = 'marty-hawk-monroe-county-council-d3'
);

INSERT INTO essentials.offices (politician_id, chamber_id, district_id, title, normalized_position_name, seats, is_vacant)
SELECT p.id, c.id, d.id, 'Monroe County Council Member - District 3', 'County Council Member', 1, false
FROM essentials.politicians p
CROSS JOIN essentials.chambers c
CROSS JOIN essentials.districts d
WHERE p.slug = 'marty-hawk-monroe-county-council-d3'
  AND c.name LIKE 'Monroe County Council%'
  AND d.geo_id = '1810500003'
  AND NOT EXISTS (
    SELECT 1 FROM essentials.offices WHERE politician_id = p.id
  )
LIMIT 1;

-- 6. District 4: Jennifer Crossley

INSERT INTO essentials.politicians (first_name, last_name, full_name, slug, party, is_appointed)
SELECT 'Jennifer', 'Crossley', 'Jennifer Crossley', 'jennifer-crossley-monroe-county-council-d4', '', false
WHERE NOT EXISTS (
  SELECT 1 FROM essentials.politicians WHERE slug = 'jennifer-crossley-monroe-county-council-d4'
);

INSERT INTO essentials.offices (politician_id, chamber_id, district_id, title, normalized_position_name, seats, is_vacant)
SELECT p.id, c.id, d.id, 'Monroe County Council Member - District 4', 'County Council Member', 1, false
FROM essentials.politicians p
CROSS JOIN essentials.chambers c
CROSS JOIN essentials.districts d
WHERE p.slug = 'jennifer-crossley-monroe-county-council-d4'
  AND c.name LIKE 'Monroe County Council%'
  AND d.geo_id = '1810500004'
  AND NOT EXISTS (
    SELECT 1 FROM essentials.offices WHERE politician_id = p.id
  )
LIMIT 1;
