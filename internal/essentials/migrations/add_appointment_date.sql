-- Add appointment_date column for seniority ordering (e.g., Supreme Court justices)
ALTER TABLE essentials.politicians
  ADD COLUMN IF NOT EXISTS appointment_date DATE;

-- Indiana Supreme Court justices — appointment dates from https://www.in.gov/courts/supreme/justices/
-- Seniority order: Massa (2012) → Rush (2012) → Slaughter (2016) → Goff (2017) → Molter (2022)

UPDATE essentials.politicians
SET appointment_date = '2012-04-02'
WHERE slug = 'mark-s-massa'
  AND is_active = true;

UPDATE essentials.politicians
SET appointment_date = '2012-11-07'
WHERE slug = 'loretta-h-rush'
  AND is_active = true;

UPDATE essentials.politicians
SET appointment_date = '2016-06-13'
WHERE slug = 'geoffrey-slaughter'
  AND is_active = true;

UPDATE essentials.politicians
SET appointment_date = '2017-07-24'
WHERE slug = 'christopher-m-goff'
  AND is_active = true;

UPDATE essentials.politicians
SET appointment_date = '2022-09-01'
WHERE slug = 'derek-r-molter'
  AND is_active = true;

-- Update Rush's title to Chief Justice
UPDATE essentials.offices
SET title = 'Indiana Supreme Court Chief Justice'
WHERE politician_id = (
  SELECT id FROM essentials.politicians
  WHERE slug = 'loretta-h-rush' AND is_active = true
);
