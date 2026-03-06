-- apply_noem_vacancy.sql
-- Marks Kristi Noem as inactive and her DHS Secretary office as vacant.
-- Run against the production database after deploying the is_vacant/vacant_since migration.
-- Usage: psql $DATABASE_URL -f scripts/apply_noem_vacancy.sql

BEGIN;

-- 1. Find Noem's politician ID, deactivate her record, and mark her office vacant
DO $$
DECLARE
  noem_id UUID;
  affected INT;
BEGIN
  SELECT id INTO noem_id FROM essentials.politicians
  WHERE last_name = 'Noem' AND first_name = 'Kristi';

  IF noem_id IS NULL THEN
    RAISE EXCEPTION 'Kristi Noem not found in essentials.politicians';
  END IF;

  -- 2. Deactivate politician record (preserves all data for potential future reference)
  UPDATE essentials.politicians SET is_active = false WHERE id = noem_id;
  GET DIAGNOSTICS affected = ROW_COUNT;
  IF affected != 1 THEN
    RAISE EXCEPTION 'Expected 1 row updated for politician, got %', affected;
  END IF;

  -- 3. Mark her office as vacant (is_vacant = true supersedes the is_active check on the politician)
  UPDATE essentials.offices
  SET is_vacant = true, vacant_since = '2025-01-20'
  WHERE politician_id = noem_id;
  GET DIAGNOSTICS affected = ROW_COUNT;
  IF affected != 1 THEN
    RAISE EXCEPTION 'Expected 1 row updated for office, got %', affected;
  END IF;

  RAISE NOTICE 'Successfully marked Kristi Noem (%) as inactive and her office as vacant', noem_id;
END $$;

COMMIT;

-- Verify: should show is_active=false, is_vacant=true, vacant_since=2025-01-20
SELECT p.id, p.full_name, p.is_active, o.title, o.is_vacant, o.vacant_since
FROM essentials.politicians p
JOIN essentials.offices o ON o.politician_id = p.id
WHERE p.last_name = 'Noem';
