-- Fix Monroe County 10th Circuit Court judge names and relabel Seat → Division
-- Issue: Official in.gov data shows Division 1–9 labels and fuller judge names.
--   The DB has shortened names and incorrect "Seat N" labels that do not correspond
--   to the correct division numbers (Cicero sourced seat numbers were scrambled).
--
-- This script is idempotent and safe to re-run.
-- Run via: psql $DATABASE_URL -f fix_monroe_county_circuit_court_judges.sql
--   or paste into Supabase SQL Editor.

BEGIN;

-- =============================================================================
-- Section 1: Fix politician names (essentials.politicians)
-- =============================================================================

-- Geoffrey Bradley: add middle initial J
UPDATE essentials.politicians
SET first_name = 'Geoffrey',
    middle_initial = 'J',
    full_name = 'Geoffrey J. Bradley'
WHERE id = '999a9d38-9894-45f0-80c7-228880089699';

-- Christine Talley Haseman: middle initial is compound "Talley"
UPDATE essentials.politicians
SET first_name = 'Christine',
    middle_initial = 'Talley',
    full_name = 'Christine Talley Haseman'
WHERE id = '36af947b-b548-4964-8003-889cffbf7dad';

-- Catherine B. Stafford: add middle initial B
UPDATE essentials.politicians
SET first_name = 'Catherine',
    middle_initial = 'B',
    full_name = 'Catherine B. Stafford'
WHERE id = '1db5eafb-b7c5-4716-b3a8-ad0ff8a7dc63';

-- Mary Ellen Diekhoff: middle name is Ellen
UPDATE essentials.politicians
SET first_name = 'Mary',
    middle_initial = 'Ellen',
    full_name = 'Mary Ellen Diekhoff'
WHERE id = '3e95fc5c-6927-492c-9f1c-6d65b5b7b9cb';

-- =============================================================================
-- Section 2: Relabel districts (essentials.districts)
-- Seat N → Division N (correct division numbers per in.gov)
-- =============================================================================

-- Bradley: Seat 9 → Division 1
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 1',
    district_id = '1'
WHERE id = '07c65675-dabd-4fc1-b723-3c8af82d87f6';

-- Haughton: Seat 6 → Division 2
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 2',
    district_id = '2'
WHERE id = '2a12897c-410a-4649-8bc8-1763d13ac4cb';

-- Haseman: Seat 2 → Division 3
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 3',
    district_id = '3'
WHERE id = '06448769-df8b-4810-94a2-b73e87a0cbce';

-- Stafford: Seat 3 → Division 4
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 4',
    district_id = '4'
WHERE id = 'f95266d5-2624-4a75-88f5-6229f426f25a';

-- Diekhoff: Seat 4 → Division 5
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 5',
    district_id = '5'
WHERE id = '412d8f9e-de03-46bf-9037-5e83e3faee5b';

-- Krothe: Seat 5 → Division 6
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 6',
    district_id = '6'
WHERE id = 'ed7421cf-47b7-43ee-98f9-06916748093d';

-- Harvey: Seat 1 → Division 7
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 7',
    district_id = '7'
WHERE id = '72f8d191-0c9a-421a-83fb-7680553c8978';

-- Salzmann: Seat 7 → Division 8
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 8',
    district_id = '8'
WHERE id = 'f482816e-0387-4623-b6b4-13bc7eac06ce';

-- Fawcett: Seat 8 → Division 9
UPDATE essentials.districts
SET label = 'Indiana Circuit Court Judge - 10th Circuit (Monroe County), Division 9',
    district_id = '9'
WHERE id = 'abd6f087-de4d-4872-b8b6-b60bbb302eef';

-- =============================================================================
-- Section 3: Relabel chambers (essentials.chambers)
-- Seat N → Division N
-- =============================================================================

-- Bradley: Division 1
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 1'
WHERE id = '78c06524-a8cb-415a-9943-1b8aaa5ba39c';

-- Haughton: Division 2
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 2'
WHERE id = 'a563c265-22ca-4c0e-a870-aeba00a11436';

-- Haseman: Division 3
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 3'
WHERE id = 'f480b81f-1e3f-4b6c-abc6-475a281eec6b';

-- Stafford: Division 4
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 4'
WHERE id = '20abf84a-a5fb-431e-bb13-76df1a1fa868';

-- Diekhoff: Division 5
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 5'
WHERE id = 'c2e818a7-fa54-44aa-bb3a-9957f5b67e41';

-- Krothe: Division 6
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 6'
WHERE id = '783c3991-5941-4a61-b9e7-acc6ffbb8e48';

-- Harvey: Division 7
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 7'
WHERE id = '043a199c-0b40-4e97-a3bb-3bac7f0d54b6';

-- Salzmann: Division 8
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 8'
WHERE id = 'ea940b0e-348c-44bb-91f4-5c9e7fab46e2';

-- Fawcett: Division 9
UPDATE essentials.chambers
SET name = 'Indiana Circuit Court Judge - 10th Circuit, Division 9'
WHERE id = 'b98ce33f-1e6a-4061-a9a4-443b36f29186';

-- =============================================================================
-- Section 4: Relabel offices (essentials.offices)
-- Seat N → Division N
-- =============================================================================

-- Bradley: Division 1
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 1'
WHERE id = '44a339c0-e32a-4b4a-aa83-b5617277c8f2';

-- Haughton: Division 2
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 2'
WHERE id = '1b893cc9-0ff6-479b-810a-22120dd44e8a';

-- Haseman: Division 3
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 3'
WHERE id = '79ad0a9b-8e51-4e53-a515-407217dddd23';

-- Stafford: Division 4
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 4'
WHERE id = 'e7445bb2-2f55-45bb-97bc-d910133d2d2f';

-- Diekhoff: Division 5
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 5'
WHERE id = '1fceb227-022a-4089-a459-f080ca645e61';

-- Krothe: Division 6
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 6'
WHERE id = '516ecc9a-2c82-4d51-aacd-67032634e26c';

-- Harvey: Division 7
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 7'
WHERE id = 'ca3aa3fa-851b-4907-994a-aaeae011e0c4';

-- Salzmann: Division 8
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 8'
WHERE id = 'd6517a23-9864-4b18-b2cd-2cd539264c00';

-- Fawcett: Division 9
UPDATE essentials.offices
SET title = 'Indiana Circuit Court Judge - 10th Circuit, Division 9'
WHERE id = '2261f602-d241-41fe-806b-9048b492cfba';

COMMIT;

-- =============================================================================
-- Verification queries (run after migration to confirm results):
-- =============================================================================

-- 1. All 9 judges with updated full_name, label, district_id (ordered by division)
-- SELECT p.full_name, d.label, d.district_id
-- FROM essentials.politicians p
-- JOIN essentials.offices o ON o.politician_id = p.id
-- JOIN essentials.districts d ON d.id = o.district_id
-- WHERE p.id IN (
--   '999a9d38-9894-45f0-80c7-228880089699',  -- Bradley
--   '741be6f7-eac1-4458-bef1-6576ba5acd98',  -- Haughton
--   '36af947b-b548-4964-8003-889cffbf7dad',  -- Haseman
--   '1db5eafb-b7c5-4716-b3a8-ad0ff8a7dc63',  -- Stafford
--   '3e95fc5c-6927-492c-9f1c-6d65b5b7b9cb',  -- Diekhoff
--   '048cb4ba-9b07-42e6-b45d-f4dffad2a088',  -- Krothe
--   '52a1db12-bef7-4e68-9daf-bdd17c82f6c0',  -- Harvey
--   '4e058a9c-a4ab-403c-88d3-32918e860cc8',  -- Salzmann
--   'd60ef0ea-b764-47eb-989a-3d5f1c461d10'   -- Fawcett
-- )
-- ORDER BY d.district_id::int;
--
-- Expected: 9 rows with full official names, Division 1–9 labels in order
--
-- 2. Confirm 0 rows still have "Seat" in district label for Monroe County Circuit Court
-- SELECT COUNT(*) FROM essentials.districts
-- WHERE label LIKE '%10th Circuit (Monroe County)%'
--   AND label LIKE '%Seat%';
-- Expected: 0
--
-- 3. Confirm 0 rows still have "Seat" in chamber name for Monroe County Circuit Court
-- SELECT COUNT(*) FROM essentials.chambers
-- WHERE name LIKE '%10th Circuit%'
--   AND name LIKE '%Seat%';
-- Expected: 0
--
-- 4. Confirm 0 rows still have "Seat" in office title for Monroe County Circuit Court
-- SELECT COUNT(*) FROM essentials.offices
-- WHERE title LIKE '%10th Circuit%'
--   AND title LIKE '%Seat%';
-- Expected: 0
