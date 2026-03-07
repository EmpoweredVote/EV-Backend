-- seed_faizah.sql
-- Idempotent seed script for Faizah Malik (LA City Council District 11 candidate)
-- Run against the essentials/compass schemas.

-- 1. Politician record
INSERT INTO essentials.politicians (
  id, external_id, first_name, last_name, full_name,
  party, party_short_name, bio_text, data_source, is_active,
  urls
) VALUES (
  uuid_generate_v4(), 0,
  'Faizah', 'Malik', 'Faizah Malik',
  'Democratic', 'D',
  'Public interest attorney with 15+ years advocating for renters, immigrants, and working families in Los Angeles.',
  'manual', true,
  ARRAY['https://www.faizahforla.com']
)
ON CONFLICT DO NOTHING;

-- Get the politician ID for subsequent inserts
DO $$
DECLARE
  v_pol_id uuid;
  v_chamber_id uuid;
  v_district_id uuid;
BEGIN
  -- Find or confirm the politician
  SELECT id INTO v_pol_id
    FROM essentials.politicians
    WHERE first_name = 'Faizah' AND last_name = 'Malik' AND data_source = 'manual'
    LIMIT 1;

  IF v_pol_id IS NULL THEN
    RAISE NOTICE 'Faizah Malik politician record not found — skipping dependent inserts';
    RETURN;
  END IF;

  RAISE NOTICE 'Faizah Malik politician_id: %', v_pol_id;

  -- 2. Office record — find LA City Council chamber and District 11
  SELECT id INTO v_chamber_id
    FROM essentials.chambers
    WHERE name LIKE '%Los Angeles City Council%'
    LIMIT 1;

  SELECT id INTO v_district_id
    FROM essentials.districts
    WHERE district_type = 'LOCAL' AND district_id = '11'
      AND geo_id LIKE '%los-angeles%'
    LIMIT 1;

  -- Fallback: try matching by city in the geo_id or label
  IF v_district_id IS NULL THEN
    SELECT id INTO v_district_id
      FROM essentials.districts
      WHERE district_type = 'LOCAL' AND district_id = '11'
      LIMIT 1;
  END IF;

  IF v_chamber_id IS NOT NULL AND v_district_id IS NOT NULL THEN
    INSERT INTO essentials.offices (
      id, politician_id, chamber_id, district_id,
      title, representing_city, representing_state
    ) VALUES (
      uuid_generate_v4(), v_pol_id, v_chamber_id, v_district_id,
      'Los Angeles City Council', 'Los Angeles', 'CA'
    )
    ON CONFLICT DO NOTHING;
    RAISE NOTICE 'Office record inserted (chamber_id: %, district_id: %)', v_chamber_id, v_district_id;
  ELSE
    RAISE NOTICE 'WARNING: Could not find LA City Council chamber (%) or District 11 (%)', v_chamber_id, v_district_id;
  END IF;

  -- 3. Election record
  INSERT INTO essentials.election_records (
    id, politician_id, candidacy_external_id,
    election_name, election_date, position_name,
    is_active, withdrawn, party_name,
    is_primary, is_runoff, is_unexpired_term
  ) VALUES (
    uuid_generate_v4(), v_pol_id, 'manual-faizah-malik-2026',
    '2026 Los Angeles City Council District 11', '2026-06-02', 'Los Angeles City Council',
    true, false, 'Democratic',
    false, false, false
  )
  ON CONFLICT (candidacy_external_id) DO NOTHING;

  -- 4. ZIP code mappings for District 11 areas
  INSERT INTO essentials.zip_politicians (zip, politician_id, last_seen, is_contained)
  VALUES
    ('90049', v_pol_id, NOW(), false),
    ('90066', v_pol_id, NOW(), false),
    ('90291', v_pol_id, NOW(), false),
    ('90293', v_pol_id, NOW(), false),
    ('90094', v_pol_id, NOW(), false),
    ('90045', v_pol_id, NOW(), false),
    ('90056', v_pol_id, NOW(), false),
    ('90064', v_pol_id, NOW(), false),
    ('90272', v_pol_id, NOW(), false)
  ON CONFLICT (zip, politician_id) DO NOTHING;

  -- 5. Compass stances — map platform areas to closest existing topics
  -- Fetch active topics and map Faizah's positions:
  --   Housing & Tenants' Rights → Housing (strong support)
  --   Immigrants' Rights → Immigration (strong support)
  --   Climate → Environment (strong support)
  --   Economy For All → Economy (strong support)
  --   Homelessness → Healthcare / Social Services (strong support)
  --   Fire Recovery → Government Spending (moderate support)

  -- Insert compass answers for matching topics
  INSERT INTO compass.answers (id, politician_id, topic_id, value)
  SELECT
    uuid_generate_v4(),
    v_pol_id,
    t.id,
    CASE t.short_title
      WHEN 'Housing' THEN 2.0
      WHEN 'Immigration' THEN 2.0
      WHEN 'Environment' THEN 1.75
      WHEN 'Economy' THEN 1.5
      WHEN 'Healthcare' THEN 1.75
      WHEN 'Government Spending' THEN 1.5
      WHEN 'Education' THEN 1.5
      WHEN 'Criminal Justice' THEN 1.5
      ELSE NULL
    END
  FROM compass.topics t
  WHERE t.is_active = true
    AND t.short_title IN ('Housing', 'Immigration', 'Environment', 'Economy', 'Healthcare', 'Government Spending', 'Education', 'Criminal Justice')
    AND NOT EXISTS (
      SELECT 1 FROM compass.answers a
      WHERE a.politician_id = v_pol_id AND a.topic_id = t.id
    );

  -- 6. Endorsements
  INSERT INTO essentials.endorsements (id, politician_id, endorser_string, status, recommendation, candidacy_external_id, election_date)
  VALUES
    (uuid_generate_v4(), v_pol_id, 'SEIU Local 721', 'endorsed', 'PRO', 'manual-faizah-malik-2026', '2026-06-02'),
    (uuid_generate_v4(), v_pol_id, 'DSA-LA', 'endorsed', 'PRO', 'manual-faizah-malik-2026', '2026-06-02'),
    (uuid_generate_v4(), v_pol_id, 'Unite Here! Local 11', 'endorsed', 'PRO', 'manual-faizah-malik-2026', '2026-06-02'),
    (uuid_generate_v4(), v_pol_id, 'CA Working Families Party', 'endorsed', 'PRO', 'manual-faizah-malik-2026', '2026-06-02'),
    (uuid_generate_v4(), v_pol_id, 'LA County Federation of Labor', 'endorsed', 'PRO', 'manual-faizah-malik-2026', '2026-06-02')
  ON CONFLICT DO NOTHING;

  RAISE NOTICE 'Faizah Malik seed complete';
END $$;
