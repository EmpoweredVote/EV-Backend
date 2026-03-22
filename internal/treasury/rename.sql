-- Phase 92: Pre-deployment rename script
-- Run via Supabase SQL editor BEFORE deploying updated Go code
-- Per D-09: GORM AutoMigrate cannot rename tables/columns

-- Step 1: Rename table
ALTER TABLE treasury.cities RENAME TO municipalities;

-- Step 2: Rename FK column on budgets
ALTER TABLE treasury.budgets RENAME COLUMN city_id TO municipality_id;

-- Step 3: Drop old two-column unique index on budgets (SCHM-01 fix prep)
DROP INDEX IF EXISTS treasury.idx_budget_city_year;

-- Step 4: Drop old single-column unique index on municipalities.name
-- GORM auto-generates this as idx_cities_name or similar
DROP INDEX IF EXISTS treasury.idx_cities_name;

-- Step 5: Drop any other auto-generated GORM indexes on old table name
-- Safe to run even if they don't exist
DROP INDEX IF EXISTS treasury.idx_cities_name_1;
