-- Check if topics table exists and what constraints it has
SELECT 
    conname AS constraint_name,
    contype AS constraint_type
FROM pg_constraint 
WHERE conrelid = 'compass.topics'::regclass;

-- Check indexes
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE schemaname = 'compass' AND tablename = 'topics';
