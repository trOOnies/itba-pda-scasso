SELECT table_name
FROM information_schema.tables
WHERE table_schema = '{DB_SCHEMA}'
ORDER BY table_name;