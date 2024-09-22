SELECT table_name
FROM information_schema.tables
where table_schema = '{DB_SCHEMA}'
ORDER BY table_name;