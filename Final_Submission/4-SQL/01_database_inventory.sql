-- NYC Mobility Analytics Platform
-- Purpose: Inspect the existing PostgreSQL database objects.
-- Run in pgAdmin Query Tool while connected to qeema_nyc.

SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_type, table_name;

-- Check the main fact table row count.
SELECT COUNT(*) AS total_fact_trips
FROM public.fact_trips;

-- Check ETL file processing status, if the audit table is present.
SELECT *
FROM public.etl_file_status
ORDER BY 1;
