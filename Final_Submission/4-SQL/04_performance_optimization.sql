-- Materialized view performance test
EXPLAIN (ANALYZE, BUFFERS)
SELECT year, month, month_name, total_trips
FROM public.monthly_trip_summary
ORDER BY year, month;

-- Confirm index usage for January 2026.
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) AS total_trips
FROM public.fact_trips
WHERE pickup_date_key BETWEEN 20260101 AND 20260131;

-- Index definition used in the project.
-- Run only if the index does not already exist.
CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_date
ON public.fact_trips (pickup_date_key);

ANALYZE public.fact_trips;

-- Refresh the monthly summary after loading new source data.
-- Uncomment and run only when the fact table has been updated.
-- REFRESH MATERIALIZED VIEW public.monthly_trip_summary;
