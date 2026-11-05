-- Monthly trip volume analysis
-- Uses the materialized summary created in the project.

SELECT year, month, month_name, total_trips
FROM public.monthly_trip_summary
ORDER BY year, month;

-- Alternative: read the dashboard trend view.
SELECT trip_month, year, month, month_name, total_trips
FROM public.vw_monthly_trip_trends
ORDER BY trip_month;
