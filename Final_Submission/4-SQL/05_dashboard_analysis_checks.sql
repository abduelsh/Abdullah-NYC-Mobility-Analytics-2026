-- Dashboard analysis objects
-- These queries inspect the existing dashboard views/materialized views.
-- Run each section separately if an object is not present in your database.

SELECT * FROM public.vw_avg_fare_by_borough;
SELECT * FROM public.vw_trips_by_day_of_week;
SELECT * FROM public.vw_avg_duration_by_borough;
SELECT * FROM public.vw_trips_by_provider;
SELECT * FROM public.vw_trips_by_hour;
SELECT * FROM public.vw_tip_analysis_by_borough;
