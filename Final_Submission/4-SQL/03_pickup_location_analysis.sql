-- Top 10 NYC pickup locations
-- Uses the existing materialized view created for dashboard performance.

SELECT *
FROM public.mv_top_10_pickup_locations
ORDER BY 2 DESC
LIMIT 10;

-- Dashboard-facing view.
SELECT *
FROM public.vw_top_10_pickup_locations
ORDER BY 2 DESC
LIMIT 10;
