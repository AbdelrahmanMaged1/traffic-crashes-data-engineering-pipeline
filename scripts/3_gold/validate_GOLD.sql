-- 1. Row counts
SELECT 'dim_date'      tbl, COUNT(*) rows FROM gold.dim_date
UNION ALL SELECT 'dim_weather',  COUNT(*) FROM gold.dim_weather
UNION ALL SELECT 'dim_road',     COUNT(*) FROM gold.dim_road
UNION ALL SELECT 'fact_crashes', COUNT(*) FROM gold.fact_crashes
UNION ALL SELECT 'fact_people',  COUNT(*) FROM gold.fact_people
UNION ALL SELECT 'fact_vehicles',COUNT(*) FROM gold.fact_vehicles;

-- 2. No orphan fact rows (FK integrity check)
SELECT COUNT(*) AS orphan_crashes
FROM gold.fact_crashes fc
LEFT JOIN gold.dim_date    dd ON fc.date_key    = dd.date_key
LEFT JOIN gold.dim_weather dw ON fc.weather_key = dw.weather_key
LEFT JOIN gold.dim_road    dr ON fc.road_key    = dr.road_key
WHERE dd.date_key    IS NULL
   OR dw.weather_key IS NULL
   OR dr.road_key    IS NULL;

-- 3. Injuries sanity check
SELECT
    SUM(injuries_fatal)          total_fatal,
    SUM(injuries_incapacitating) total_incapacitating,
    SUM(injuries_total)          total_injuries
FROM gold.fact_crashes;

-- 4. Age group distribution
SELECT age_group, COUNT(*) cnt
FROM gold.fact_people
GROUP BY age_group ORDER BY cnt DESC;

-- 5. Vehicle year group distribution
SELECT vehicle_year_group, COUNT(*) cnt
FROM gold.fact_vehicles
GROUP BY vehicle_year_group ORDER BY cnt DESC;