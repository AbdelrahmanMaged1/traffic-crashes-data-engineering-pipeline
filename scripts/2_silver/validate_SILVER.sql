-- 1. Row counts vs bronze
SELECT 'crashes' tbl, COUNT(*) silver_rows FROM silver.crashes
UNION ALL SELECT 'people',   COUNT(*) FROM silver.people
UNION ALL SELECT 'vehicles', COUNT(*) FROM silver.vehicles;

-- 2. Confirm dates parsed correctly (should see actual datetimes, not NULL)
SELECT TOP 5 CRASH_DATE, DATE_POLICE_NOTIFIED FROM silver.crashes WHERE CRASH_DATE IS NOT NULL;

-- 3. Catch any bad casts (should be 0 or very low)
SELECT COUNT(*) bad_dates    FROM silver.crashes  WHERE CRASH_DATE IS NULL;
SELECT COUNT(*) bad_ages     FROM silver.people   WHERE AGE IS NULL ; -- compare to bronze nulls
SELECT COUNT(*) bad_veh_year FROM silver.vehicles WHERE VEHICLE_YEAR IS NULL;


