/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    Performs the ETL process to populate all gold schema tables from silver.
    Loading order (respects logical FK dependencies):
        1. dim_date
        2. dim_weather
        3. dim_road
        4. fact_crashes   (lookups into the 3 dims above; counts from people/vehicles)
        5. fact_people    (lookup into fact_crashes for crash_key)
        6. fact_vehicles  (lookup into fact_crashes for crash_key)

Transformations Applied:
    dim_date
        date_key         : YEAR*1000000 + MONTH*10000 + DAY*100 + CRASH_HOUR
        time_of_day      : Morning (06-11) / Afternoon (12-17) / Evening (18-21) / Night

    dim_road
        speed_limit_range: Low (<=25) / Moderate (26-40) / High (41-55) / Very High (>55)

    fact_crashes
        total_people_involved  : COUNT(*) from silver.people  grouped by crash_record_id
        total_vehicles_involved: COUNT(*) from silver.vehicles grouped by crash_record_id

    fact_people
        age_group        : Under 16 / 16-25 / 26-40 / 41-60 / 61+ / Unknown
        crash_key        : looked up from gold.fact_crashes via crash_record_id

    fact_vehicles
        vehicle_year_group: Before 2000 / 2000-2009 / 2010-2019 / 2020+ / Unknown
        crash_key         : looked up from gold.fact_crashes via crash_record_id

Usage:
    EXEC gold.load_gold;
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time      DATETIME,
            @end_time        DATETIME,
            @batch_start     DATETIME,
            @batch_end       DATETIME;

    BEGIN TRY
        SET @batch_start = GETDATE();
        PRINT '===========================================================================';
        PRINT 'Loading Gold Layer';
        PRINT '===========================================================================';


-- ===========================================================================
-- TRUNCATE ORDER: facts first (deepest children), then dims
-- Using TRUNCATE + DBCC CHECKIDENT to reset IDENTITY seeds
-- ===========================================================================
        PRINT '>> Truncating Gold tables...';

        TRUNCATE TABLE gold.fact_vehicles;
        DBCC CHECKIDENT('gold.fact_vehicles', RESEED, 0) WITH NO_INFOMSGS;

        TRUNCATE TABLE gold.fact_people;
        DBCC CHECKIDENT('gold.fact_people',   RESEED, 0) WITH NO_INFOMSGS;

        TRUNCATE TABLE gold.fact_crashes;
        DBCC CHECKIDENT('gold.fact_crashes',  RESEED, 0) WITH NO_INFOMSGS;

        TRUNCATE TABLE gold.dim_date;

        TRUNCATE TABLE gold.dim_weather;
        DBCC CHECKIDENT('gold.dim_weather',   RESEED, 0) WITH NO_INFOMSGS;

        TRUNCATE TABLE gold.dim_road;
        DBCC CHECKIDENT('gold.dim_road',      RESEED, 0) WITH NO_INFOMSGS;

        PRINT '>> All Gold tables truncated.';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 1. dim_date
--    Source  : silver.crashes (CRASH_DATE + CRASH_HOUR)
--    Key     : YEAR*1000000 + MONTH*10000 + DAY*100 + CRASH_HOUR
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.dim_date';

        INSERT INTO gold.dim_date
        (
            date_key,
            full_date,
            [year],
            quarter,
            [month],
            month_name,
            day_of_week,
            day_name,
            crash_hour,
            time_of_day
        )
        SELECT DISTINCT
            -- Surrogate key: YYYYMMDDhh  (e.g. 2020011514 = 15 Jan 2020, 14:00)
            YEAR(CRASH_DATE)  * 1000000
            + MONTH(CRASH_DATE) * 10000
            + DAY(CRASH_DATE)   * 100
            + CRASH_HOUR                                      AS date_key,

            CAST(CRASH_DATE AS DATE)                          AS full_date,
            YEAR(CRASH_DATE)                                  AS [year],
            DATEPART(QUARTER,  CRASH_DATE)                    AS quarter,
            MONTH(CRASH_DATE)                                 AS [month],
            DATENAME(MONTH,    CRASH_DATE)                    AS month_name,
            DATEPART(WEEKDAY,  CRASH_DATE)                    AS day_of_week,
            DATENAME(WEEKDAY,  CRASH_DATE)                    AS day_name,
            CRASH_HOUR                                        AS crash_hour,

            CASE
                WHEN CRASH_HOUR BETWEEN  6 AND 11 THEN 'Morning'
                WHEN CRASH_HOUR BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN CRASH_HOUR BETWEEN 18 AND 21 THEN 'Evening'
                ELSE                                    'Night'
            END                                               AS time_of_day

        FROM silver.crashes
        WHERE CRASH_DATE IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 2. dim_weather
--    Source  : silver.crashes
--    Grain   : distinct (weather_condition, lighting_condition, roadway_surface_cond)
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.dim_weather';

        INSERT INTO gold.dim_weather
        (
            weather_condition,
            lighting_condition,
            roadway_surface_cond
        )
        SELECT DISTINCT
            ISNULL(WEATHER_CONDITION,    'UNKNOWN') AS weather_condition,
            ISNULL(LIGHTING_CONDITION,   'UNKNOWN') AS lighting_condition,
            ISNULL(ROADWAY_SURFACE_COND, 'UNKNOWN') AS roadway_surface_cond
        FROM silver.crashes;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 3. dim_road
--    Source  : silver.crashes
--    Grain   : distinct road-control + speed combo
--    Derived : speed_limit_range bucket
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.dim_road';

        INSERT INTO gold.dim_road
        (
            traffic_control_device,
            device_condition,
            trafficway_type,
            alignment,
            road_defect,
            posted_speed_limit,
            speed_limit_range
        )
        SELECT DISTINCT
            ISNULL(TRAFFIC_CONTROL_DEVICE, 'UNKNOWN') AS traffic_control_device,
            ISNULL(DEVICE_CONDITION,       'UNKNOWN') AS device_condition,
            ISNULL(TRAFFICWAY_TYPE,        'UNKNOWN') AS trafficway_type,
            ISNULL(ALIGNMENT,              'UNKNOWN') AS alignment,
            ISNULL(ROAD_DEFECT,            'UNKNOWN') AS road_defect,
            POSTED_SPEED_LIMIT                        AS posted_speed_limit,

            CASE
                WHEN POSTED_SPEED_LIMIT <= 25              THEN 'Low (<=25)'
                WHEN POSTED_SPEED_LIMIT BETWEEN 26 AND 40  THEN 'Moderate (26-40)'
                WHEN POSTED_SPEED_LIMIT BETWEEN 41 AND 55  THEN 'High (41-55)'
                WHEN POSTED_SPEED_LIMIT > 55               THEN 'Very High (>55)'
                ELSE                                            'Unknown'
            END                                           AS speed_limit_range

        FROM silver.crashes;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 4. fact_crashes
--    Source  : silver.crashes  (main)
--              silver.people   (aggregated count per crash)
--              silver.vehicles (aggregated count per crash)
--    Lookups : gold.dim_date, gold.dim_weather, gold.dim_road
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.fact_crashes';

        -- Pre-aggregate people and vehicle counts to avoid correlated subqueries
        ;WITH people_counts AS (
            SELECT
                CRASH_RECORD_ID,
                COUNT(*) AS total_people
            FROM silver.people
            GROUP BY CRASH_RECORD_ID
        ),
        vehicle_counts AS (
            SELECT
                CRASH_RECORD_ID,
                COUNT(*) AS total_vehicles
            FROM silver.vehicles
            GROUP BY CRASH_RECORD_ID
        )
        INSERT INTO gold.fact_crashes
        (
            crash_record_id,
            date_key,
            weather_key,
            road_key,
            crash_type,
            first_crash_type,
            prim_contributory_cause,
            sec_contributory_cause,
            damage,
            report_type,
            most_severe_injury,
            num_units,
            injuries_total,
            injuries_fatal,
            injuries_incapacitating,
            injuries_non_incapacitating,
            injuries_reported_not_evident,
            injuries_no_indication,
            injuries_unknown,
            total_people_involved,
            total_vehicles_involved
        )
        SELECT
            c.CRASH_RECORD_ID,

            -- Date surrogate key (must match how dim_date was built)
            YEAR(c.CRASH_DATE)  * 1000000
            + MONTH(c.CRASH_DATE) * 10000
            + DAY(c.CRASH_DATE)   * 100
            + c.CRASH_HOUR                                AS date_key,

            -- Weather dimension lookup
            dw.weather_key,

            -- Road dimension lookup
            dr.road_key,

            -- Degenerate dimensions
            c.CRASH_TYPE,
            c.FIRST_CRASH_TYPE,
            c.PRIM_CONTRIBUTORY_CAUSE,
            c.SEC_CONTRIBUTORY_CAUSE,
            c.DAMAGE,
            c.REPORT_TYPE,
            c.MOST_SEVERE_INJURY,

            -- Additive measures
            c.NUM_UNITS,
            c.INJURIES_TOTAL,
            c.INJURIES_FATAL,
            c.INJURIES_INCAPACITATING,
            c.INJURIES_NON_INCAPACITATING,
            c.INJURIES_REPORTED_NOT_EVIDENT,
            c.INJURIES_NO_INDICATION,
            c.INJURIES_UNKNOWN,

            -- Pre-aggregated counts
            ISNULL(pc.total_people,   0) AS total_people_involved,
            ISNULL(vc.total_vehicles, 0) AS total_vehicles_involved

        FROM silver.crashes c

        -- Weather dim: match on all three weather columns
        LEFT JOIN gold.dim_weather dw
            ON  ISNULL(c.WEATHER_CONDITION,    'UNKNOWN') = dw.weather_condition
            AND ISNULL(c.LIGHTING_CONDITION,   'UNKNOWN') = dw.lighting_condition
            AND ISNULL(c.ROADWAY_SURFACE_COND, 'UNKNOWN') = dw.roadway_surface_cond

        -- Road dim: match on all road columns including speed limit
        LEFT JOIN gold.dim_road dr
            ON  ISNULL(c.TRAFFIC_CONTROL_DEVICE, 'UNKNOWN') = dr.traffic_control_device
            AND ISNULL(c.DEVICE_CONDITION,        'UNKNOWN') = dr.device_condition
            AND ISNULL(c.TRAFFICWAY_TYPE,         'UNKNOWN') = dr.trafficway_type
            AND ISNULL(c.ALIGNMENT,               'UNKNOWN') = dr.alignment
            AND ISNULL(c.ROAD_DEFECT,             'UNKNOWN') = dr.road_defect
            AND c.POSTED_SPEED_LIMIT                         = dr.posted_speed_limit

        -- People count per crash
        LEFT JOIN people_counts  pc ON c.CRASH_RECORD_ID = pc.CRASH_RECORD_ID

        -- Vehicle count per crash
        LEFT JOIN vehicle_counts vc ON c.CRASH_RECORD_ID = vc.CRASH_RECORD_ID

        WHERE c.CRASH_DATE IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 5. fact_people
--    Source  : silver.people
--    Lookup  : gold.fact_crashes (for crash_key via crash_record_id)
--    Derived : age_group bucket
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.fact_people';

        INSERT INTO gold.fact_people
        (
            person_id,
            crash_record_id,
            crash_key,
            person_type,
            sex,
            age,
            age_group,
            safety_equipment,
            airbag_deployed,
            ejection,
            injury_classification,
            driver_action,
            driver_vision,
            physical_condition
        )
        SELECT
            p.PERSON_ID,
            p.CRASH_RECORD_ID,

            -- Surrogate key from fact_crashes (NULL if crash not in gold — should not happen)
            fc.crash_key,

            p.PERSON_TYPE,
            p.SEX,
            p.AGE,

            -- Age group bucket
            CASE
                WHEN p.AGE < 16                    THEN 'Under 16'
                WHEN p.AGE BETWEEN 16 AND 25       THEN '16-25'
                WHEN p.AGE BETWEEN 26 AND 40       THEN '26-40'
                WHEN p.AGE BETWEEN 41 AND 60       THEN '41-60'
                WHEN p.AGE > 60                    THEN '61+'
                ELSE                                    'Unknown'
            END                                    AS age_group,

            p.SAFETY_EQUIPMENT,
            p.AIRBAG_DEPLOYED,
            p.EJECTION,
            p.INJURY_CLASSIFICATION,
            p.DRIVER_ACTION,
            p.DRIVER_VISION,
            p.PHYSICAL_CONDITION

        FROM silver.people p
        LEFT JOIN gold.fact_crashes fc ON p.CRASH_RECORD_ID = fc.crash_record_id;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
-- 6. fact_vehicles
--    Source  : silver.vehicles
--    Lookup  : gold.fact_crashes (for crash_key via crash_record_id)
--    Derived : vehicle_year_group bucket
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Inserting Data Into: gold.fact_vehicles';

        INSERT INTO gold.fact_vehicles
        (
            crash_unit_id,
            crash_record_id,
            crash_key,
            unit_type,
            vehicle_type,
            vehicle_use,
            make,
            model,
            vehicle_year,
            vehicle_year_group,
            vehicle_defect,
            maneuver,
            travel_direction,
            first_contact_point,
            exceed_speed_limit
        )
        SELECT
            v.CRASH_UNIT_ID,
            v.CRASH_RECORD_ID,

            -- Surrogate key from fact_crashes
            fc.crash_key,

            v.UNIT_TYPE,
            v.VEHICLE_TYPE,
            v.VEHICLE_USE,
            v.MAKE,
            v.MODEL,
            v.VEHICLE_YEAR,

            -- Vehicle year bucket
            CASE
                WHEN v.VEHICLE_YEAR < 2000                   THEN 'Before 2000'
                WHEN v.VEHICLE_YEAR BETWEEN 2000 AND 2009    THEN '2000-2009'
                WHEN v.VEHICLE_YEAR BETWEEN 2010 AND 2019    THEN '2010-2019'
                WHEN v.VEHICLE_YEAR >= 2020                  THEN '2020+'
                ELSE                                              'Unknown'
            END                                              AS vehicle_year_group,

            v.VEHICLE_DEFECT,
            v.MANEUVER,
            v.TRAVEL_DIRECTION,
            v.FIRST_CONTACT_POINT,
            v.EXCEED_SPEED_LIMIT_I

        FROM silver.vehicles v
        LEFT JOIN gold.fact_crashes fc ON v.CRASH_RECORD_ID = fc.crash_record_id;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -----------------------------------------------------------------';


-- ===========================================================================
        SET @batch_end = GETDATE();
        PRINT '==========================================================================='
        PRINT 'Loading Gold Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS NVARCHAR) + ' seconds';
        PRINT '==========================================================================='
    END TRY

    BEGIN CATCH
        PRINT '==========================================================================='
        PRINT 'ERROR OCCURRED DURING LOADING GOLD LAYER'
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER()  AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE()   AS NVARCHAR);
        PRINT '==========================================================================='
    END CATCH

END;
GO


-- Execute
EXEC gold.load_gold;