/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
        - Truncates Silver tables.
        - Inserts transformed and cleansed data from Bronze into Silver tables.

Transformations Applied:
    - CRASH_DATE / DATE_POLICE_NOTIFIED  : TRY_CAST to DATETIME2
    - VEHICLE_ID, CRASH_UNIT_ID          : TRY_CAST NVARCHAR/FLOAT -> BIGINT
    - VEHICLE_YEAR, AGE                  : TRY_CAST FLOAT -> INT
    - INJURIES_*                         : TRY_CAST NVARCHAR -> FLOAT
    - All string categoricals            : UPPER(TRIM(...)) for consistency
    - Empty strings                      : NULLIF(..., '') -> NULL

Parameters:
    None.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, 
            @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==========================================================================================';
        PRINT 'Loading Silver Layer';
        PRINT '==========================================================================================';


-- ===========================================================================
-- Loading silver.crashes
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crashes';
        TRUNCATE TABLE silver.crashes;

        PRINT '>> Inserting Data Into: silver.crashes';
        INSERT INTO silver.crashes
        (
            CRASH_RECORD_ID,
            CRASH_DATE,
            POSTED_SPEED_LIMIT,
            TRAFFIC_CONTROL_DEVICE,
            DEVICE_CONDITION,
            WEATHER_CONDITION,
            LIGHTING_CONDITION,
            FIRST_CRASH_TYPE,
            TRAFFICWAY_TYPE,
            ALIGNMENT,
            ROADWAY_SURFACE_COND,
            ROAD_DEFECT,
            REPORT_TYPE,
            CRASH_TYPE,
            DAMAGE,
            DATE_POLICE_NOTIFIED,
            PRIM_CONTRIBUTORY_CAUSE,
            SEC_CONTRIBUTORY_CAUSE,
            NUM_UNITS,
            MOST_SEVERE_INJURY,
            INJURIES_TOTAL,
            INJURIES_FATAL,
            INJURIES_INCAPACITATING,
            INJURIES_NON_INCAPACITATING,
            INJURIES_REPORTED_NOT_EVIDENT,
            INJURIES_NO_INDICATION,
            INJURIES_UNKNOWN,
            CRASH_HOUR,
            CRASH_DAY_OF_WEEK,
            CRASH_MONTH
        )
        SELECT
            -- Row Identifier: trim whitespace only
            TRIM(CRASH_RECORD_ID),

            -- Date conversion: Chicago format is 'MM/DD/YYYY HH:MI:SS AM'
            TRY_CAST(CRASH_DATE AS DATETIME2),

            -- Numeric: already INT in bronze, keep as-is
            POSTED_SPEED_LIMIT,

            -- Categorical columns: normalize to uppercase trimmed strings,
            -- empty strings become NULL
            UPPER(TRIM(NULLIF(TRAFFIC_CONTROL_DEVICE,  ''))),
            UPPER(TRIM(NULLIF(DEVICE_CONDITION,         ''))),
            UPPER(TRIM(NULLIF(WEATHER_CONDITION,        ''))),
            UPPER(TRIM(NULLIF(LIGHTING_CONDITION,       ''))),
            UPPER(TRIM(NULLIF(FIRST_CRASH_TYPE,         ''))),
            UPPER(TRIM(NULLIF(TRAFFICWAY_TYPE,          ''))),
            UPPER(TRIM(NULLIF(ALIGNMENT,                ''))),
            UPPER(TRIM(NULLIF(ROADWAY_SURFACE_COND,     ''))),
            UPPER(TRIM(NULLIF(ROAD_DEFECT,              ''))),
            UPPER(TRIM(NULLIF(REPORT_TYPE,              ''))),
            UPPER(TRIM(NULLIF(CRASH_TYPE,               ''))),
            UPPER(TRIM(NULLIF(DAMAGE,                   ''))),

            -- Second date column: same conversion
            TRY_CAST(DATE_POLICE_NOTIFIED AS DATETIME2),

            UPPER(TRIM(NULLIF(PRIM_CONTRIBUTORY_CAUSE,  ''))),
            UPPER(TRIM(NULLIF(SEC_CONTRIBUTORY_CAUSE,   ''))),

            -- Numeric: already INT in bronze
            NUM_UNITS,

            UPPER(TRIM(NULLIF(MOST_SEVERE_INJURY,       ''))),

            -- Injury columns: stored as NVARCHAR in bronze -> cast to FLOAT
            TRY_CAST(INJURIES_TOTAL                 AS FLOAT),
            TRY_CAST(INJURIES_FATAL                 AS FLOAT),
            TRY_CAST(INJURIES_INCAPACITATING        AS FLOAT),
            TRY_CAST(INJURIES_NON_INCAPACITATING    AS FLOAT),
            TRY_CAST(INJURIES_REPORTED_NOT_EVIDENT  AS FLOAT),
            TRY_CAST(INJURIES_NO_INDICATION         AS FLOAT),
            TRY_CAST(INJURIES_UNKNOWN               AS FLOAT),

            -- Time dimensions: already INT in bronze
            CRASH_HOUR,
            CRASH_DAY_OF_WEEK,
            CRASH_MONTH

        FROM bronze.crashes;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------------------------------';


-- ===========================================================================
-- Loading silver.people
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.people';
        TRUNCATE TABLE silver.people;

        PRINT '>> Inserting Data Into: silver.people';
        INSERT INTO silver.people
        (
            PERSON_ID,
            PERSON_TYPE,
            CRASH_RECORD_ID,
            VEHICLE_ID,
            CRASH_DATE,
            SEX,
            AGE,
            SAFETY_EQUIPMENT,
            AIRBAG_DEPLOYED,
            EJECTION,
            INJURY_CLASSIFICATION,
            DRIVER_ACTION,
            DRIVER_VISION,
            PHYSICAL_CONDITION
        )
        SELECT
            -- Row Identifier
            TRIM(PERSON_ID),

            UPPER(TRIM(NULLIF(PERSON_TYPE,   ''))),

            TRIM(CRASH_RECORD_ID),

            -- VEHICLE_ID arrives as FLOAT from CSV/Python -> cast to BIGINT
            TRY_CAST(TRY_CAST(VEHICLE_ID AS FLOAT) AS BIGINT),

            -- Date conversion
            TRY_CAST(CRASH_DATE AS DATETIME2),

            -- SEX: keep X (non-binary/unknown) as a valid value
            UPPER(TRIM(NULLIF(SEX,           ''))),

            -- AGE arrives as FLOAT from Python -> cast to INT
            -- Unrealistic ages become NULL via TRY_CAST naturally,
            -- additional guard: ages outside 0-120 set to NULL
            CASE
				WHEN TRY_CAST(TRY_CAST(AGE AS FLOAT) AS INT) BETWEEN 0 AND 120
				THEN TRY_CAST(TRY_CAST(AGE AS FLOAT) AS INT)
				ELSE NULL
			END,
            
            
            
            

            UPPER(TRIM(NULLIF(SAFETY_EQUIPMENT,      ''))),
            UPPER(TRIM(NULLIF(AIRBAG_DEPLOYED,       ''))),
            UPPER(TRIM(NULLIF(EJECTION,              ''))),
            UPPER(TRIM(NULLIF(INJURY_CLASSIFICATION, ''))),
            UPPER(TRIM(NULLIF(DRIVER_ACTION,         ''))),
            UPPER(TRIM(NULLIF(DRIVER_VISION,         ''))),
            UPPER(TRIM(NULLIF(PHYSICAL_CONDITION,    '')))

        FROM bronze.people;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------------------------------';


-- ===========================================================================
-- Loading silver.vehicles
-- ===========================================================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.vehicles';
        TRUNCATE TABLE silver.vehicles;

        PRINT '>> Inserting Data Into: silver.vehicles';
        INSERT INTO silver.vehicles
        (
            CRASH_UNIT_ID,
            CRASH_RECORD_ID,
            CRASH_DATE,
            UNIT_NO,
            UNIT_TYPE,
            VEHICLE_ID,
            MAKE,
            MODEL,
            VEHICLE_YEAR,
            VEHICLE_DEFECT,
            VEHICLE_TYPE,
            VEHICLE_USE,
            TRAVEL_DIRECTION,
            MANEUVER,
            EXCEED_SPEED_LIMIT_I,
            FIRST_CONTACT_POINT
        )
        SELECT
            -- Row Identifier: INT in source, stored as NVARCHAR in bronze
			TRY_CAST(TRY_CAST(CRASH_UNIT_ID AS FLOAT) AS BIGINT),
            TRIM(CRASH_RECORD_ID),

            -- Date conversion
            TRY_CAST(CRASH_DATE AS DATETIME2),

            TRIM(NULLIF(UNIT_NO,  '')),

            UPPER(TRIM(NULLIF(UNIT_TYPE,  ''))),

            -- VEHICLE_ID arrives as FLOAT -> cast to BIGINT
            TRY_CAST(TRY_CAST(VEHICLE_ID AS FLOAT) AS BIGINT),

            -- MAKE / MODEL: title-cased in source, just trim
            UPPER(TRIM(NULLIF(MAKE,   ''))),
            UPPER(TRIM(NULLIF(MODEL,  ''))),

            -- VEHICLE_YEAR arrives as FLOAT -> INT
            -- Guard: realistic model years only (1885 = first car, cap at current year + 1)
            CASE
				WHEN TRY_CAST(TRY_CAST(VEHICLE_YEAR AS FLOAT) AS INT) BETWEEN 1885 AND 2026
				THEN TRY_CAST(TRY_CAST(VEHICLE_YEAR AS FLOAT) AS INT)
				ELSE NULL
			END,

            UPPER(TRIM(NULLIF(VEHICLE_DEFECT,       ''))),
            UPPER(TRIM(NULLIF(VEHICLE_TYPE,         ''))),
            UPPER(TRIM(NULLIF(VEHICLE_USE,          ''))),
            UPPER(TRIM(NULLIF(TRAVEL_DIRECTION,     ''))),
            UPPER(TRIM(NULLIF(MANEUVER,             ''))),
            UPPER(TRIM(NULLIF(EXCEED_SPEED_LIMIT_I, ''))),
            UPPER(TRIM(NULLIF(FIRST_CONTACT_POINT,  '')))

        FROM bronze.vehicles
        -- Filter rows where UNIT_TYPE is NULL (569 rows in source, no analytical value)
        WHERE UNIT_TYPE IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ---------------------------------------';


-- ===========================================================================
        SET @batch_end_time = GETDATE();
        PRINT '===================================================================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '===================================================================================='
    END TRY

    BEGIN CATCH
        PRINT '===================================================================================='
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER()  AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE()   AS NVARCHAR);
        PRINT '===================================================================================='
    END CATCH

END
GO

-- Execute
EXEC silver.load_silver;