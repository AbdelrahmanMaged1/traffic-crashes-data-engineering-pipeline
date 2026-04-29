/*
===============================================================================
DDL Script: Create Silver Tables (v2 - Cleaned)
===============================================================================
Changes from Bronze → Silver:
  - Dropped columns with excessive nulls or no analytical value
  - Applied correct data types (DATETIME2, INT, FLOAT)
  - Retained only columns relevant to analysis
===============================================================================
*/


-- ============================================================
-- silver.crashes
-- ============================================================
-- DROPPED: CRASH_DATE_EST_I, LANE_CNT, INTERSECTION_RELATED_I,
--          NOT_RIGHT_OF_WAY_I, HIT_AND_RUN_I, PHOTOS_TAKEN_I,
--          STATEMENTS_TAKEN_I, DOORING_I, WORK_ZONE_I, WORK_ZONE_TYPE,
--          WORKERS_PRESENT_I, STREET_NO, STREET_DIRECTION, STREET_NAME,
--          BEAT_OF_OCCURRENCE, LATITUDE, LONGITUDE, LOCATION

IF OBJECT_ID('silver.crashes', 'U') IS NOT NULL
    DROP TABLE silver.crashes;
GO

CREATE TABLE silver.crashes (

    CRASH_RECORD_ID                NVARCHAR(255)    NOT NULL,   -- Row Identifier
    CRASH_DATE                     DATETIME2       NULL,       -- Converted from NVARCHAR
    POSTED_SPEED_LIMIT             INT             NULL,
    TRAFFIC_CONTROL_DEVICE         NVARCHAR(100)   NULL,
    DEVICE_CONDITION               NVARCHAR(100)   NULL,
    WEATHER_CONDITION              NVARCHAR(100)   NULL,
    LIGHTING_CONDITION             NVARCHAR(100)   NULL,
    FIRST_CRASH_TYPE               NVARCHAR(100)   NULL,
    TRAFFICWAY_TYPE                NVARCHAR(100)   NULL,
    ALIGNMENT                      NVARCHAR(100)   NULL,
    ROADWAY_SURFACE_COND           NVARCHAR(100)   NULL,
    ROAD_DEFECT                    NVARCHAR(100)   NULL,
    REPORT_TYPE                    NVARCHAR(100)   NULL,
    CRASH_TYPE                     NVARCHAR(100)   NULL,
    DAMAGE                         NVARCHAR(100)   NULL,
    DATE_POLICE_NOTIFIED           DATETIME2       NULL,       -- Converted from NVARCHAR
    PRIM_CONTRIBUTORY_CAUSE        NVARCHAR(100)   NULL,
    SEC_CONTRIBUTORY_CAUSE         NVARCHAR(100)   NULL,
    NUM_UNITS                      INT             NULL,
    MOST_SEVERE_INJURY             NVARCHAR(100)   NULL,
    INJURIES_TOTAL                 FLOAT           NULL,
    INJURIES_FATAL                 FLOAT           NULL,
    INJURIES_INCAPACITATING        FLOAT           NULL,
    INJURIES_NON_INCAPACITATING    FLOAT           NULL,
    INJURIES_REPORTED_NOT_EVIDENT  FLOAT           NULL,
    INJURIES_NO_INDICATION         FLOAT           NULL,
    INJURIES_UNKNOWN               FLOAT           NULL,
    CRASH_HOUR                     INT             NULL,
    CRASH_DAY_OF_WEEK              INT             NULL,
    CRASH_MONTH                    INT             NULL

);
GO

-- ============================================================
-- silver.people
-- ============================================================
-- DROPPED: SEAT_NO, CITY, STATE, ZIPCODE, DRIVERS_LICENSE_STATE,
--          DRIVERS_LICENSE_CLASS, HOSPITAL, EMS_AGENCY, EMS_RUN_NO,
--          PEDPEDAL_ACTION, PEDPEDAL_VISIBILITY, PEDPEDAL_LOCATION,
--          BAC_RESULT, BAC_RESULT VALUE, CELL_PHONE_USE

IF OBJECT_ID('silver.people', 'U') IS NOT NULL
    DROP TABLE silver.people;
GO

CREATE TABLE silver.people (

    PERSON_ID              NVARCHAR(255)    NOT NULL,   -- Row Identifier
    PERSON_TYPE            NVARCHAR(100)   NULL,
    CRASH_RECORD_ID        NVARCHAR(255)    NOT NULL,
    VEHICLE_ID             BIGINT             NULL,       -- Converted from FLOAT
    CRASH_DATE             DATETIME2       NULL,       -- Converted from NVARCHAR
    SEX                    NVARCHAR(10)    NULL,
    AGE                    INT             NULL,       -- Converted from FLOAT
    SAFETY_EQUIPMENT       NVARCHAR(100)   NULL,
    AIRBAG_DEPLOYED        NVARCHAR(100)   NULL,
    EJECTION               NVARCHAR(100)   NULL,
    INJURY_CLASSIFICATION  NVARCHAR(100)   NULL,
    DRIVER_ACTION          NVARCHAR(100)   NULL,
    DRIVER_VISION          NVARCHAR(100)   NULL,
    PHYSICAL_CONDITION     NVARCHAR(100)   NULL

);
GO


-- ============================================================
-- silver.vehicles
-- ============================================================
-- DROPPED: NUM_PASSENGERS, CMRC_VEH_I, LIC_PLATE_STATE, TOWED_I,
--          FIRE_I, OCCUPANT_CNT, TOWED_BY, TOWED_TO,
--          AREA_00_I through AREA_99_I (all damage area flags),
--          CMV_ID, USDOT_NO, CCMC_NO, ILCC_NO, COMMERCIAL_SRC,
--          GVWR, CARRIER_NAME, CARRIER_STATE, CARRIER_CITY,
--          HAZMAT_* (all hazmat columns), MCS_* (all MCS columns),
--          IDOT_PERMIT_NO, WIDE_LOAD_I, TRAILER1_WIDTH, TRAILER2_WIDTH,
--          TRAILER1_LENGTH, TRAILER2_LENGTH, TOTAL_VEHICLE_LENGTH,
--          AXLE_CNT, VEHICLE_CONFIG, CARGO_BODY_TYPE, LOAD_TYPE

IF OBJECT_ID('silver.vehicles', 'U') IS NOT NULL
    DROP TABLE silver.vehicles;
GO

CREATE TABLE silver.vehicles (

    CRASH_UNIT_ID			BIGINT             NOT NULL,   -- Row Identifier (was NVARCHAR in bronze)
    CRASH_RECORD_ID			NVARCHAR(255)    NOT NULL,
    CRASH_DATE				DATETIME2       NULL,       -- Converted from NVARCHAR
    UNIT_NO					NVARCHAR(100)    NULL,
    UNIT_TYPE				NVARCHAR(100)   NULL,
    VEHICLE_ID				BIGINT             NULL,       -- Converted from FLOAT
    MAKE					NVARCHAR(100)   NULL,
    MODEL					NVARCHAR(100)   NULL,
    VEHICLE_YEAR			INT             NULL,       -- Converted from FLOAT
    VEHICLE_DEFECT			NVARCHAR(100)   NULL,
    VEHICLE_TYPE			NVARCHAR(100)   NULL,
    VEHICLE_USE				NVARCHAR(100)   NULL,
    TRAVEL_DIRECTION		NVARCHAR(50)    NULL,
    MANEUVER				NVARCHAR(100)   NULL,
    EXCEED_SPEED_LIMIT_I	 NVARCHAR(10)  NULL,
    FIRST_CONTACT_POINT		NVARCHAR(100)  NULL

);
GO

