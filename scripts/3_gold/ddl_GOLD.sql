/*
===============================================================================
DDL Script: Create Gold Layer Tables (Star Schema)
===============================================================================
Architecture  : Medallion  →  Gold Layer
Source        : silver.crashes | silver.people | silver.vehicles
Schema        : gold

Tables Created:
    Dimensions
    ----------
    gold.dim_date       – Date + hour grain; time-of-day bucket
    gold.dim_weather    – Weather / lighting / road surface combos
    gold.dim_road       – Road control / type / defect / speed combos

    Facts
    -----
    gold.fact_crashes   – One row per crash (main fact, all injury metrics)
    gold.fact_people    – One row per person per crash (person-level detail)
    gold.fact_vehicles  – One row per vehicle per crash (vehicle-level detail)

Notes:
    - FK relationships are documented in comments but NOT enforced as
      constraints; this is standard DWH practice for load performance.
    - Surrogate keys use IDENTITY(1,1) on all dimension and fact tables
      except dim_date, whose key is a computed integer (YYYYMMDDhh).
    - Drop order: fact_vehicles → fact_people → fact_crashes → dims
===============================================================================
*/


-- ============================================================
-- 0. Create schema if it does not exist
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO


-- ============================================================
-- DROP ORDER: facts first, then dims (respects logical FK order)
-- ============================================================
IF OBJECT_ID('gold.fact_vehicles', 'U') IS NOT NULL DROP TABLE gold.fact_vehicles;
IF OBJECT_ID('gold.fact_people',   'U') IS NOT NULL DROP TABLE gold.fact_people;
IF OBJECT_ID('gold.fact_crashes',  'U') IS NOT NULL DROP TABLE gold.fact_crashes;
IF OBJECT_ID('gold.dim_road',      'U') IS NOT NULL DROP TABLE gold.dim_road;
IF OBJECT_ID('gold.dim_weather',   'U') IS NOT NULL DROP TABLE gold.dim_weather;
IF OBJECT_ID('gold.dim_date',      'U') IS NOT NULL DROP TABLE gold.dim_date;
GO


-- ============================================================
-- 1. dim_date
--    Grain  : one row per unique (date + crash_hour) combination
--    Key    : date_key = YYYY * 1000000 + MM * 10000 + DD * 100 + HH
--             e.g. 2020011514  →  15 Jan 2020, 14:00
-- ============================================================
CREATE TABLE gold.dim_date (
    date_key     INT           NOT NULL,   -- Surrogate key (computed, see above)
    full_date    DATE          NULL,
    [year]       INT           NULL,
    quarter      INT           NULL,
    [month]      INT           NULL,
    month_name   NVARCHAR(20)  NULL,
    day_of_week  INT           NULL,       -- 1 = Sunday … 7 = Saturday
    day_name     NVARCHAR(20)  NULL,
    crash_hour   INT           NULL,       -- 0–23
    time_of_day  NVARCHAR(20)  NULL,       -- Morning / Afternoon / Evening / Night

    CONSTRAINT PK_dim_date PRIMARY KEY (date_key)
);
GO


-- ============================================================
-- 2. dim_weather
--    Grain  : one row per unique combination of
--             (weather_condition, lighting_condition, roadway_surface_cond)
-- ============================================================
CREATE TABLE gold.dim_weather (
    weather_key          INT           NOT NULL IDENTITY(1,1),
    weather_condition    NVARCHAR(100) NULL,
    lighting_condition   NVARCHAR(100) NULL,
    roadway_surface_cond NVARCHAR(100) NULL,

    CONSTRAINT PK_dim_weather PRIMARY KEY (weather_key)
);
GO


-- ============================================================
-- 3. dim_road
--    Grain  : one row per unique combination of road-control attributes
--             + speed limit bucket
-- ============================================================
CREATE TABLE gold.dim_road (
    road_key               INT           NOT NULL IDENTITY(1,1),
    traffic_control_device NVARCHAR(100) NULL,
    device_condition       NVARCHAR(100) NULL,
    trafficway_type        NVARCHAR(100) NULL,
    alignment              NVARCHAR(100) NULL,
    road_defect            NVARCHAR(100) NULL,
    posted_speed_limit     INT           NULL,
    speed_limit_range      NVARCHAR(50)  NULL,   -- Derived: Low / Moderate / High / Very High

    CONSTRAINT PK_dim_road PRIMARY KEY (road_key)
);
GO


-- ============================================================
-- 4. fact_crashes
--    Grain  : one row per crash (CRASH_RECORD_ID)
--    FKs    : date_key → dim_date
--             weather_key → dim_weather
--             road_key    → dim_road
-- ============================================================
CREATE TABLE gold.fact_crashes (
    crash_key                     INT           NOT NULL IDENTITY(1,1),
    crash_record_id               NVARCHAR(255) NOT NULL,   -- Natural key (from silver)

    -- Dimension foreign keys
    date_key                      INT           NULL,       -- → gold.dim_date
    weather_key                   INT           NULL,       -- → gold.dim_weather
    road_key                      INT           NULL,       -- → gold.dim_road

    -- Degenerate dimensions (low-cardinality, no separate dim needed)
    crash_type                    NVARCHAR(100) NULL,
    first_crash_type              NVARCHAR(100) NULL,
    prim_contributory_cause       NVARCHAR(255) NULL,
    sec_contributory_cause        NVARCHAR(255) NULL,
    damage                        NVARCHAR(100) NULL,
    report_type                   NVARCHAR(100) NULL,
    most_severe_injury            NVARCHAR(100) NULL,

    -- Additive measures
    num_units                     INT           NULL,
    injuries_total                FLOAT         NULL,
    injuries_fatal                FLOAT         NULL,
    injuries_incapacitating       FLOAT         NULL,
    injuries_non_incapacitating   FLOAT         NULL,
    injuries_reported_not_evident FLOAT         NULL,
    injuries_no_indication        FLOAT         NULL,
    injuries_unknown              FLOAT         NULL,

    -- Pre-aggregated counts from people / vehicles tables
    total_people_involved         INT           NULL,
    total_vehicles_involved       INT           NULL,

    CONSTRAINT PK_fact_crashes PRIMARY KEY (crash_key)
);
GO


-- ============================================================
-- 5. fact_people
--    Grain  : one row per person per crash
--    FKs    : crash_key → fact_crashes
-- ============================================================
CREATE TABLE gold.fact_people (
    person_key            INT           NOT NULL IDENTITY(1,1),
    person_id             NVARCHAR(255) NOT NULL,   -- Natural key
    crash_record_id       NVARCHAR(255) NOT NULL,
    crash_key             INT           NULL,        -- → gold.fact_crashes

    -- Person attributes
    person_type           NVARCHAR(100) NULL,
    sex                   NVARCHAR(10)  NULL,
    age                   INT           NULL,
    age_group             NVARCHAR(50)  NULL,        -- Derived bucket

    -- Safety / injury
    safety_equipment      NVARCHAR(100) NULL,
    airbag_deployed       NVARCHAR(100) NULL,
    ejection              NVARCHAR(100) NULL,
    injury_classification NVARCHAR(100) NULL,

    -- Driver behaviour
    driver_action         NVARCHAR(100) NULL,
    driver_vision         NVARCHAR(100) NULL,
    physical_condition    NVARCHAR(100) NULL,

    CONSTRAINT PK_fact_people PRIMARY KEY (person_key)
);
GO


-- ============================================================
-- 6. fact_vehicles
--    Grain  : one row per vehicle unit per crash
--    FKs    : crash_key → fact_crashes
-- ============================================================
CREATE TABLE gold.fact_vehicles (
    vehicle_fact_key    INT           NOT NULL IDENTITY(1,1),
    crash_unit_id       BIGINT        NOT NULL,   -- Natural key
    crash_record_id     NVARCHAR(255) NOT NULL,
    crash_key           INT           NULL,        -- → gold.fact_crashes

    -- Vehicle classification
    unit_type           NVARCHAR(100) NULL,
    vehicle_type        NVARCHAR(100) NULL,
    vehicle_use         NVARCHAR(100) NULL,
    make                NVARCHAR(100) NULL,
    model               NVARCHAR(100) NULL,
    vehicle_year        INT           NULL,
    vehicle_year_group  NVARCHAR(50)  NULL,        -- Derived bucket

    -- Condition / behaviour
    vehicle_defect      NVARCHAR(100) NULL,
    maneuver            NVARCHAR(100) NULL,
    travel_direction    NVARCHAR(50)  NULL,
    first_contact_point NVARCHAR(100) NULL,
    exceed_speed_limit  NVARCHAR(10)  NULL,

    CONSTRAINT PK_fact_vehicles PRIMARY KEY (vehicle_fact_key)
);
GO