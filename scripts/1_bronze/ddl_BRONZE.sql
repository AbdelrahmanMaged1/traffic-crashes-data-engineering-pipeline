/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

------------------------> Source tables
-- ============================================================
-- bronze.crashes
-- ============================================================

IF OBJECT_ID('bronze.crashes', 'U') IS NOT NULL
    DROP TABLE bronze.crashes;
GO

CREATE TABLE bronze.crashes (

CRASH_RECORD_ID                NVARCHAR(MAX) ,	----> Row Identifier
CRASH_DATE_EST_I               NVARCHAR(100) ,
CRASH_DATE                     NVARCHAR(100) ,
POSTED_SPEED_LIMIT             INT  ,
TRAFFIC_CONTROL_DEVICE         NVARCHAR(100) ,
DEVICE_CONDITION               NVARCHAR(100) ,
WEATHER_CONDITION              NVARCHAR(100) ,
LIGHTING_CONDITION             NVARCHAR(100) ,
FIRST_CRASH_TYPE               NVARCHAR(100) ,
TRAFFICWAY_TYPE                NVARCHAR(100) ,
LANE_CNT                       NVARCHAR(100),
ALIGNMENT                      NVARCHAR(100) ,
ROADWAY_SURFACE_COND           NVARCHAR(100) ,
ROAD_DEFECT                    NVARCHAR(100) ,
REPORT_TYPE                    NVARCHAR(100) ,
CRASH_TYPE                     NVARCHAR(100) ,
INTERSECTION_RELATED_I         NVARCHAR(100) ,
NOT_RIGHT_OF_WAY_I             NVARCHAR(100) ,
HIT_AND_RUN_I                  NVARCHAR(100) ,
DAMAGE                         NVARCHAR(100) ,
DATE_POLICE_NOTIFIED           NVARCHAR(100) ,
PRIM_CONTRIBUTORY_CAUSE        NVARCHAR(100) ,
SEC_CONTRIBUTORY_CAUSE         NVARCHAR(100) ,
STREET_NO                      INT  ,
STREET_DIRECTION               NVARCHAR(100) ,
STREET_NAME                    NVARCHAR(100) ,
BEAT_OF_OCCURRENCE             NVARCHAR(100),
PHOTOS_TAKEN_I                 NVARCHAR(100) ,
STATEMENTS_TAKEN_I             NVARCHAR(100) ,
DOORING_I                      NVARCHAR(100) ,
WORK_ZONE_I                    NVARCHAR(100) ,
WORK_ZONE_TYPE                 NVARCHAR(100) ,
WORKERS_PRESENT_I              NVARCHAR(100) ,
NUM_UNITS                      INT  ,
MOST_SEVERE_INJURY             NVARCHAR(100) ,
INJURIES_TOTAL                 NVARCHAR(100),
INJURIES_FATAL                 NVARCHAR(100),
INJURIES_INCAPACITATING        NVARCHAR(100),
INJURIES_NON_INCAPACITATING    NVARCHAR(100),
INJURIES_REPORTED_NOT_EVIDENT  NVARCHAR(100),
INJURIES_NO_INDICATION         NVARCHAR(100),
INJURIES_UNKNOWN               NVARCHAR(100),
CRASH_HOUR                     INT  ,
CRASH_DAY_OF_WEEK              INT  ,
CRASH_MONTH                    INT  ,
LATITUDE                       NVARCHAR(MAX),
LONGITUDE                      NVARCHAR(MAX),
LOCATION                       NVARCHAR(MAX) 

);
GO

-- ============================================================
-- bronze.vehicles
-- ============================================================

IF OBJECT_ID('bronze.vehicles', 'U') IS NOT NULL
    DROP TABLE bronze.vehicles;
GO

CREATE TABLE bronze.vehicles (
CRASH_UNIT_ID             NVARCHAR(MAX)  ,		----> Row Identifier 
CRASH_RECORD_ID           NVARCHAR(MAX) ,
CRASH_DATE                NVARCHAR(255) ,
UNIT_NO                   NVARCHAR(MAX)  ,
UNIT_TYPE                 NVARCHAR(255) ,
NUM_PASSENGERS            NVARCHAR(255),
VEHICLE_ID                NVARCHAR(255),
CMRC_VEH_I                NVARCHAR(255) ,
MAKE                      NVARCHAR(255) ,
MODEL                     NVARCHAR(255) ,
LIC_PLATE_STATE           NVARCHAR(255) ,
VEHICLE_YEAR              NVARCHAR(255),
VEHICLE_DEFECT            NVARCHAR(255) ,
VEHICLE_TYPE              NVARCHAR(255) ,
VEHICLE_USE               NVARCHAR(255) ,
TRAVEL_DIRECTION          NVARCHAR(255) ,
MANEUVER                  NVARCHAR(255) ,
TOWED_I                   NVARCHAR(255) ,
FIRE_I                    NVARCHAR(255) ,
OCCUPANT_CNT              NVARCHAR(255),
EXCEED_SPEED_LIMIT_I      NVARCHAR(255) ,
TOWED_BY                  NVARCHAR(255) ,
TOWED_TO                  NVARCHAR(255) ,
AREA_00_I                 NVARCHAR(255) ,
AREA_01_I                 NVARCHAR(255) ,
AREA_02_I                 NVARCHAR(255) ,
AREA_03_I                 NVARCHAR(255) ,
AREA_04_I                 NVARCHAR(255) ,
AREA_05_I                 NVARCHAR(255) ,
AREA_06_I                 NVARCHAR(255) ,
AREA_07_I                 NVARCHAR(255) ,
AREA_08_I                 NVARCHAR(255) ,
AREA_09_I                 NVARCHAR(255) ,
AREA_10_I                 NVARCHAR(255) ,
AREA_11_I                 NVARCHAR(255) ,
AREA_12_I                 NVARCHAR(255) ,
AREA_99_I                 NVARCHAR(255) ,
FIRST_CONTACT_POINT       NVARCHAR(255) ,
CMV_ID                    NVARCHAR(255) ,
USDOT_NO                  NVARCHAR(255) ,
CCMC_NO                   NVARCHAR(255) ,
ILCC_NO                   NVARCHAR(255) ,
COMMERCIAL_SRC            NVARCHAR(255) ,
GVWR                      NVARCHAR(255) ,
CARRIER_NAME              NVARCHAR(255) ,
CARRIER_STATE             NVARCHAR(255) ,
CARRIER_CITY              NVARCHAR(255) ,
HAZMAT_PLACARDS_I         NVARCHAR(255) ,
HAZMAT_NAME               NVARCHAR(255) ,
UN_NO                     NVARCHAR(255) ,
HAZMAT_PRESENT_I          NVARCHAR(255) ,
HAZMAT_REPORT_I           NVARCHAR(255) ,
HAZMAT_REPORT_NO          NVARCHAR(255),
MCS_REPORT_I              NVARCHAR(255) ,
MCS_REPORT_NO             NVARCHAR(255) ,
HAZMAT_VIO_CAUSE_CRASH_I  NVARCHAR(255) ,
MCS_VIO_CAUSE_CRASH_I     NVARCHAR(255) ,
IDOT_PERMIT_NO            NVARCHAR(255) ,
WIDE_LOAD_I               NVARCHAR(255) ,
TRAILER1_WIDTH            NVARCHAR(255) ,
TRAILER2_WIDTH            NVARCHAR(255) ,
TRAILER1_LENGTH           NVARCHAR(255),
TRAILER2_LENGTH           NVARCHAR(255),
TOTAL_VEHICLE_LENGTH      NVARCHAR(255),
AXLE_CNT                  NVARCHAR(255),
VEHICLE_CONFIG            NVARCHAR(255) ,
CARGO_BODY_TYPE           NVARCHAR(255) ,
LOAD_TYPE                 NVARCHAR(255) ,
HAZMAT_OUT_OF_SERVICE_I   NVARCHAR(255) ,
MCS_OUT_OF_SERVICE_I      NVARCHAR(255) ,
HAZMAT_CLASS              NVARCHAR(255) 


);
GO


-- ============================================================
-- bronze.people
-- ============================================================
IF OBJECT_ID('bronze.people', 'U') IS NOT NULL
    DROP TABLE bronze.people;
GO

CREATE TABLE bronze.people (

PERSON_ID              NVARCHAR(255) ,	----> Row Identifier
PERSON_TYPE            NVARCHAR(255) ,
CRASH_RECORD_ID        NVARCHAR(MAX) ,
VEHICLE_ID             NVARCHAR(255),
CRASH_DATE             NVARCHAR(255) ,
SEAT_NO                NVARCHAR(255),
CITY                   NVARCHAR(255) ,
STATE                  NVARCHAR(255) ,
ZIPCODE                NVARCHAR(255) ,
SEX                    NVARCHAR(255) ,
AGE                    NVARCHAR(255),
DRIVERS_LICENSE_STATE  NVARCHAR(255) ,
DRIVERS_LICENSE_CLASS  NVARCHAR(255) ,
SAFETY_EQUIPMENT       NVARCHAR(255) ,
AIRBAG_DEPLOYED        NVARCHAR(255) ,
EJECTION               NVARCHAR(255) ,
INJURY_CLASSIFICATION  NVARCHAR(255) ,
HOSPITAL               NVARCHAR(255) ,
EMS_AGENCY             NVARCHAR(255) ,
EMS_RUN_NO             NVARCHAR(255) ,
DRIVER_ACTION          NVARCHAR(255) ,
DRIVER_VISION          NVARCHAR(255) ,
PHYSICAL_CONDITION     NVARCHAR(255) ,
PEDPEDAL_ACTION        NVARCHAR(255) ,
PEDPEDAL_VISIBILITY    NVARCHAR(255) ,
PEDPEDAL_LOCATION      NVARCHAR(255) ,
BAC_RESULT             NVARCHAR(255) ,
BAC_RESULT_VALUE       NVARCHAR(255),
CELL_PHONE_USE         NVARCHAR(255) 


);
GO
