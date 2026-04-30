/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'crashes_DWH' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'crashes_DWH' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'crashes_DWH' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'crashes_DWH')
BEGIN
    ALTER DATABASE crashes_DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE crashes_DWH;
END;
GO

-- Create the 'crashes_DWH' database
CREATE DATABASE crashes_DWH;
GO

USE crashes_DWH;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
