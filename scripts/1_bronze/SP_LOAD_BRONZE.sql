/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==========================================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================================================================';
-------------------------- Loading bronze.crashes --------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crashes';
		TRUNCATE TABLE bronze.crashes;

		PRINT '>> Inserting Data Into: bronze.crashes';
		BULK INSERT bronze.crashes
		
		FROM 'E:\MY_Documents\self_learning\Data_Engineering\ITI\EDA\EDA_project\raw_datasets\crashes_pipe.csv'
		WITH (
			FIRSTROW        = 2,
			FIELDTERMINATOR = '|',
			ROWTERMINATOR   = '\n',
			CODEPAGE        = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------';

-------------------------- Loading bronze.people --------------------------

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.people';
		TRUNCATE TABLE bronze.people;

		PRINT '>> Inserting Data Into: bronze.people';
		BULK INSERT bronze.people

		FROM 'E:\MY_Documents\self_learning\Data_Engineering\ITI\EDA\EDA_project\raw_datasets\people_pipe.csv'
		WITH (
			FIRSTROW        = 2,
			FIELDTERMINATOR = '|',
			ROWTERMINATOR   = '\n',
			CODEPAGE        = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------';

-------------------------- Loading bronze.vehicles --------------------------
-- Loaded via Python script due to encoding complexity in source data
-- See: load_vehicles_bronze.py
PRINT '>> bronze.vehicles loaded via Python script (see load_vehicles_bronze.py)';
/*
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.vehicles';
		TRUNCATE TABLE bronze.vehicles;

		PRINT '>> Inserting Data Into: bronze.vehicles';
		BULK INSERT bronze.vehicles
		
		FROM 'E:\MY_Documents\self_learning\Data_Engineering\ITI\EDA\EDA_project\raw_datasets\vehicles_pipe.csv'
		WITH (
			FIRSTROW        = 2,
			FIELDTERMINATOR = '|',
			ROWTERMINATOR   = '\n',
			CODEPAGE        = '65001',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------';
*/
--
--===================================================================================================

		SET @batch_end_time = GETDATE();
		PRINT '===================================================================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================================================='
	END TRY

	BEGIN CATCH
		PRINT '===================================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================================================='
	END CATCH

END

GO
--
EXEC bronze.load_bronze

