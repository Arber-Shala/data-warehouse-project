USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT '============================';
		PRINT 'Loading Bronze Layer...';
		PRINT '============================';
		SET @batch_start_time = GETDATE();

		PRINT '----------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------';
		-- Calculate the duration of loading tables
		SET @start_time = GETDATE();

		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.crm_cust_info;
		-- Insert CSV file info into table
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);

		-- Test quality of table information
		/*
		SELECT * -- check if each column has data in the right place
		FROM bronze.crm_cust_info;

		SELECT COUNT(*) -- count number of rows to see if it matches CSV file rows
		FROM bronze.crm_cust_info;
		*/

		-- ==============================================================================================================
		-- load info for all other files
		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.crm_prd_info;
		-- Insert CSV file info into table
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);

		-- ==============================================================================================================
		-- load info for all other files
		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.crm_sales_details;
		-- Insert CSV file info into table
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '---------------------------------------------------------------------------'
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------------------------'
		-- ==============================================================================================================
		PRINT '----------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------';
		SET @start_time = GETDATE();

		-- load info for all other files
		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.erp_CUST_AZ12;
		-- Insert CSV file info into table
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);
		-- ==============================================================================================================
		-- load info for all other files
		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.erp_LOC_A101;
		-- Insert CSV file info into table
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);
		-- ==============================================================================================================
		-- load info for all other files
		-- make table empty before loading content to it (full load)
		TRUNCATE TABLE  bronze.erp_PX_CAT_G1V2;
		-- Insert CSV file info into table
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\Arber\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH  (
			FIRSTROW = 2, -- skip the header row in the CSV file
			FIELDTERMINATOR = ',', -- tell SQL what delimeter separates data in the CSV file
			TABLOCK
		);
		
		SET @end_time = GETDATE()
		PRINT '---------------------------------------------------------------------------'
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '---------------------------------------------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '>> Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

	END TRY
	BEGIN CATCH 
		PRINT '===============================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '===============================';
	END CATCH
END
