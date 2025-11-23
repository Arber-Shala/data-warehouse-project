-- Stored Procedure for insering cleaned data into the silver layer

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE
	@batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================================================='
		PRINT 'Loading Silver Layer'
		PRINT '=========================================================================================='
		--========================================================================================================================
		/* 
		crm_cust_info cleaning
		Data Transformations done in this script:
		-- get only the most recent primary key if there are duplicates (find cuplicates using ROW_NUMBER() rank window function)
		-- remove leading and following spaces
		-- use full version of genders
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.crm_cust_info;;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE	
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_maritial_status,
			CASE	
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM
			(
			SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last -- most recent data is ranked highest
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL -- do NOT include nulls in the flag calculation
			) AS t 
		WHERE flag_last = 1;
		--========================================================================================================================
		/*
		crm_prd_info cleaning
		Data Transformations done:
		-- Derive New Column: split the prd_key into a new column called cat_id with only first 5 values to be uses to link the erp_px_cat_g1v2 table
		-- replace '-' with '_' to match row values of primary key of erp_px_cat_g1v2
		-- get last part of prd_key code to link table with crm_sales_details
		-- turn abbreviations of prd_line rows into their full names
		-- fix prd_end_dt by using the next prd_start_dt - 1 day as the end date
		-- need to cast prd_start_dt to DATETIME in toder to do -1 day, then cast whole thing back to DATE
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.crm_prd_info;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- get first part of prd_id as anohter id
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- get last few values of prd_key to create a foreign key to crm_sales_details
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost, -- replace NULL values with zero
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sale'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			-- create new dates where all values of prd_end_dt are replaced by the next prd_start_dt record
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(CAST(prd_start_dt AS DATETIME)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS DATE) AS prd_end_dt -- use the next start date as the end date, subtract by 1 to get previous day
		FROM bronze.crm_prd_info;
		--========================================================================================================================
		/* 
		crm_sales_details cleaning
		Data Transformations done:
		-- turn dates from INT to DATE datatypes
		-- turn dates = 0 to NULL
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.crm_sales_details;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity, 
			sls_price
		)

		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE -- turn the date from INT to DATE and turn invalid values to NULL
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE -- turn the date from INT to DATE and turn invalid values to NULL
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE -- turn the date from INT to DATE and turn invalid values to NULL
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE --  sales = quantity * price; if sales is <= 0 or NULL, derive is using quantity and price
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE -- if price is <= 0  or NULL, derive it using sales and quantity; if price is negative, turn it positive
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) -- prevent divide by zero
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;

		--========================================================================================================================
		/* 
		erp_CUST_AZ12 cleaning
		Data Transformations done:
		-- remove NAS from cid to match key of other table
		-- make sure there are no birthdays past the current date
		-- turn abbreviations in the gender column into the full name
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.erp_CUST_AZ12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- remove 'NAS' from start of cid
				ELSE cid
			END AS cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_CUST_AZ12;

		--========================================================================================================================
		/* 
		erp_LOC_A101 cleaning
		Data Transformations done:
		-- remove '-' from cid to match primary key of crm_cust_info
		-- fix abbreviations of cntry column into their full name and fix invalid values
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.erp_LOC_A101;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.erp_LOC_A101(
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry
		FROM bronze.erp_LOC_A101;

		--========================================================================================================================
		/* 
		erp_CAT_G1V2 cleaning
		Data Transformations done:
		-- NONE :D
		*/
		PRINT '>> Truncating Table';
		TRUNCATE TABLE  silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data ...';
		INSERT INTO silver.erp_PX_CAT_G1V2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat, 
			subcat,
			maintenance
		FROM bronze.erp_PX_CAT_G1V2;

		SET @batch_end_time = GETDATE();

		PRINT 'TOTAL DURATION OF SCRIPT: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' SECONDS';

	END TRY
	BEGIN CATCH
		PRINT 'AN ERROR HAS OCCURED'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
		PRINT 'ERROR NUMBER: ' + ERROR_NUMBER()
		PRINT 'ERROR STATE: ' + ERROR_STATE()
	END CATCH
END
