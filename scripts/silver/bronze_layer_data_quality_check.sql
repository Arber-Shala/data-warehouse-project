USE DataWarehouse;
-- find primary id duplicates or NULLs
SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- check for unwanted spaces
-- expectation: no results
SELECT 
	cst_id,
	cst_key,
    cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- check for data standarization and consistency
SELECT DISTINCT cst_gndr -- check all possible values for cst_gndr
FROM bronze.crm_cust_info;

--========================================================================================
-- bronze.crm_prd_info
--========================================================================================
-- check if prd_nm has unwanted spaces 
SELECT
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- check for NULLs or negative numbers in the cost column
SELECT 
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check data standardization and consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- check for invalid order dates
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt -- start and end date are flipped

--========================================================================================
-- bronze.crm_sales_details
--========================================================================================
-- check if the foreign keys all connect to the other table
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- check invalid dates
-- cant have dates <= 0
-- cant have dates with more or less than 8 digits
SELECT
	sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0  OR LEN(sls_order_dt) != 8

-- order date must always be earlier than shipping date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- sales = quantity * price
-- sales, quantity, and price CANNOT be <= 0
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- check fixes to price
SELECT DISTINCT
    sls_sales AS old_sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE --  sales = quantity * price; if sales is <= 0 or NULL, derive is using quantity and price
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE -- if price = 0 or NULL, derive it using quantity and price, if price < 0, turn is positive
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) -- prevent divide by zero
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
		OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
		OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--========================================================================================
-- bronze.erp_CUST_AZ12
--========================================================================================
-- check if birthdate is out of range
SELECT DISTINCT
	bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- check data standardization and consistency
SELECT DISTINCT gen
FROM bronze.erp_CUST_AZ12

--========================================================================================
-- bronze.erp_LOC_A101
--========================================================================================
-- check if key matches primey key of crm_cust_info
-- expectation: returns nothing
SELECT
	cid,
	cntry
FROM bronze.erp_LOC_A101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- standardization and consistency
SELECT DISTINCT cntry
FROM bronze.erp_LOC_A101
ORDER BY cntry

--========================================================================================
-- bronze.erp_PX_CAT_G1V2
--========================================================================================
-- check if foreign key matches primary key of crm_prd_info
SELECT
	id -- primary key of crm_prd_info
FROM bronze.erp_PX_CAT_G1V2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)

-- check for unwanted spaces
SELECT *
FROM bronze.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- data standardization and consistency
SELECT DISTINCT 
	cat
FROM bronze.erp_PX_CAT_G1V2

SELECT DISTINCT 
	subcat
FROM bronze.erp_PX_CAT_G1V2

SELECT DISTINCT 
	maintenance
FROM bronze.erp_PX_CAT_G1V2
