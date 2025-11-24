--==============================================================================================
/* CUSTOMER [DIMENSION TABLE] <- contains qualitative data
Changes made: 
-- collect all CUSTOMER information from all source system
-- this is a DIMENSION TABLE **
-- fix data itnegrations if the same data is found in multiple tables (eg. gender)
-- make better names for business users
-- group relevant data together
-- create surrogate key as a primary key for the business layer
-- create a view with the joined table
*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- create surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS maritial_status,
	CASE -- only use ca.gen when cst_gnder is not available
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON ci.cst_key = la.cid

--==============================================================================================
/* PRODUCT [DIMENSION TABLE] <- contains qualitative data
Changes made:
-- remove historical information (only use data with prd_end_dt = NULL)
-- group relevant information together
-- create better names
-- build the view
*/
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filter out all historical data

--==============================================================================================
/* SALES [FACT TABLE] <- contrains quantitative data
Changes made:
-- ** FACT TABLES ARE CONSIDERED MAIN TABLE TO DIMENSION TABLES [star / snowflake model]
-- get surrogate keys from other tables
--- remove un-needed ids  (old customer and product ids)
-- create better names
-- group relevant columns together (dimension keys, dates, metrics / measuements)
-- build the view
*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key, -- get surrogate key from the PRODUCTS table
	cu.customer_key, -- get surrogate key from the CUSTOMERS table
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id
