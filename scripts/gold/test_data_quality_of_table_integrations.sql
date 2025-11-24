/* Check quality of CUSTOMER gold layer integration */
-- check for duplicates
SELECT cst_id, COUNT(*) 
FROM 
(
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_CUST_AZ12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_LOC_A101 AS la
	ON ci.cst_key = la.cid
) AS t 
GROUP BY cst_id
HAVING COUNT(*) > 1

-- check for discrepancy of gender data in both tables
-- ** if discrepancy is found, then determine which source system had more accurate data
SELECT DISTINCT
		ci.cst_gndr, 
		ca.gen,
		CASE -- only use ca.gen when cst_gnder is not available
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is Master for gender info
			ELSE COALESCE(ca.gen, 'n/a')
		END AS new_gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_CUST_AZ12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_LOC_A101 AS la
ON ci.cst_key = la.cid
--================================================================================================
-- PRODUCT
-- check uniqueness of keys
-- expectation NO OUTPUT
SELECT prd_key, COUNT(*)
FROM
(
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filter out all historical data
) AS t
GROUP BY prd_key
HAVING COUNT(*) > 1;

--================================================================================================
-- SALES
-- check if surrogate key of SALES connects to key of CUSTOMERS
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

-- check if surrogate key of SALES connects to key of PRODUCTS
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL
