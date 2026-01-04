/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  


--check which is the master table/source in the sources which can be trusted primarily while data integration
/* 
	suppose CRM is master
	for customers use inclusive join like LEFT JOIN and not INNER JOIN so that even if some data is
	missing for some customers in some table we can will still get its data
	while integrating the data of gender from different tables trusted source is CRM 
*/
SELECT cid FROM silver.erp_cust_az12 WHERE cid NOT IN(
SELECT cst_key FROM silver.crm_cust_info);

SELECT cid FROM silver.erp_loc_a101 WHERE cid NOT IN(
SELECT cst_key FROM silver.crm_cust_info);
-- that's why use LEFT JOIN 

--check for gendre
SELECT DISTINCT
	ci.cst_gndr AS crm_gen,
	ca.gen AS erp_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid;

SELECT
	ci.cst_gndr AS crm_gen,
	ca.gen AS erp_gen,
	CASE WHEN ci.cst_gndr = 'n/a' THEN COALESCE(ca.gen, 'n/a')
		 ELSE ci.cst_gndr
	END AS customer_gendre
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid;

-- check for gendre in dim_customers view
SELECT DISTINCT gendre FROM gold.dim_customers;

/*
	for the products we have historical data so first we must discuss that if we want historical analysis or not
	and then take the decision
	so here suppose we do not want historical data and will only focus on current data
*/
-- current data information
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt IS NULL;

SELECT id FROM silver.erp_px_cat_g1v2 WHERE id NOT IN(
SELECT cat_id FROM silver.crm_prd_info);
-- that's why use LEFT JOIN

-- check uniqueness of the data
SELECT t.prd_key, COUNT(*) FROM(
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
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL)t
GROUP BY t.prd_key
HAVING COUNT(*) > 1;

/*
	for the fact_sales we join the data from both dimension views and 
	instead of using the keys that are already present in silver.crm_sales_details table for product and customer
	we replace them with the surrogate key made by us in the dimension views in gold layer
*/

-- foreign key integrity(dimensions)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL;

SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;
