/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'>> Inserting Data into the Table silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			UPPER(LEFT(TRIM(cst_firstname), 1)) + LOWER(RIGHT(TRIM(cst_firstname), LEN(TRIM(cst_firstname))-1)) AS cst_firstname, -- for standardizing the firstname
			UPPER(LEFT(TRIM(cst_lastname), 1)) + LOWER(RIGHT(TRIM(cst_lastname), LEN(TRIM(cst_lastname))-1)) AS cst_lastname, ---- for standardizing the lastname
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a' -- use full forms rather than abbreviation
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
				 ELSE 'n/a' -- use full forms rather than abbreviation
			END AS cst_gndr,
			cst_create_date
		FROM(
		SELECT *,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t
		WHERE flag = 1; -- to take the data with latest create date
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';


		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>> Inserting Data into the Table silver.crm_prd_info';
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- introduces new column used to join with another table in ERP source
			SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key, -- is the real product key used to join with another table
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost, -- remove nulls by replacing it with 0
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'R' THEN 'Road'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a' -- use full forms rather than abbreviation
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt, -- remove time part
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info; -- end date is 1 day less than start date of next record of same product key
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';


		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'>> Inserting Data into the Table silver.crm_sales_details';
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
			TRIM(sls_ord_num),
			TRIM(sls_prd_key),
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR(10)) AS DATE) -- transforming to date value from int
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR(10)) AS DATE) -- transforming to date value from int
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR(10)) AS DATE) -- transforming to date value from int
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales -- sales can not be null/-ve/0 and must be equal to multiplication of quantity & price
			END AS sla_sales, 
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price -- price can not be null/-ve/0 
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'>> Inserting Data into the Table silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid -- remove 'NAS' prefix if present to perfectly join it with another table
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate -- set future birthdates to NULL
			END AS bdate, 
			CASE 
				WHEN UPPER(TRIM(gen)) IN('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN('M', 'MALE') THEN 'Male'
				ELSE 'n/a' -- use full forms rather than abbreviation
			END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';


		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'>> Inserting Data into the Table silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, -- transforming cid to correctly join with another table
			CASE 
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = 'UK' THEN 'United Kingdom'
				WHEN (TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry) -- use full forms rather than abbreviation
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';


		SET @start_time = GETDATE();
		PRINT'+++++++++++';
		PRINT'>> Truncating the Table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'>> Inserting Data into the Table silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'+++++++++++';

		SET @batch_end_time = GETDATE();
		PRINT'==========================================';
		PRINT'LOADING OF SILVER LAYER IS COMPLETED';
		PRINT'>> Total Loading Time for Silver Layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT'==========================================';
	END TRY

	BEGIN CATCH
		PRINT'--------------------------------------';
		PRINT'Error occured during loading process in Silver Layer';
		PRINT'Error Mssage ' + ERROR_MESSAGE();
		PRINT'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'--------------------------------------';
	END CATCH
END
