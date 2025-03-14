CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE 
    err_msg TEXT;        
    err_state TEXT;      
    err_detail TEXT;     
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    load_duration INTERVAL;
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> STARTING DATA TRANSFORMATION INTO SILVER LAYER <<';
    RAISE NOTICE '==============================================';

    -- TRY BLOCK
    BEGIN
        -- Loading silver.crm_cust_info
        RAISE NOTICE '>> Preparing to refresh: silver.crm_cust_info <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: silver.crm_cust_info <<<';
        TRUNCATE TABLE silver.crm_cust_info;
        RAISE NOTICE '✔ Table truncated successfully: silver.crm_cust_info';

        RAISE NOTICE '>>> Inserting Data Into: silver.crm_cust_info <<<';
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        
        end_time := clock_timestamp();
        load_duration := end_time - start_time;
        
        RAISE NOTICE '✔ Data transformed successfully into: silver.crm_cust_info';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading silver.erp_cust_az12
        start_time := NOW();
        RAISE NOTICE '>> Preparing to refresh: silver.erp_cust_az12 <<';
        RAISE NOTICE '>>> Truncating Table: silver.erp_cust_az12 <<<';
        TRUNCATE TABLE silver.erp_cust_az12;
        RAISE NOTICE '✔ Table truncated successfully: silver.erp_cust_az12';

        RAISE NOTICE '>>> Inserting Data Into: silver.erp_cust_az12 <<<';
        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen, dwh_create_date
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
                ELSE 'n/a'
            END AS gen,
			CURRENT_TIMESTAMP
        FROM bronze.erp_cust_az12;
        end_time := NOW();
        RAISE NOTICE '✔ Data successfully inserted into: silver.erp_cust_az12';
        RAISE NOTICE '⏳ Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '------------------------------------------------------';

        -- Loading silver.erp_loc_a101
        start_time := NOW();
        RAISE NOTICE '>> Preparing to refresh: silver.erp_loc_a101 <<';
        RAISE NOTICE '>>> Truncating Table: silver.erp_loc_a101 <<<';
        TRUNCATE TABLE silver.erp_loc_a101;
        RAISE NOTICE '✔ Table truncated successfully: silver.erp_loc_a101';

        RAISE NOTICE '>>> Inserting Data Into: silver.erp_loc_a101 <<<';
        INSERT INTO silver.erp_loc_a101 (
            cid, cntry, dwh_create_date
        )
        SELECT
            REPLACE(cid, '-','') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry,
			CURRENT_TIMESTAMP
        FROM bronze.erp_loc_a101;
        end_time := NOW();
        RAISE NOTICE '✔ Data successfully inserted into: silver.erp_loc_a101';
        RAISE NOTICE '⏳ Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '------------------------------------------------------';

        -- Loading silver.erp_px_cat_g1v2
        start_time := NOW();
        RAISE NOTICE '>> Preparing to refresh: silver.erp_px_cat_g1v2 <<';
        RAISE NOTICE '>>> Truncating Table: silver.erp_px_cat_g1v2 <<<';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        RAISE NOTICE '✔ Table truncated successfully: silver.erp_px_cat_g1v2';

        RAISE NOTICE '>>> Inserting Data Into: silver.erp_px_cat_g1v2 <<<';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id, cat, subcat, maintenance, dwh_create_date
        )
        SELECT
            id, cat, subcat, maintenance,CURRENT_TIMESTAMP
        FROM bronze.erp_px_cat_g1v2;
        end_time := NOW();
        RAISE NOTICE '✔ Data successfully inserted into: silver.erp_px_cat_g1v2';
        RAISE NOTICE '⏳ Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '------------------------------------------------------';

        -- Final Completion Message
        RAISE NOTICE '==============================================';
        RAISE NOTICE '🎉 SILVER LAYER DATA TRANSFORMATION COMPLETED SUCCESSFULLY 🎉';
        RAISE NOTICE '==============================================';
    
    EXCEPTION
        WHEN OTHERS THEN
            -- Capture error details
            GET STACKED DIAGNOSTICS err_msg = MESSAGE_TEXT,
                                   err_state = RETURNED_SQLSTATE,
                                   err_detail = PG_EXCEPTION_DETAIL;
            
            -- Display error message in console
            RAISE NOTICE '===================================================';
            RAISE NOTICE '❌ ERROR OCCURRED WHILE LOADING DATA INTO SILVER LAYER ❌';
            RAISE NOTICE 'ERROR MESSAGE: %', err_msg;
            RAISE NOTICE 'SQLSTATE CODE: %', err_state;
            RAISE NOTICE 'DETAILS: %', err_detail;
            RAISE NOTICE '===================================================';
    END;
END;
$$;

-- Executing the procedure
CALL silver.load_silver();

select count(*) from bronze.erp_loc_a101;
select count(*) from silver.erp_loc_a101;
