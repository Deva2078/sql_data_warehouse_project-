CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    err_msg TEXT;        -- Stores error message
    err_state TEXT;      -- Stores SQLSTATE (error code)
    err_detail TEXT;     -- Stores detailed error info
    start_time TIMESTAMP; -- Start time of operation
    end_time TIMESTAMP;   -- End time of operation
    load_duration INTERVAL; -- Duration calculation
BEGIN
    RAISE NOTICE '==============================================';
    RAISE NOTICE '>> STARTING DATA LOAD INTO BRONZE LAYER <<';
    RAISE NOTICE '==============================================';

    -- TRY BLOCK
    BEGIN
        -- Loading crm_cust_info
        RAISE NOTICE '>> Preparing to refresh: bronze.crm_cust_info <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.crm_cust_info <<<';
        TRUNCATE TABLE bronze.crm_cust_info;
        RAISE NOTICE '✔ Table truncated successfully: bronze.crm_cust_info';

        RAISE NOTICE '>>> Inserting Data Into: bronze.crm_cust_info <<<';
        EXECUTE format(
            'COPY bronze.crm_cust_info FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.crm_cust_info';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading crm_prd_info
        RAISE NOTICE '>> Preparing to refresh: bronze.crm_prd_info <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.crm_prd_info <<<';
        TRUNCATE TABLE bronze.crm_prd_info;
        RAISE NOTICE '✔ Table truncated successfully: bronze.crm_prd_info';

        RAISE NOTICE '>>> Inserting Data Into: bronze.crm_prd_info <<<';
        EXECUTE format(
            'COPY bronze.crm_prd_info FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.crm_prd_info';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading crm_sales_details
        RAISE NOTICE '>> Preparing to refresh: bronze.crm_sales_details <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.crm_sales_details <<<';
        TRUNCATE TABLE bronze.crm_sales_details;
        RAISE NOTICE '✔ Table truncated successfully: bronze.crm_sales_details';

        RAISE NOTICE '>>> Inserting Data Into: bronze.crm_sales_details <<<';
        EXECUTE format(
            'COPY bronze.crm_sales_details FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.crm_sales_details';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading erp_cust_az12
        RAISE NOTICE '>> Preparing to refresh: bronze.erp_cust_az12 <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.erp_cust_az12 <<<';
        TRUNCATE TABLE bronze.erp_cust_az12;
        RAISE NOTICE '✔ Table truncated successfully: bronze.erp_cust_az12';

        RAISE NOTICE '>>> Inserting Data Into: bronze.erp_cust_az12 <<<';
        EXECUTE format(
            'COPY bronze.erp_cust_az12 FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.erp_cust_az12';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading erp_loc_a101
        RAISE NOTICE '>> Preparing to refresh: bronze.erp_loc_a101 <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.erp_loc_a101 <<<';
        TRUNCATE TABLE bronze.erp_loc_a101;
        RAISE NOTICE '✔ Table truncated successfully: bronze.erp_loc_a101';

        RAISE NOTICE '>>> Inserting Data Into: bronze.erp_loc_a101 <<<';
        EXECUTE format(
            'COPY bronze.erp_loc_a101 FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.erp_loc_a101';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;
        RAISE NOTICE '------------------------------------------------------';

        -- Loading erp_px_cat_g1v2
        RAISE NOTICE '>> Preparing to refresh: bronze.erp_px_cat_g1v2 <<';
        start_time := clock_timestamp();
        
        RAISE NOTICE '>>> Truncating Table: bronze.erp_px_cat_g1v2 <<<';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        RAISE NOTICE '✔ Table truncated successfully: bronze.erp_px_cat_g1v2';

        RAISE NOTICE '>>> Inserting Data Into: bronze.erp_px_cat_g1v2 <<<';
        EXECUTE format(
            'COPY bronze.erp_px_cat_g1v2 FROM %L DELIMITER '','' CSV HEADER',
            'E:/DATA WITH BARAA/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
        );
        end_time := clock_timestamp();
        load_duration := end_time - start_time;

        RAISE NOTICE '✔ Data inserted successfully into: bronze.erp_px_cat_g1v2';
        RAISE NOTICE '⏳ Load Duration: % seconds', load_duration;

        RAISE NOTICE '==============================================';
        RAISE NOTICE '🎉 BRONZE LAYER DATA LOAD COMPLETED SUCCESSFULLY 🎉';
        RAISE NOTICE '==============================================';

    EXCEPTION
        WHEN OTHERS THEN
            -- Capture error details
            GET STACKED DIAGNOSTICS err_msg = MESSAGE_TEXT,
                                   err_state = RETURNED_SQLSTATE,
                                   err_detail = PG_EXCEPTION_DETAIL;

            -- Display error message in console
            RAISE NOTICE '===================================================';
            RAISE NOTICE '❌ ERROR OCCURRED WHILE LOADING DATA ❌';
            RAISE NOTICE 'ERROR MESSAGE: %', err_msg;
            RAISE NOTICE 'SQLSTATE CODE: %', err_state;
            RAISE NOTICE 'DETAILS: %', err_detail;
            RAISE NOTICE '===================================================';
    END;
END;
$$;
-- executing or calling the procedure to insert the CSV file data into tables

CALL bronze.load_bronze();

select count(*) from bronze.crm_cust_info;
