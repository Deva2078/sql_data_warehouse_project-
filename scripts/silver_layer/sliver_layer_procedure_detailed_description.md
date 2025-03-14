📌 Procedure: silver.load_silver()
🔹 Purpose
The silver.load_silver() procedure is designed to extract, transform, and load (ETL) data from the bronze layer to the silver layer in a data warehouse.

The bronze layer contains raw data.
The silver layer contains cleaned and transformed data for further analysis.
📖 What Does This Procedure Do?
✨ 1. Prints Start Notification
✅ Displays a notice that the ETL process for the silver layer has started.

🚨 2. Handles Errors Gracefully
✅ Uses a BEGIN...EXCEPTION block to catch and display errors if something goes wrong.

🔄 3. Loads Data into Silver Tables
✅ Deletes old data (TRUNCATE) before inserting new data into each silver table.
✅ Transforms data before inserting into the silver tables.

⏳ 4. Tracks Execution Time
✅ Measures and prints the load duration for each table.

🛠️ Tables Involved
1️⃣ silver.crm_cust_info (Customer Information)
🔹 Extracts customer data from bronze.crm_cust_info.
🔹 Ensures:

Names are trimmed.
Marital status is standardized:
'S' → 'Single'
'M' → 'Married'
Gender is standardized:
'F' → 'Female'
'M' → 'Male'
Keeps only the latest record for each customer.
2️⃣ silver.erp_cust_az12 (ERP Customer Information)
🔹 Extracts customer records from bronze.erp_cust_az12.
🔹 Fixes:

Cleans customer IDs (removes "NAS" prefix).
Handles invalid birth dates (removes future dates).
Standardizes gender format (F, M, or n/a).
Adds timestamp (dwh_create_date) to track when the record was inserted.
3️⃣ silver.erp_loc_a101 (Location Data)
🔹 Extracts location records from bronze.erp_loc_a101.
🔹 Fixes:

Removes hyphens (-) from cid.
Standardizes country names:
'DE' → 'Germany'
'US' / 'USA' → 'United States'
Empty or NULL → 'n/a'
Adds timestamp (dwh_create_date).
4️⃣ silver.erp_px_cat_g1v2 (Product Categories)
🔹 Extracts product category details from bronze.erp_px_cat_g1v2.
🔹 Adds timestamp (dwh_create_date).

📌 How to Execute the Procedure?
1️⃣ Running the Procedure
To execute the procedure, run:

sql
Copy
Edit
CALL silver.load_silver();
This will trigger the ETL process.

2️⃣ Validating the Data
After executing the procedure, check if data was correctly transferred using these queries:

🔹 Check Row Count in bronze.erp_loc_a101 (Raw Data)

sql
Copy
Edit
SELECT COUNT(*) FROM bronze.erp_loc_a101;
This tells how many records exist in the bronze layer.

🔹 Check Row Count in silver.erp_loc_a101 (Processed Data)

sql
Copy
Edit
SELECT COUNT(*) FROM silver.erp_loc_a101;
This tells how many records were successfully transformed and inserted into the silver layer.

📌 Expected Output
✅ If the process runs successfully:
markdown
Copy
Edit
==============================================
>> STARTING DATA TRANSFORMATION INTO SILVER LAYER <<
==============================================
...
✔ Data successfully inserted into: silver.crm_cust_info
⏳ Load Duration: 2.5 seconds
------------------------------------------------------
✔ Data successfully inserted into: silver.erp_cust_az12
⏳ Load Duration: 1.8 seconds
------------------------------------------------------
✔ Data successfully inserted into: silver.erp_loc_a101
⏳ Load Duration: 2.0 seconds
------------------------------------------------------
✔ Data successfully inserted into: silver.erp_px_cat_g1v2
⏳ Load Duration: 1.5 seconds
------------------------------------------------------
🎉 **SILVER LAYER DATA TRANSFORMATION COMPLETED SUCCESSFULLY** 🎉
==============================================
❌ If an error occurs:
sql
Copy
Edit
❌ ERROR OCCURRED WHILE LOADING DATA INTO SILVER LAYER ❌
ERROR MESSAGE: INSERT has more target columns than expressions
SQLSTATE CODE: 42601
DETAILS: Column mismatch in table 'silver.erp_cust_az12'
🚀 Summary
✅ This procedure cleans, transforms, and loads raw data into the silver layer.
✅ It includes error handling and performance tracking.
✅ Run CALL silver.load_silver(); to execute it.
✅ Use SELECT COUNT(*) queries to verify the data.
