# 📌 Procedure: `bronze.load_bronze()`

## 🔹 Purpose
The `bronze.load_bronze()` procedure is designed to **Extract, Transform, and Load (ETL)** data from external **CSV files** into the **bronze layer** of a data warehouse.

- **Bronze Layer** → Stores *raw, unprocessed data* directly from source systems.

---

## 📖 What Does This Procedure Do?

### ✅ 1. Prints Start Notification
- Displays a message indicating the **bronze layer data load** process has begun.

### ✅ 2. Handles Errors Gracefully
- Uses a `BEGIN...EXCEPTION` block to **catch errors** and display an appropriate message if something goes wrong.

### ✅ 3. Loads Data into Bronze Tables
- **Deletes old data** (*TRUNCATE*) before inserting new data.
- **Loads fresh data** from **CSV files** into the bronze tables.

### ✅ 4. Tracks Execution Time
- Measures and **prints the load duration** for each table to track performance.

---

## 🛠️ Tables Involved

### **1️⃣ bronze.crm_cust_info (Customer Information)**
- Extracts customer data from `source_crm/cust_info.csv`.
- Ensures:
  - Old data is removed before inserting new data.
  - New data is inserted using the `COPY` command.

### **2️⃣ bronze.crm_prd_info (Product Information)**
- Extracts product data from `source_crm/prd_info.csv`.
- Ensures:
  - Old data is removed.
  - New records are inserted correctly.

### **3️⃣ bronze.crm_sales_details (Sales Data)**
- Extracts sales details from `source_crm/sales_details.csv`.
- Ensures:
  - Old sales records are removed.
  - Fresh data is inserted into the table.

### **4️⃣ bronze.erp_cust_az12 (ERP Customer Information)**
- Extracts customer records from `source_erp/cust_az12.csv`.
- Ensures:
  - Table is truncated before inserting data.
  - New records are loaded properly.

### **5️⃣ bronze.erp_loc_a101 (Location Data)**
- Extracts location details from `source_erp/loc_a101.csv`.
- Ensures:
  - Table is refreshed with the latest data.
  - Data is inserted correctly.

### **6️⃣ bronze.erp_px_cat_g1v2 (Product Categories)**
- Extracts product categories from `source_erp/px_cat_g1v2.csv`.
- Ensures:
  - Table is cleared before loading data.
  - New product category data is inserted.

---

## 📌 How to Execute the Procedure?

### **1️⃣ Running the Procedure**
Execute the following command to trigger the **ETL process**:

```sql
CALL bronze.load_bronze();
```

### **2️⃣ Validating the Data**
After execution, verify that data was correctly transferred using these queries:

#### 🔹 Check Row Count in `bronze.crm_cust_info` (Raw Data)
```sql
SELECT COUNT(*) FROM bronze.crm_cust_info;
```
This checks how many records exist in the **bronze layer**.

#### 🔹 Check Row Count in `bronze.erp_loc_a101` (Processed Data)
```sql
SELECT COUNT(*) FROM bronze.erp_loc_a101;
```
This verifies the number of transformed records inserted into the **bronze layer**.

---

## 📌 Expected Output

### ✅ If the process runs successfully:
```plaintext
==============================================
>> STARTING DATA LOAD INTO BRONZE LAYER <<
==============================================
✔ Data successfully inserted into: bronze.crm_cust_info
⏳ Load Duration: 2.5 seconds
------------------------------------------------------
✔ Data successfully inserted into: bronze.crm_prd_info
⏳ Load Duration: 1.8 seconds
------------------------------------------------------
✔ Data successfully inserted into: bronze.crm_sales_details
⏳ Load Duration: 2.0 seconds
------------------------------------------------------
✔ Data successfully inserted into: bronze.erp_cust_az12
⏳ Load Duration: 1.5 seconds
------------------------------------------------------
✔ Data successfully inserted into: bronze.erp_loc_a101
⏳ Load Duration: 2.2 seconds
------------------------------------------------------
✔ Data successfully inserted into: bronze.erp_px_cat_g1v2
⏳ Load Duration: 1.7 seconds
------------------------------------------------------
🎉 BRONZE LAYER DATA LOAD COMPLETED SUCCESSFULLY 🎉
==============================================
```

### ❌ If an error occurs:
```plaintext
❌ ERROR OCCURRED WHILE LOADING DATA INTO BRONZE LAYER ❌
ERROR MESSAGE: COPY failed due to missing column
SQLSTATE CODE: 23502
DETAILS: Column mismatch in table 'bronze.erp_cust_az12'
```

---

## 🚀 Summary
✅ This procedure **loads raw data from CSV files** into the **bronze layer**.
✅ It includes **error handling** and **performance tracking**.
✅ Run `CALL bronze.load_bronze();` to execute it.
✅ Use `SELECT COUNT(*)` queries to **verify the data**.

Would you like to add logging improvements or automation for error tracking? 😊

