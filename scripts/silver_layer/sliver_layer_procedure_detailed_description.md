# 📌 Procedure: `silver.load_silver()`

## 🔹 Purpose
The `silver.load_silver()` procedure is designed to **Extract, Transform, and Load (ETL)** data from the **bronze layer** to the **silver layer** in a data warehouse.

- **Bronze Layer** → Contains *raw data*.
- **Silver Layer** → Contains *cleaned and transformed data* for analysis.

---

## 📖 Steps to Execute the Procedure

### **Step 1: Start Notification**
- Displays a message indicating that the **ETL process for the silver layer** has started.

### **Step 2: Handle Errors**
- Uses a `BEGIN...EXCEPTION` block to **catch errors** and display a message if something goes wrong.

### **Step 3: Load Data into Silver Tables**
- **Deletes old data** (*TRUNCATE*) before inserting new data.
- **Transforms data** before inserting into the silver tables.

### **Step 4: Track Execution Time**
- Measures and **prints the load duration** for each table.

---

## 🛠️ Tables Involved in Transformation

### **1️⃣ silver.crm_cust_info (Customer Information)**
- Extracts customer data from `bronze.crm_cust_info`.
- Cleans the data:
  - **Trims names**.
  - **Standardizes marital status**:
    - `'S' → 'Single'`
    - `'M' → 'Married'`
  - **Standardizes gender**:
    - `'F' → 'Female'`
    - `'M' → 'Male'`
  - Keeps **only the latest record** for each customer.

### **2️⃣ silver.erp_cust_az12 (ERP Customer Information)**
- Extracts customer records from `bronze.erp_cust_az12`.
- Fixes:
  - **Removes "NAS" prefix** from customer IDs.
  - **Filters out future birthdates**.
  - **Standardizes gender format** (`F`, `M`, or `n/a`).
  - Adds **timestamp (`dwh_create_date`)**.

### **3️⃣ silver.erp_loc_a101 (Location Data)**
- Extracts location records from `bronze.erp_loc_a101`.
- Cleans the data:
  - **Removes hyphens (`-`)** from `cid`.
  - **Standardizes country names**:
    - `'DE' → 'Germany'`
    - `'US' / 'USA' → 'United States'`
    - *Empty or NULL* → `'n/a'`
  - Adds **timestamp (`dwh_create_date`)**.

### **4️⃣ silver.erp_px_cat_g1v2 (Product Categories)**
- Extracts product category details from `bronze.erp_px_cat_g1v2`.
- Adds **timestamp (`dwh_create_date`)**.

---

## 📌 How to Execute the Procedure?

### **Step 1: Run the Procedure**
Execute the following SQL command:

```sql
CALL silver.load_silver();
```

---

## 📌 Validating the Data

### **Step 2: Verify Data Transfer**
After executing the procedure, use the following queries to check if the data was transferred correctly.

#### ✅ Check Row Count in `bronze.erp_loc_a101` (Raw Data)
```sql
SELECT COUNT(*) FROM bronze.erp_loc_a101;
```
🔹 This checks the number of records in the bronze layer.

#### ✅ Check Row Count in `silver.erp_loc_a101` (Processed Data)
```sql
SELECT COUNT(*) FROM silver.erp_loc_a101;
```
🔹 This verifies how many records were successfully transformed and inserted into the silver layer.

---

## 📌 Expected Output

### ✅ **If the process runs successfully**
```plaintext
==============================================
>> STARTING DATA TRANSFORMATION INTO SILVER LAYER <<
==============================================
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
🎉 SILVER LAYER DATA TRANSFORMATION COMPLETED SUCCESSFULLY 🎉
==============================================
```

### ❌ **If an error occurs**
```plaintext
❌ ERROR OCCURRED WHILE LOADING DATA INTO SILVER LAYER ❌
ERROR MESSAGE: INSERT has more target columns than expressions
SQLSTATE CODE: 42601
DETAILS: Column mismatch in table 'silver.erp_cust_az12'
```

---

## 🚀 Summary
✅ This procedure cleans, transforms, and loads raw data into the silver layer.  
✅ It includes error handling and performance tracking.  
✅ Run `CALL silver.load_silver();` to execute it.  
✅ Use `SELECT COUNT(*)` queries to verify the data.  

---

Let me know if you need further refinements or enhancements! 🚀
