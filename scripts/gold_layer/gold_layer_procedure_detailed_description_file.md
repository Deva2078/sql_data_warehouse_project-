📌 Procedure: Creating Gold Layer Views

🔹 Purpose

The Gold Layer is designed to provide a structured and enriched dataset for business intelligence and analytical reporting. It consolidates, standardizes, and joins data from multiple Silver Layer tables into a dimensional model.

🛠 Tables Involved

1️⃣ gold.dim_customers (Customer Dimension)2️⃣ gold.dim_products (Product Dimension)3️⃣ gold.fact_sales (Fact Table - Sales Transactions)

📖 What Does This Process Do?

✅ Creates the gold.dim_customers View

Generates a surrogate key using ROW_NUMBER() for customer_key.

Selects and standardizes customer data from silver.crm_cust_info.

Joins with silver.erp_cust_az12 to supplement missing values.

Merges location details from silver.erp_loc_a101.

Ensures gender data prioritization (CRM as primary source, ERP as fallback).

✅ Creates the gold.dim_products View

Generates a surrogate key using ROW_NUMBER() for product_key.

Extracts and standardizes product details from silver.crm_prd_info.

Enriches category details from silver.erp_px_cat_g1v2.

Filters out historical data, keeping only active products.

✅ Creates the gold.fact_sales View

Defines fact_sales as the primary sales transaction table.

Joins crm_sales_details with gold.dim_products and gold.dim_customers.

Maps customer and product surrogate keys for efficient analytics.

📌 How to Execute the Procedure?

1️⃣ Creating Gold Layer Views

To execute the transformation and generate the gold views, run the following queries:

-- Drop if exists and create gold.dim_customers
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

-- Drop if exists and create gold.dim_products
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- Drop if exists and create gold.fact_sales
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,
    pr.product_key AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

🔍 Validating the Data

After creating the views, validate if data has been correctly transformed by running:

-- Count records in gold.dim_customers
SELECT COUNT(*) FROM gold.dim_customers;

-- Count records in gold.dim_products
SELECT COUNT(*) FROM gold.dim_products;

-- Count records in gold.fact_sales
SELECT COUNT(*) FROM gold.fact_sales;

📌 Expected Output

If the process runs successfully, you should see:

==============================================
>> GOLD LAYER VIEWS CREATED SUCCESSFULLY <<
==============================================
✔ gold.dim_customers view created
✔ gold.dim_products view created
✔ gold.fact_sales view created

If an error occurs:

❌ ERROR OCCURRED WHILE CREATING GOLD LAYER VIEWS ❌
ERROR MESSAGE: relation "gold.dim_products" does not exist
SQLSTATE CODE: 42P01
DETAILS: Missing dependency in Silver Layer

🚀 Summary

✅ The Gold Layer provides clean, structured, and enriched data for reporting.✅ It contains Dimension Tables (dim_customers, dim_products) and a Fact Table (fact_sales).✅ The data is transformed and joined from Silver Layer tables.✅ Execute the provided SQL scripts to create and validate the views.
