
 -- Check1 Verify Table Schema & Record Existence

-- 1. Check total rows written by PySpark SELECT COUNT(*) AS total_line_items

FROM OrdersLineItems;

-- 2. Inspect latest 5 records

SELECT

order_id, user_id, product_id, quantity, order_date

FROM OrdersLineItems ORDER BY order_date DESC LIMIT 5;

Expected Result: total_line_items > 0 and column values match parsed bronze JSON records.


-- Check2 Data Integrity & Constraint Validation

-- Verify no NULL keys or invalid quantities exist SELECT

COUNTIF(order_id IS NULL) AS null_order_ids, COUNTIF(product_id IS NULL) AS null_product_ids, COUNTIF(quantity <= 0) AS invalid_quantities

FROM OrdersLineItems;

Expected Result: All three counts return 0.


-- Check 3: Test Spanner External Connection (EXTERNAL_QUERY) SQL

-- Test connectivity and basic read permissions SELECT *

FROM EXTERNAL_QUERY(

"projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom-conn-v2",

"SELECT 1 AS connection_test;"

);

Expected Result: Returns a single row with value 1 without permission or timeout errors.


-- Check 4: Live Federated Fetch vs Direct Spanner Counts

-- Verify BigQuery can read line items directly through the Spanner connection

SELECT

COUNT(*) AS federated_row_count,

COUNT(DISTINCT product_id) AS distinct_products FROM EXTERNAL_QUERY(

"projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom-conn-v2",

"SELECT product_id FROM OrdersLineItems;"

);

Expected Result: federated_row_count matches the total count retrieved in Spanner Check 1
  

-- Check 5: Inspect Materialized Daily Summary Table 

-- Inspect the latest metrics inserted by Airflow Task 2 SELECT

product_id, total_items_sold, total_orders, aggregated_at

FROM `snappy-mapper-498509-e0.ecom_analytics.daily_product_summary`

ORDER BY aggregated_at DESC, total_items_sold DESC LIMIT 10;

Expected Result: Populated product rows with valid aggregation timestamps.


-- Check 6: Reconciliation Test (Gold Layer vs Live Federated Aggregation)

-- Compare BigQuery summary table totals against live federated calculations

WITH live_federated_calc AS ( SELECT

product_id,

SUM(quantity) AS expected_items_sold, COUNT(DISTINCT order_id) AS expected_orders

FROM EXTERNAL_QUERY(

"projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom-conn-v2",

"SELECT order_id, product_id, quantity FROM OrdersLineItems;"

)

GROUP BY product_id

),

latest_materialized AS ( SELECT

product_id, total_items_sold, total_orders

FROM `snappy-mapper-498509-e0.ecom_analytics.daily_product_summary`

QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER

BY aggregated_at DESC) = 1

) SELECT

f.product_id, f.expected_items_sold, m.total_items_sold,

(f.expected_items_sold - m.total_items_sold) AS item_diff,

(f.expected_orders - m.total_orders) AS order_diff FROM live_federated_calc f

LEFT JOIN latest_materialized m ON f.product_id = m.product_id;

Expected Result: item_diff and order_diff are both 0 for all products.
