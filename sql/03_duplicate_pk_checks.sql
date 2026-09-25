-- 03_duplicate_pk_checks.sql
-- Verifies primary key uniqueness across core entities

SELECT 'orders' AS table_name, 'order_id' AS pk_column, COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_count FROM `data-engineering-509307.olist_ecommerce.orders`
UNION ALL
SELECT 'customers', 'customer_id', COUNT(*) - COUNT(DISTINCT customer_id) FROM `data-engineering-509307.olist_ecommerce.customers`
UNION ALL
SELECT 'products', 'product_id', COUNT(*) - COUNT(DISTINCT product_id) FROM `data-engineering-509307.olist_ecommerce.products`
UNION ALL
SELECT 'sellers', 'seller_id', COUNT(*) - COUNT(DISTINCT seller_id) FROM `data-engineering-509307.olist_ecommerce.sellers`;
