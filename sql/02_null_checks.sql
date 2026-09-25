-- 02_null_checks.sql
-- Audits mandatory primary and foreign key columns for unexpected NULLs

SELECT 'orders.order_id' AS column_checked, COUNT(*) AS failed_records FROM `data-engineering-509307.olist_ecommerce.orders` WHERE order_id IS NULL
UNION ALL
SELECT 'orders.customer_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.orders` WHERE customer_id IS NULL
UNION ALL
SELECT 'customers.customer_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.customers` WHERE customer_id IS NULL
UNION ALL
SELECT 'products.product_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.products` WHERE product_id IS NULL
UNION ALL
SELECT 'sellers.seller_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.sellers` WHERE seller_id IS NULL
UNION ALL
SELECT 'order_items.order_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.order_items` WHERE order_id IS NULL
UNION ALL
SELECT 'order_items.product_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.order_items` WHERE product_id IS NULL
UNION ALL
SELECT 'order_items.seller_id', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.order_items` WHERE seller_id IS NULL;
