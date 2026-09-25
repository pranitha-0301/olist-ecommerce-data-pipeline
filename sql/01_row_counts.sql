-- 01_row_counts.sql
-- Checks row counts across all 9 loaded BigQuery tables in olist_ecommerce

SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM `data-engineering-509307.olist_ecommerce.customers`
UNION ALL
SELECT 'geolocation', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.geolocation`
UNION ALL
SELECT 'order_items', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.order_items`
UNION ALL
SELECT 'payments', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.payments`
UNION ALL
SELECT 'reviews', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.reviews`
UNION ALL
SELECT 'orders', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.orders`
UNION ALL
SELECT 'products', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.products`
UNION ALL
SELECT 'sellers', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.sellers`
UNION ALL
SELECT 'product_category_translation', COUNT(*) FROM `data-engineering-509307.olist_ecommerce.product_category_translation`
ORDER BY row_count DESC;
