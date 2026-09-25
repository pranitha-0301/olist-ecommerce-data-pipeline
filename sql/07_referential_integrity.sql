-- 07_referential_integrity.sql
-- Checks foreign key relationships between entities for orphaned records

SELECT 'orders -> customers (orphaned orders)' AS check_name, COUNT(*) AS orphan_count
FROM `data-engineering-509307.olist_ecommerce.orders` o
LEFT JOIN `data-engineering-509307.olist_ecommerce.customers` c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 'order_items -> orders (orphaned order_items)', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.order_items` oi
LEFT JOIN `data-engineering-509307.olist_ecommerce.orders` o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL

UNION ALL

SELECT 'order_items -> products (orphaned products)', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.order_items` oi
LEFT JOIN `data-engineering-509307.olist_ecommerce.products` p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL

UNION ALL

SELECT 'order_items -> sellers (orphaned sellers)', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.order_items` oi
LEFT JOIN `data-engineering-509307.olist_ecommerce.sellers` s ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL;
