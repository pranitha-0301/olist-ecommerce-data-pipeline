-- 04_date_validations.sql
-- Checks chronological consistency across order lifecycle events

SELECT 'delivered_before_purchase' AS check_name, COUNT(*) AS failed_records
FROM `data-engineering-509307.olist_ecommerce.orders`
WHERE order_delivered_customer_date < order_purchase_timestamp
UNION ALL
SELECT 'approval_before_purchase', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.orders`
WHERE order_approved_at < order_purchase_timestamp
UNION ALL
SELECT 'carrier_delivery_before_purchase', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.orders`
WHERE order_delivered_carrier_date < order_purchase_timestamp;
