-- 05_numeric_validations.sql
-- Audits numerical values for negative or invalid figures

SELECT 'negative_price' AS check_name, COUNT(*) AS failed_records
FROM `data-engineering-509307.olist_ecommerce.order_items`
WHERE price < 0
UNION ALL
SELECT 'negative_freight_value', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.order_items`
WHERE freight_value < 0
UNION ALL
SELECT 'negative_payment_value', COUNT(*)
FROM `data-engineering-509307.olist_ecommerce.payments`
WHERE payment_value < 0;
