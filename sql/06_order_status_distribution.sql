-- 06_order_status_distribution.sql
-- Displays volume and percentage breakdown of order statuses

SELECT 
    order_status, 
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM `data-engineering-509307.olist_ecommerce.orders`
GROUP BY order_status
ORDER BY total_orders DESC;
