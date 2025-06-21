-- 07_customers_by_operator.sql
SELECT
    o.operator,
    COUNT(DISTINCT cm.customer_id) AS customer_count
FROM 
    gold.gold_customer_monthly AS cm
JOIN 
    gold.gold_operator AS o ON cm.operator_id = o.operator_id
GROUP BY 
    o.operator
ORDER BY 
    customer_count DESC;