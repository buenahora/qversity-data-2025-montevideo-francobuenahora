-- 12_top_services.sql
SELECT
    service_name,
    COUNT(DISTINCT customer_id) AS customer_count
FROM
    gold.gold_customers_services_monthly
WHERE
    is_active
GROUP BY
    service_name
ORDER BY
    customer_count DESC;
