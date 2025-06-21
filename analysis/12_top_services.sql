-- 12_top_services.sql
SELECT
    service_name,
    COUNT(DISTINCT customer_id) AS customer_count
FROM
    gold.gold_service_client_monthly
WHERE
    is_active
GROUP BY
    service_name
ORDER BY
    customer_count DESC;
