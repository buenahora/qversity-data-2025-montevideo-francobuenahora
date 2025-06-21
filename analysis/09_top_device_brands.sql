-- 09_top_device_brands.sql
SELECT
    d.device_brand,
    COUNT(DISTINCT c.customer_id) AS customer_count
FROM
    gold.gold_customer AS c
JOIN
    gold.gold_device AS d ON c.device_id = d.device_id
GROUP BY
    d.device_brand
ORDER BY
    customer_count DESC;
