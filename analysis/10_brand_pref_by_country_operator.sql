-- 10_brand_pref_by_country_operator.sql
SELECT
    c.country,
    o.operator,
    d.device_brand,
    COUNT(DISTINCT cm.customer_id) AS customer_count
FROM
    gold.gold_customer_monthly AS cm
JOIN
    gold.gold_customer AS c ON cm.customer_id = c.customer_id
JOIN
    gold.gold_device AS d ON cm.device_id = d.device_id
JOIN
    gold.gold_operator AS o ON cm.operator_id = o.operator_id
GROUP BY
    c.country, o.operator, d.device_brand
ORDER BY
    customer_count DESC;
