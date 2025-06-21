-- 11_brand_pref_by_plan_type.sql
SELECT
    p.plan_type,
    d.device_brand,
    COUNT(DISTINCT cm.customer_id) AS customer_count
FROM
    gold.gold_customer_monthly AS cm
JOIN
    gold.gold_plan AS p ON cm.plan_id = p.plan_id
JOIN
    gold.gold_device AS d ON cm.device_id = d.device_id
GROUP BY
    p.plan_type, d.device_brand
ORDER BY
    p.plan_type, customer_count DESC;
