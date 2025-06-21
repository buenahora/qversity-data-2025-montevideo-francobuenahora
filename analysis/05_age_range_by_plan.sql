-- 05_age_range_by_plan.sql
SELECT
    c.age_range,
    p.plan_type,
    COUNT(DISTINCT cm.customer_id) AS customer_count
FROM 
    gold.gold_customer_monthly AS cm
JOIN 
    gold.gold_customer AS c ON cm.customer_id = c.customer_id
JOIN 
    gold.gold_plan AS p ON cm.plan_id = p.plan_id
GROUP BY 
    c.age_range, p.plan_type
ORDER BY 
    p.plan_type, c.age_range;
