-- 02_revenue_by_region.sql
SELECT
    c.region,
    c.country,
    ROUND(SUM(pay.total_paid_usd), 3) AS total_revenue_usd
FROM 
    gold.gold_payment AS pay
JOIN 
    gold.gold_customer AS c 
ON 
    pay.customer_id = c.customer_id
GROUP BY 
    c.region, c.country
ORDER BY 
    total_revenue_usd DESC;