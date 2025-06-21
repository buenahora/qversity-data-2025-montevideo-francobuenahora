-- 01_arpu_by_plan.sql
SELECT
    p.plan_type,
    ROUND(AVG(pay.total_paid_usd), 3) AS arpu_usd
FROM 
    gold.gold_payment AS pay
JOIN 
    gold.gold_customer_monthly AS cm   
ON 
    (pay.customer_id , pay.date_month) = (cm.customer_id , cm.date_month)
JOIN 
    gold.gold_plan AS p 
ON 
    cm.plan_id = p.plan_id
GROUP BY 
    p.plan_type
ORDER BY 
    p.plan_type;
