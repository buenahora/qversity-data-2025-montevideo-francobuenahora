-- 20_mean_median_revenue_plan_operator.sql
SELECT
    p.plan_type,
    o.operator,
    ROUND(AVG(pay.total_paid_usd), 3) AS mean_rev_usd,
    ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pay.total_paid_usd)::int,
        3
    ) AS median_rev_usd
FROM
    gold.gold_payment AS pay
JOIN
    gold.gold_customer_monthly AS cm 
ON 
    (pay.customer_id, pay.date_month) = (cm.customer_id, cm.date_month)
JOIN
    gold.gold_plan AS p ON cm.plan_id = p.plan_id
JOIN
    gold.gold_operator AS o ON cm.operator_id = o.operator_id
GROUP BY
    p.plan_type, o.operator
ORDER BY
    p.plan_type, o.operator;
