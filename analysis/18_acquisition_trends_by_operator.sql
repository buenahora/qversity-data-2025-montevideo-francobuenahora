-- 18_acquisition_trends_by_operator.sql
SELECT
    o.operator,
    date_month,
    SUM(is_new_customer::int) AS new_customers
FROM
    gold.gold_customer_monthly AS cm
JOIN
    gold.gold_operator AS o ON cm.operator_id = o.operator_id
GROUP BY
    o.operator, date_month
ORDER BY
    o.operator, date_month;
