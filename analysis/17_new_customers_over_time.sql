-- 17_new_customers_over_time.sql
SELECT
    date_month,
    SUM(is_new_customer::int) AS new_customers
FROM
    gold.gold_customer_monthly
GROUP BY
    date_month
ORDER BY 
    date_month;
