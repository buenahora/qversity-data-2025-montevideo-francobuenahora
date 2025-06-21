-- 19_customer_status_distribution.sql
SELECT
    status,
    ROUND(
        100.0 * COUNT(DISTINCT customer_id)
      / (SELECT COUNT(*) FROM gold.gold_customer),
        3
    ) AS pct_customers
FROM
    gold.gold_customer
GROUP BY
    status
ORDER BY
    status;
