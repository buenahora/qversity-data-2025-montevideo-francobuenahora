-- 14_pct_customers_payment_issues.sql
WITH issues AS (
    SELECT
        customer_id,
        MAX(has_payment_issue::int) AS ever_issue
    FROM
        gold.gold_customer_monthly
    GROUP BY
        customer_id
)
SELECT
    ROUND(100.0 * SUM(ever_issue)::numeric / COUNT(*), 3) AS pct_with_issues
FROM issues;
