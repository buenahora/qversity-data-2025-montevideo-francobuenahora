-- 16_corr_credit_vs_payment_issues.sql
WITH cust_issues AS (
    SELECT
        c.customer_id,
        c.credit_score::numeric                     AS credit_score,
        MAX(cm.has_payment_issue::int)              AS ever_issue
    FROM
        gold.gold_customer AS c
    JOIN
        gold.gold_customer_monthly AS cm ON cm.customer_id = c.customer_id
    GROUP BY
        c.customer_id, c.credit_score
)
SELECT
    ROUND(CORR(credit_score, ever_issue)::numeric, 3) AS corr_credit_vs_issues
FROM cust_issues;
