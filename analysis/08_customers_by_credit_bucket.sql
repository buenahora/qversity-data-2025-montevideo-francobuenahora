-- 08_customers_by_credit_bucket.sql
SELECT
    credit_bucket,
    COUNT(DISTINCT customer_id) AS customer_count
FROM 
    gold.gold_customer
GROUP BY 
    credit_bucket
ORDER BY 
    credit_bucket;
