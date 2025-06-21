-- 04_customers_by_country.sql
SELECT
    country,
    COUNT(DISTINCT customer_id) AS customer_count
FROM 
    gold.gold_customer
GROUP BY 
    country
ORDER BY 
    customer_count DESC;
