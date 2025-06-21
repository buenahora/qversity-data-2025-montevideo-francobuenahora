-- 15_customers_with_pending_payments.sql
SELECT DISTINCT customer_id
FROM 
    gold.gold_customer_monthly
WHERE latest_status = 'Pending';
