SELECT
  customer_id,
  DATE_TRUNC('month', payment_date)::DATE AS date_month,
  SUM(payment_amount) AS total_paid_usd,
  COUNT(*) AS num_payments,
  MAX(payment_status) AS latest_status
FROM 
  silver.silver_payments
GROUP BY 
  customer_id, 
  DATE_TRUNC('month', payment_date)