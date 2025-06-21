-- 13_popular_service_combos.sql
WITH service_combos AS (
  SELECT
    customer_id,
    date_month,
    STRING_AGG(service_name, ' + ' ORDER BY service_name) AS service_combo
  FROM gold.gold_customers_services_monthly
  WHERE is_active = TRUE
  GROUP BY customer_id, date_month
),

combo_counts AS (
  SELECT
    service_combo,
    COUNT(*) AS num_customers
  FROM service_combos
  GROUP BY service_combo
  ORDER BY num_customers DESC
)

SELECT *
FROM combo_counts