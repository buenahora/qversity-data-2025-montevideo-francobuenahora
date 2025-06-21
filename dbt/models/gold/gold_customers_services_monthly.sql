{{ config(materialized='table') }}

WITH base AS (
  SELECT
    cs.customer_id,
    cs.service_id,
    s.service_name,
    c.registration_date
  FROM 
    {{ ref('silver_customers_services') }} cs
  JOIN 
    {{ ref('silver_customers') }} c ON cs.customer_id = c.customer_id
  JOIN 
    {{ ref('silver_services') }} s ON cs.service_id = s.service_id
),

months AS (
  SELECT date_month
  FROM 
    {{ ref('gold_date') }}
),

subscriptions AS (
  SELECT
    b.customer_id,
    b.service_name,
    m.date_month,
    TRUE AS is_active
  FROM 
    base b
  CROSS JOIN 
    months m
)

SELECT 
  *,
  CURRENT_TIMESTAMP AS run_ts
FROM 
  subscriptions
