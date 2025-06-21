{{ config(materialized = 'table') }}

SELECT
  p.plan_id,
  p.plan_type,
  p.monthly_data_gb,
  p.monthly_bill_usd,
  p.operator_id,
  CURRENT_TIMESTAMP AS run_ts
FROM 
  {{ ref('silver_plans') }} AS p