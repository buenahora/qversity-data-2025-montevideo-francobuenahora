{{ config(materialized = 'table') }}

SELECT DISTINCT
    c.customer_id,
    p.plan_id,
    c.credit_limit,
    c.data_usage_current_month,
    c.ingestion_date AS plan_start_ts
FROM 
{{ ref('staging_mobile_raw') }} c
JOIN {{ ref('silver_customers') }} sc
  ON c.customer_id = sc.customer_id
JOIN {{ ref('silver_plans') }} p
  ON md5(concat_ws('|',
       COALESCE({{ normalize_plan_type('c.plan_type') }}, 'Unknown'),
       COALESCE(c.monthly_data_gb::text, '0'),
       COALESCE(c.monthly_bill_usd::text, '0')
     )) = p.plan_id
WHERE c.customer_id IS NOT NULL AND c.plan_type IS NOT NULL