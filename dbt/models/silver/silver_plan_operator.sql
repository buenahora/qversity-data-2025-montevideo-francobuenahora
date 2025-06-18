{{ config(materialized = 'table') }}

SELECT DISTINCT

    md5(concat_ws('|',
        coalesce({{ normalize_plan_type('plan_type') }}, 'Unknown'),
        coalesce(monthly_data_gb::text, '0'),
        coalesce(monthly_bill_usd::text, '0')
    )) AS plan_id,
    
    o.operator_id
FROM 
  {{ ref('staging_mobile_raw') }} AS c
JOIN 
  {{ ref('silver_operators') }} AS o
ON 
  {{ normalize_operator('c.operator') }} = o.operator
WHERE 
  c.plan_type IS NOT NULL