{{ config(materialized = 'table') }}

SELECT DISTINCT
    md5(concat_ws('|',
        COALESCE({{ normalize_plan_type('plan_type') }}, 'Unknown'),
        COALESCE(monthly_data_gb::text, '0'),
        COALESCE(monthly_bill_usd::text, '0')
    )) AS plan_id,  -- FK a silver_plans

    o.operator_id
FROM {{ ref('staging_mobile_raw') }} AS c
JOIN {{ ref('silver_operators') }} o
  ON {{ normalize_operator('c.operator') }} = o.operator
WHERE c.plan_type IS NOT NULL