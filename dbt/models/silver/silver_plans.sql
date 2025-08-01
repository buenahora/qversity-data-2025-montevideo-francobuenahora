{{ config(materialized = 'table') }}

WITH raw_plans AS (
    SELECT
        md5(
          concat_ws('|',
            coalesce({{ normalize_plan_type('plan_type') }}, 'Unknown'),
            coalesce(monthly_data_gb::TEXT,  '0'),
            coalesce(monthly_bill_usd::TEXT, '0')
          )
        ) AS plan_id,

        {{ normalize_plan_type('plan_type') }} AS plan_type,
        greatest(monthly_data_gb,  0) AS monthly_data_gb,
        greatest(monthly_bill_usd, 0) AS monthly_bill_usd,
        o.operator_id AS operator_id,
        CURRENT_TIMESTAMP AS run_ts
    FROM 
      {{ ref('staging_mobile_raw') }} AS p
    JOIN 
      {{ ref('silver_operators') }} AS o
    ON 
      {{ normalize_operator('p.operator') }} = o.operator
    WHERE 
      plan_type IS NOT NULL
)

SELECT DISTINCT ON (plan_id)
    plan_id,
    plan_type,
    monthly_data_gb,
    monthly_bill_usd,
    operator_id,
    run_ts
FROM 
  raw_plans
ORDER BY 
  plan_id
