{{ config(materialized='table') }}

SELECT DISTINCT
    md5(plan_type || '|' || monthly_data_gb || '|' || monthly_bill_usd) AS plan_id,
    {{ normalize_plan_type('plan_type') }} AS plan_type,
    greatest(monthly_data_gb, 0) AS monthly_data_gb,
    greatest(monthly_bill_usd, 0) AS  monthly_bill_usd
FROM 
    {{ ref('staging_mobile_raw') }} 
        WHERE plan_type IS NOT NULL