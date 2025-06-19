{{ config(materialized = 'table') }}

SELECT
        plan_id,
        plan_type,
        monthly_data_gb,
        monthly_bill_usd,
        operator_id,
        CURRENT_TIMESTAMP AS run_ts
FROM 
    {{ ref('silver_plans') }} AS p