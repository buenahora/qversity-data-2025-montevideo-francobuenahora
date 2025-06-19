{{ config(materialized = 'table') }}

SELECT
    operator_id,
    operator,
    CURRENT_TIMESTAMP AS run_ts
FROM 
    {{ ref('silver_operators') }}