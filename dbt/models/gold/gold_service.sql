{{ config(materialized='table') }}

SELECT
    service_id,
    service_name,
    CURRENT_TIMESTAMP AS run_ts
FROM 
    {{ ref('silver_services') }}