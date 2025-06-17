{{ config(materialized='table') }}

SELECT DISTINCT
    md5(operator) AS operator_id,   -- surrogate PK
    {{ normalize_country('operator') }} AS operator
FROM 
    {{ ref('staging_mobile_raw') }}
