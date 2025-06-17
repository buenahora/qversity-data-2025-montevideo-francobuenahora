{{ config(materialized = 'table') }}

SELECT DISTINCT
    md5({{ normalize_operator('operator') }}) AS operator_id,
    {{ normalize_operator('operator') }}     AS operator
FROM 
    {{ ref('staging_mobile_raw') }}
WHERE operator IS NOT NULL
  AND operator != ''