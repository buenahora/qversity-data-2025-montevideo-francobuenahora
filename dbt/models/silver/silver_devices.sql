{{ config(materialized = 'table') }}

SELECT
  md5(device_brand || '|' || device_model) AS device_id,
  device_brand,
  device_model,
  CURRENT_TIMESTAMP AS run_ts
FROM (
    
  SELECT DISTINCT
    {{ normalize_brand('device_brand') }} AS device_brand,
    {{normalize_model('device_model')}} AS device_model
  FROM 
    {{ ref('staging_mobile_raw') }}
  WHERE 
    device_brand IS NOT NULL
    AND device_model IS NOT NULL
    AND device_model != ''
    AND device_brand != ''

) AS deduplicated