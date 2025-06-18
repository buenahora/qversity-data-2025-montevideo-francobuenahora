{{ config(materialized = 'table') }}

SELECT
  md5({{normalize_brand('device_brand')}} || '|' || device_model) AS device_id,
  device_brand,
  device_model
FROM (
    
  SELECT DISTINCT
    {{ normalize_brand('device_brand') }} AS device_brand,
    initcap(device_model) AS device_model
  FROM 
    {{ ref('staging_mobile_raw') }}
  WHERE 
    device_brand IS NOT NULL

) AS deduplicated