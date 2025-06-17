{{ config(materialized='table') }}

SELECT DISTINCT
    md5(concat_ws('|', device_brand, device_model)) AS device_id,
    {{ normalize_brand('device_brand') }} AS device_brand,
    initcap(device_model) AS device_model
FROM 
    {{ ref('staging_mobile_raw') }}
WHERE 
    device_brand IS NOT NULL