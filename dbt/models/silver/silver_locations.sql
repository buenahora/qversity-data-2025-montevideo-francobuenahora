{{ config(materialized='table') }}

SELECT DISTINCT
    md5(concat_ws('|', country, city, latitude::text, longitude::text)) AS location_id,
    {{ normalize_city('city') }} AS city,
    {{ normalize_country('country') }} AS country,
    latitude::numeric,
    longitude::numeric
FROM 
    {{ ref('staging_mobile_raw') }}
WHERE 
    country IS NOT NULL
    AND city IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude BETWEEN -90 AND 90
    AND longitude BETWEEN -180 AND 180
