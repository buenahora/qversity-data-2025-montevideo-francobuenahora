{{ config(materialized='table') }}

WITH normalized AS (
    SELECT DISTINCT
        md5(concat_ws('|', country, city, latitude::text, longitude::text)) AS location_id,
        {{ normalize_city('city') }} AS city,
        {{ normalize_country('country') }} AS country,
        ROUND(latitude::NUMERIC, 3) AS latitude,
        ROUND(longitude::NUMERIC, 3) AS longitude,
        CURRENT_TIMESTAMP AS run_ts
    FROM 
        {{ ref('staging_mobile_raw') }}
    WHERE 
        country IS NOT NULL
        AND city IS NOT NULL
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
)

SELECT *,
    CASE
    WHEN country = 'Mexico' THEN 'North America'
    WHEN country IN (
    'Peru',
    'Argentina',
    'Chile',
    'Colombia'
    ) THEN 'South America'
    ELSE 
        'Unknown'
    END AS region
FROM normalized
