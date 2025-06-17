SELECT *
FROM {{ ref('silver_locations') }}
WHERE latitude IS NULL
   OR longitude IS NULL
   OR latitude NOT BETWEEN -90 AND 90
   OR longitude NOT BETWEEN -180 AND 180