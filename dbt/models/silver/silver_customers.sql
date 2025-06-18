{{ config(materialized = 'table') }}

SELECT DISTINCT ON (customer_id)
    customer_id,
    INITCAP(first_name) AS first_name,
    INITCAP(last_name) AS last_name,
    email,
    phone_number,
    age,
    registration_date,
    credit_score,
    INITCAP(status) AS status,
    l.location_id AS location_id,
    md5({{normalize_brand('c.device_brand')}} || '|' || c.device_model) AS device_id,
    ingestion_date,
    source
FROM 
    {{ ref('staging_mobile_raw') }} AS c
LEFT JOIN 
    {{ ref('silver_locations') }} AS l
ON 
    {{ normalize_country('c.country') }} = l.country
AND 
    {{ normalize_city('c.city') }} = l.city

WHERE
    customer_id IS NOT NULL
    AND age BETWEEN 0 AND 119
    AND credit_score BETWEEN 300 AND 850
    AND INITCAP(status) IN ('Active', 'Inactive', 'Suspended')
    AND email ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'

ORDER BY
    customer_id,
    ingestion_date DESC
