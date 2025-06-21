{{ config(materialized = 'table') }}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    age,
    FLOOR(age / 10) * 10 AS age_range,
    registration_date,
    credit_score,

    CASE
        WHEN credit_score IS NULL THEN 'Unknown'
        WHEN credit_score < 580 THEN 'Poor'
        WHEN credit_score BETWEEN 580 AND 669 THEN 'Fair'
        WHEN credit_score BETWEEN 670 AND 739 THEN 'Good'
        WHEN credit_score BETWEEN 740 AND 799 THEN 'Very Good'
        WHEN credit_score >= 800 THEN 'Excellent'
    END AS credit_bucket,
    status,
    l.city,
    l.country,
    l.region,
    device_id,
    ingestion_date,
    source,
    CURRENT_TIMESTAMP AS run_ts
FROM 
    {{ ref('silver_customers') }} AS c
JOIN 
    {{ ref('silver_locations') }} AS l 
ON 
    c.location_id = l.location_id

