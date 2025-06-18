{{ config(materialized='table') }}

WITH exploded AS (
  SELECT
    customer_id,
    jsonb_array_elements_text(contracted_services::jsonb) AS service_name
  FROM 
    {{ ref('staging_mobile_raw') }}
  WHERE 
    contracted_services IS NOT NULL
    AND jsonb_typeof(contracted_services::jsonb) = 'array'
)

SELECT DISTINCT
  md5(concat_ws('|', e.customer_id, e.service_name)) AS customer_service_id,
  e.customer_id,
  s.service_id,
  CURRENT_TIMESTAMP AS run_ts
FROM exploded AS e
JOIN 
  {{ ref('silver_customers') }} AS c
ON 
  e.customer_id = c.customer_id
JOIN 
  {{ ref('silver_services') }} AS s
ON 
  e.service_name = s.service_name
