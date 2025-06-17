{{ config(materialized='table') }}

WITH exploded AS (
    SELECT
        jsonb_array_elements_text(contracted_services::jsonb) AS service_name
    FROM 
        {{ ref('staging_mobile_raw') }}
    WHERE 
        contracted_services ~ '^\[.*\]$'
)

SELECT DISTINCT
    md5(service_name) AS service_id,
    service_name
FROM 
    exploded
