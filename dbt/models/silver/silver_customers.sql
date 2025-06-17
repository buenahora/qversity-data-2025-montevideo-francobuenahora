{{ config(materialized='table') }}


 -- 1. filtros de calidad
WITH filtered AS (  
    SELECT *
    FROM {{ ref('staging_mobile_raw') }}
    where
        customer_id IS NOT NULL
        AND age BETWEEN 0 AND 119
        AND credit_score BETWEEN 300 AND 850
        AND initcap(status) IN ('Active','Inactive','Suspended')
),

 -- 2. priorizamos primero por fecha de ingesta
ranked AS (         
    SELECT
        customer_id,
        initcap(first_name)  AS first_name,
        initcap(last_name)   AS last_name,
        email,
        phone_number,
        age,
        registration_date,
        credit_score,
        initcap(status) AS status,
        ingestion_date,
        row_number() OVER (
            PARTITION BY customer_id
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM filtered
)

-- 3. seleccionamos el primer registro por cliente, para asegurar que no hay duplicados y que tenemos el más reciente
SELECT  *
FROM   ranked
WHERE   rn = 1