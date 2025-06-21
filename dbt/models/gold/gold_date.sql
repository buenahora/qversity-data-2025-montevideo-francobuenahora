{{ config(materialized='table') }}

WITH bounds AS (
  SELECT
    DATE_TRUNC('month', MIN(registration_date)) AS start_month,
    DATE_TRUNC('month', MAX(registration_date)) AS end_month
  FROM {{ ref('silver_customers') }}
),

date_series AS (
  SELECT
    DATE_TRUNC('month', dd)::DATE AS date_month
  FROM 
    bounds,

  -- Genera una serie de fechas mensuales entre el mes de inicio y el mes de fin
  LATERAL generate_series(bounds.start_month, bounds.end_month, interval '1 month') dd
)

SELECT
  date_month,
  EXTRACT(YEAR FROM date_month) AS year,
  EXTRACT(MONTH FROM date_month) AS month,
  TO_CHAR(date_month, 'Month') AS month_name,
  EXTRACT(QUARTER FROM date_month) AS quarter,
  date_month = DATE_TRUNC('month', CURRENT_DATE) AS is_current_month,
  CURRENT_TIMESTAMP AS run_ts
FROM 
  date_series