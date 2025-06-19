{{ config(materialized = 'table') }}

SELECT
  device_id,
  device_brand,
  device_model,
  CURRENT_TIMESTAMP AS run_ts
FROM 
  {{ ref('silver_devices') }}