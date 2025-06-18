{% macro normalize_plan_type(col) %}
-- Esta macro normaliza los tipos de planes a un formato consistente.
-- Se espera que el parámetro `col` sea el nombre de la columna que contiene los tipos de planes.
  CASE
    WHEN TRIM(LOWER({{ col }})) IN ('control','ctrl','contrrol') THEN 'Control'
    WHEN TRIM(LOWER({{ col }})) IN ('prepago','pre_pago','pre','pos') THEN 'Prepago'
    WHEN TRIM(LOWER({{ col }})) IN ('pospago','post_pago','post-pago','pospago') THEN 'Pospago'
    ELSE 'Unknown'
  END
{% endmacro %}