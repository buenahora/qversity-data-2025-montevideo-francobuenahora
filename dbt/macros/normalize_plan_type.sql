{% macro normalize_plan_type(col) %}
  CASE
    WHEN TRIM(LOWER({{ col }})) IN ('control','ctrl','contrrol') THEN 'Control'
    WHEN TRIM(LOWER({{ col }})) IN ('prepago','pre_pago','pre','pos') THEN 'Prepago'
    WHEN TRIM(LOWER({{ col }})) IN ('pospago','post_pago','post-pago','pospago') THEN 'Pospago'
    ELSE 'Unknown'
  END
{% endmacro %}