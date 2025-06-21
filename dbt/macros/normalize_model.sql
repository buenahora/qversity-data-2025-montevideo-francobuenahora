{% macro normalize_model(col) %}
 -- Este macro normaliza un nombre de modelo
  -- al formato de nombre de modelo estándar, eliminando caracteres especiales

  {% set step1 = "INITCAP(" ~ col ~ ")" %}
  {% set step2 = "REGEXP_REPLACE(" ~ step1 ~ ", '[^A-Za-z0-9 ]', '', 'g')" %}
  {% set step3 = "REGEXP_REPLACE(" ~ step2 ~ ", '([A-Za-z])([0-9])', E'\\\\1 \\\\2', 'g')" %}
  {{ step3 }}
{% endmacro %}