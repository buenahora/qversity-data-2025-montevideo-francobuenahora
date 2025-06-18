{% macro normalize_model(col) %}
  /**
   * normalize_model(col):
   *   1) Capitaliza la primera letra de cada palabra
   *   2) Elimina TODO lo que no sea letra, número o espacio
   *   3) Inserta un espacio entre letra y dígito
   */
  {% set step1 = "INITCAP(" ~ col ~ ")" %}
  {% set step2 = "REGEXP_REPLACE(" ~ step1 ~ ", '[^A-Za-z0-9 ]', '', 'g')" %}
  {% set step3 = "REGEXP_REPLACE(" ~ step2 ~ ", '([A-Za-z])([0-9])', E'\\\\1 \\\\2', 'g')" %}
  {{ step3 }}
{% endmacro %}