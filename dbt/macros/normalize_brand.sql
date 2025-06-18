{% macro normalize_brand(col) %}
-- Esta macro normaliza los nombres de marcas de dispositivos móviles a un formato consistente.
-- Se espera que el parámetro `col` sea el nombre de la columna que contiene los nombres de las marcas.
  case
    when lower({{ col }}) in ('xiami','xaomi','xiaomi') then 'Xiaomi'
    when lower({{ col }}) in ('huwai','hauwei','huawei') then 'Huawei'
    when lower({{ col }}) in ('samsun','samsg','samsung') then 'Samsung'
    when lower({{ col }}) in ('appl','aple','apple') then 'Apple'
    else null
  end
{% endmacro %}