{% macro normalize_brand(col) %}
  case
    when lower({{ col }}) in ('xiami','xaomi','xiaomi') then 'Xiaomi'
    when lower({{ col }}) in ('huwai','hauwei','huawei') then 'Huawei'
    when lower({{ col }}) in ('samsun','samsg','samsung') then 'Samsung'
    when lower({{ col }}) in ('appl','aple','apple') then 'Apple'
    else null
  end
{% endmacro %}