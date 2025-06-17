{% macro normalize_plan_type(col) %}
  case
    /* Control */
    when lower({{ col }}) in (
      'ctrl', 'control', 'contrrol'
    ) then 'Control'

    /* Prepago */
    when lower({{ col }}) in (
      'prepago', 'pre_pago', 'pre-pago', 'pre', 'prepago'
    ) then 'Prepago'

    /* Post-pago */
    when lower({{ col }}) in (
      'pospago', 'post_pago', 'post-pago', 'pos', 'pospago'
    ) then 'Postpago'

    else null
  end
{% endmacro %}