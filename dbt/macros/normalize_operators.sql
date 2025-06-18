{% macro normalize_operator(col) %}
-- Esta macro normaliza los nombres de operadores móviles a un formato consistente.
-- Se espera que el parámetro `col` sea el nombre de la columna que contiene los nombres de los operadores.
  case
    /* WOM */
    when lower({{ col }}) in (
      'wom','w0m','won'
    ) then 'WOM'
    /* Movistar */
    when lower({{ col }}) in (
      'movistar','movi','movistr','mov'
    ) then 'Movistar'
    /* Claro */
    when lower({{ col }}) in (
      'claro','clar','cla'
    ) then 'Claro'
    /* Tigo */
    when lower({{ col }}) in (
      'tigo','tig','tgo'
    ) then 'Tigo'
    /* por defecto, valores desconocidos */
    else 'Unknown'
  end
{% endmacro %}
