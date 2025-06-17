{% macro normalize_city(col) %}
  case
    /* México */
    when lower({{ col }}) in (
      'ciudad de méxico','ciudad de mexico','cdmx'
    ) then 'Ciudad de México'
    /* Argentina */
    when lower({{ col }}) in (
      'cordoba','córdoba','coroba'
    ) then 'Córdoba'
    when lower({{ col }}) in (
      'buenos aires'
    ) then 'Buenos Aires'
    when lower({{ col }}) in (
      'rosario'
    ) then 'Rosario'
    /* Chile */
    when lower({{ col }}) in (
      'santiago','santigo'
    ) then 'Santiago'
    when lower({{ col }}) in (
      'valparaiso','valparaíso'
    ) then 'Valparaíso'
    when lower({{ col }}) in (
      'concepcion','concepción'
    ) then 'Concepción'
    /* Colombia */
    when lower({{ col }}) in (
      'medelin','medellin'
    ) then 'Medellín'
    when lower({{ col }}) in (
      'cali','cal'
    ) then 'Cali'
    when lower({{ col }}) in (
      'barranquilla'
    ) then 'Barranquilla'
    /* Perú */
    when lower({{ col }}) in (
      'lima'
    ) then 'Lima'
    when lower({{ col }}) in (
      'arequipa','areqipa'
    ) then 'Arequipa'
    when lower({{ col }}) in (
      'trujillo'
    ) then 'Trujillo'
    /* México (ciudades de provincia) */
    when lower({{ col }}) in (
      'guadaljara','guadalajara'
    ) then 'Guadalajara'
    when lower({{ col }}) in (
      'monterrey'
    ) then 'Monterrey'
    /* por defecto, valores desconocidos */
    else 'Unknown'
  end
{% endmacro %}
