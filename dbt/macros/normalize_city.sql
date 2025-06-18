{% macro normalize_city(col) %}
-- Este macro normaliza los nombres de ciudades a un formato consistente.
-- Se espera que el parámetro `col` sea el nombre de la columna que contiene los nombres de las ciudades.
  CASE
    /* México */
    WHEN TRIM(LOWER({{ col }})) IN (
      'ciudad de méxico','ciudad de mexico','cdmx'
    ) THEN 'Ciudad de México'

    /* Argentina */
    WHEN TRIM(LOWER({{ col }})) IN (
      'cordoba','córdoba','coroba'
    ) THEN 'Córdoba'

    WHEN TRIM(LOWER({{ col }})) IN (
      'buenos aires'
    ) THEN 'Buenos Aires'

    WHEN TRIM(LOWER({{ col }})) IN (
      'rosario'
    ) THEN 'Rosario'

    /* Chile */
    WHEN TRIM(LOWER({{ col }})) IN (
      'santiago','santigo'
    ) THEN 'Santiago'

    WHEN TRIM(LOWER({{ col }})) IN (
      'valparaiso','valparaíso'
    ) THEN 'Valparaíso'

    WHEN TRIM(LOWER({{ col }})) IN (
      'concepcion','concepción'
    ) THEN 'Concepción'

    /* Colombia */
    WHEN TRIM(LOWER({{ col }})) IN (
      'medelin','medellin'
    ) THEN 'Medellín'

    WHEN TRIM(LOWER({{ col }})) IN (
      'cali','cal'
    ) THEN 'Cali'

    WHEN TRIM(LOWER({{ col }})) IN (
      'barranquilla','barranquila','barranquilla'
    ) THEN 'Barranquilla'

    /* Perú */
    WHEN TRIM(LOWER({{ col }})) IN (
      'lima'
    ) THEN 'Lima'

    WHEN TRIM(LOWER({{ col }})) IN (
      'arequipa','areqipa'
    ) THEN 'Arequipa'

    WHEN TRIM(LOWER({{ col }})) IN (
      'trujillo'
    ) THEN 'Trujillo'

    /* México (ciudades de provincia) */
    WHEN TRIM(LOWER({{ col }})) IN (
      'guadaljara','guadalajara'
    ) THEN 'Guadalajara'

    WHEN TRIM(LOWER({{ col }})) IN (
      'monterrey'
    ) THEN 'Monterrey'

    /* por defecto, valores desconocidos */
    ELSE 'Unknown'
  END
{% endmacro %}
