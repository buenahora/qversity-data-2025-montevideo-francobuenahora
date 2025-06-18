{% macro normalize_country(col) %}
-- Esta macro normaliza los nombres de países a un formato consistente.
-- Se espera que el parámetro `col` sea el nombre de la columna que contiene los nombres de los países.
  CASE
    WHEN TRIM(LOWER({{ col }})) IN (
      'mex','mexco','mx','mejico','mexico'
    ) THEN 'Mexico'

    WHEN TRIM(LOWER({{ col }})) IN (
      'chl','chi','chile','chle','cl'
    ) THEN 'Chile'

    WHEN TRIM(LOWER({{ col }})) IN (
      'argentina','argentin','argentna','arg','ar'
    ) THEN 'Argentina'

    WHEN TRIM(LOWER({{ col }})) IN (
      'colombia','col','colomia','colombi','co'
    ) THEN 'Colombia'

    WHEN TRIM(LOWER({{ col }})) IN (
      'peru','per','pru','pe'
    ) THEN 'Peru'

    ELSE 'Unknown'
  END
{% endmacro %}
