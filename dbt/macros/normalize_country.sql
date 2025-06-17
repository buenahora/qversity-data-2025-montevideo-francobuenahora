{% macro normalize_country(col) %}
  case
    when lower({{col}}) in ('mex','mexco','mx','mejico') then 'Mexico'
    when lower({{col}}) in ('chl','chi','chile','chle','cl') then 'Chile'
    when lower({{col}}) in ('argentina','argentin','argentna','arg','ar') then 'Argentina'
    when lower({{col}}) in ('colombia','col','colomia','colombi','co') then 'Colombia'
    when lower({{col}}) in ('peru','per','pru','pe') then 'Peru'
    else 'Unknown'
  end
{% endmacro %}