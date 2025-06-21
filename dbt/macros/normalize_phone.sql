{% macro normalize_phone(column_name, default_country_code='57') %}
(
    /* 1. Deja solo dígitos */
    CASE
        /* ——— Ya contiene el código de país ——— */
        WHEN regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
             ~ '^{{ default_country_code }}' THEN
            '+' || regexp_replace({{ column_name }}, '[^0-9]', '', 'g')

        /* ——— Móvil local de 10 dígitos que empieza en 3 ——— */
        WHEN regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
             ~ '^3[0-9]{9}$' THEN
            '+' || '{{ default_country_code }}' ||
                   regexp_replace({{ column_name }}, '[^0-9]', '', 'g')

        /* ——— Formatos no reconocidos ——— */
        ELSE NULL   -- o lanza un error si prefieres
    END
)
{% endmacro %}
