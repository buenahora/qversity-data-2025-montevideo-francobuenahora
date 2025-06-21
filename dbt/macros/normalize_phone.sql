{% macro normalize_phone(column_name, default_country_code='57') %}
-- Este macro normaliza números de teléfono
-- al formato internacional, agregando el código de país por defecto
(
    CASE
        WHEN regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
             ~ '^{{ default_country_code }}' THEN
            '+' || regexp_replace({{ column_name }}, '[^0-9]', '', 'g')

        WHEN regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
             ~ '^3[0-9]{9}$' THEN
            '+' || '{{ default_country_code }}' ||
                   regexp_replace({{ column_name }}, '[^0-9]', '', 'g')

        ELSE NULL
    END
)
{% endmacro %}
