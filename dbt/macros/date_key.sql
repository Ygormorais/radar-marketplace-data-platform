{% macro date_key(expression) -%}
CAST(CONVERT(CHAR(8), CAST({{ expression }} AS DATE), 112) AS INT)
{%- endmacro %}

