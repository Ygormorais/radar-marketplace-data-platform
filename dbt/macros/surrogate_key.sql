{% macro surrogate_key(expressions) -%}
(
    CASE
        WHEN CONVERT(BIGINT, SUBSTRING(HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                {%- for expression in expressions %}
                COALESCE(CONVERT(NVARCHAR(4000), {{ expression }}), N'∅')
                {%- if not loop.last %}, {% endif -%}
                {%- endfor %}
            )
        ), 1, 8)) = -9223372036854775808 THEN 0
        ELSE ABS(CONVERT(BIGINT, SUBSTRING(HASHBYTES(
            'SHA2_256',
            CONCAT_WS('|',
                {%- for expression in expressions %}
                COALESCE(CONVERT(NVARCHAR(4000), {{ expression }}), N'∅')
                {%- if not loop.last %}, {% endif -%}
                {%- endfor %}
            )
        ), 1, 8)))
    END
)
{%- endmacro %}

