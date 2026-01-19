{% macro confluent__current_timestamp() -%}
    CURRENT_TIMESTAMP
{%- endmacro %}

{% macro confluent__snapshot_string_as_time(timestamp) -%}
    {%- set result = "CAST('" ~ timestamp ~ "' AS TIMESTAMP)" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro confluent__snapshot_get_time() -%}
    CURRENT_TIMESTAMP
{%- endmacro %}
