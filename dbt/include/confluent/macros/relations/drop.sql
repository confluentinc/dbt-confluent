{% macro confluent__get_drop_sql(relation) -%}
    {%- if relation.is_view -%}
        {{ drop_view(relation) }}
    {%- elif relation.is_table -%}
        {{ drop_table(relation) }}
    {%- elif relation.is_materialized_view -%}
        {{ drop_materialized_view(relation) }}
    {%- else -%}
        {# We need to customize this to avoid using CASCADE here #}
        drop {{ relation.type }} if exists {{ relation.render() }}
    {%- endif -%}
{%- endmacro %}

