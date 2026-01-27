{% macro confluent__drop_table(relation) -%}
    {# Avoid using the default, not supported, CASCADE #}
    drop table if exists {{ relation.render() }}
{%- endmacro %}

