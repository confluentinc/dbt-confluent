{% macro confluent__drop_materialized_view(relation) -%}
    {# Avoid using the default, not supported, CASCADE #}
    {# In Confluent Flink, materialized views are tables #}
    drop table if exists {{ relation.render() }}
{%- endmacro %}

