{% macro confluent__drop_view(relation) -%}
    {# Avoid using the default, not supported, CASCADE #}
    drop view if exists {{ relation.render() }}
{%- endmacro %}
