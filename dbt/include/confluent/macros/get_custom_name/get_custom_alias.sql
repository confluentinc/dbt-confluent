{#-- Prepend resource_name_prefix to every relation name to isolate each data
     engineer's topics. Its hyphens become underscores because dbt writes ephemeral
     CTE names (__dbt__cte__<alias>) into SQL unquoted, where '-' parses as minus. --#}
{% macro confluent__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set base = default__generate_alias_name(custom_alias_name, node) | trim -%}
  {{ (target.resource_name_prefix or '') | replace('-', '_') ~ base }}
{%- endmacro %}
