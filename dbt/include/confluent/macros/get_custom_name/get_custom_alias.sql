{#-- Prepend the profile's resource_name_prefix to every relation name, so several
     developers can build the same project in one Kafka cluster
     without clobbering each other's topics. Hyphens in the prefix become
     underscores: dbt writes ephemeral CTE names (__dbt__cte__<alias>) into SQL
     unquoted, where '-' would parse as minus. Only the prefix is rewritten, so
     aliases are unchanged when no prefix is set. --#}
{% macro confluent__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set base = default__generate_alias_name(custom_alias_name, node) | trim -%}
  {{ (target.resource_name_prefix or '') | replace('-', '_') ~ base }}
{%- endmacro %}
