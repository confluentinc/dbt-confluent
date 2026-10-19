{#-- Prepend the profile's resource_name_prefix to every relation name, so several
     developers can build the same project in one Kafka cluster
     without clobbering each other's topics. --#}
{% macro confluent__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set base = default__generate_alias_name(custom_alias_name, node) | trim -%}
  {{ (target.resource_name_prefix or '') ~ base }}
{%- endmacro %}
