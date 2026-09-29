{#-- Prepend the profile's isolation_prefix to every relation name, so several
     developers (or CI runs) can build the same project in one Kafka cluster
     without clobbering each other's topics. The prefix defaults to empty,
     leaving names unchanged. --#}
{% macro confluent__generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- set base = default__generate_alias_name(custom_alias_name, node) | trim -%}
  {{ (target.isolation_prefix or '') ~ base }}
{%- endmacro %}
