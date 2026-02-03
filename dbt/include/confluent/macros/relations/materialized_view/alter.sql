{% macro confluent__get_materialized_view_configuration_changes(existing_relation, new_config) %}
  {# Return none to indicate no configuration changes detected.
     This causes the materialization to call refresh_materialized_view (which is a no-op). #}
  {{ return(none) }}
{% endmacro %}
