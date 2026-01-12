{% macro confluent__post_snapshot(staging_relation) %}
  {# Clean up the snapshot staging temp table after snapshot completes #}
  {% do drop_relation(staging_relation) %}
{% endmacro %}
