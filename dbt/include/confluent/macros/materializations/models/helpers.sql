{% macro drop_if_full_refresh(relation) %}
  -- If full refresh is set to true, drop the relation if it exists.
  -- Otherwise, raise an error and specify you need to set full_refresh to true.
  {% if relation %}
    {% if should_full_refresh() %}
      {{ drop_relation_if_exists(relation) }}
    {% else %}
      {% set msg %}
        Relation '{{ relation }}' already exists.
        Set full_refresh to true if you want to overwrite the existing relation
      {% endset %}
      {% do exceptions.raise_compiler_error(msg) %}
    {% endif %}
  {% endif %}
{% endmacro %}
