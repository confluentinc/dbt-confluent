{% macro skip_or_drop_existing(existing_relation, target_relation) %}
  {# If the relation already exists, either drop it (on --full-refresh) or skip re-creation.
     Returns early from the calling materialization if skipping. #}
  {# TODO: Compare the existing relation's schema against the expected one and raise
     a compilation error if they differ (e.g., column changes that require --full-refresh). #}
  {% if existing_relation %}
    {% if should_full_refresh() %}
      {{ drop_relation_if_exists(existing_relation) }}
    {% else %}
      {{ log("Relation " ~ existing_relation ~ " already exists. Skipping. Use --full-refresh to recreate.", info=True) }}
      {{ run_hooks(post_hooks, inside_transaction=False) }}
      {{ return({'relations': [target_relation]}) }}
    {% endif %}
  {% endif %}
{% endmacro %}
