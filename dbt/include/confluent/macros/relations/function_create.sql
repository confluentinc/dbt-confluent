{% macro get_create_function_sql(relation, udf) -%}
  {#- `udf` is the dict returned by adapter.validate_function_config. -#}
  create function {{ relation.render() }}
    as '{{ adapter.escape_string_literal(udf.class_name) }}'
    {% if udf.language == 'python' -%} language python {% endif -%}
    using jar 'confluent-artifact://{{ udf.artifact_id }}'
    {%- if udf.connections %}
    using connections ({% for c in udf.connections %}'{{ adapter.escape_string_literal(c) }}'{{ ", " if not loop.last }}{% endfor %})
    {%- endif %}
{%- endmacro %}
