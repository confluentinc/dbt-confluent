{% macro generate_bindings_row(agate_table, column_override) %}
  ({%- for column in agate_table.column_names -%}

    {# We only receive strings from the CSV. Since flink SQL does not
    automatically try to cast strings to the known column type,
    we have to do it explicitly here if possible #}
    {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
    {%- set type = column_override.get(column, inferred_type) -%}

    {%- if type in ("STRING", "VARCHAR", "TEXT") -%}
      {{ get_binding_char() }}
    {%- else -%}
      cast({{ get_binding_char() }} as {{type}})
    {%- endif -%}

    {%- if not loop.last%},{%- endif %}
  {%- endfor -%})
{% endmacro %}

{% macro confluent__load_csv_rows(model, agate_table) %}
  {% set batch_size = get_batch_size() %}

  {%- if agate_table.rows | length > batch_size -%}
    {% set msg %} Can't use a seed with more than {{ batch_size }} values! {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {%- endif -%}

  {% set column_override = model['config'].get('column_types', {}) %}
  {% set statements = [] %}
  {% set bindings_row = generate_bindings_row(agate_table, column_override) %}

  {# This is needed to convert from agate's formats to python tuples #}
  {% set bindings = [] %}
  {% for row in agate_table.rows %}
      {% do bindings.extend(row) %}
  {% endfor %}

  {% set sql %}
    insert into {{ this.render() }} values
    {% for i in range(agate_table.rows | length) -%}
      {{ bindings_row }} {%- if not loop.last %},{%- endif %}
    {%- endfor %}
  {% endset %}

  {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

  {# Return SQL so we can render it out into the compiled files #}
  {{ return(sql) }}
{% endmacro %}
