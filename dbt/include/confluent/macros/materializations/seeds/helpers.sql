{% macro confluent__load_csv_rows(model, agate_table) %}

  {% set batch_size = get_batch_size() %}
  {% set column_override = model['config'].get('column_types', {}) %}

  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}

  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {# Here is the customization: since flink SQL does not automatically
                  try to cast strings to the known column type, we have to do it
                  explicitly here if possible #}
                  {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
                  {%- set type = column_override.get(column, inferred_type) -%}
                  {%- if type in ("STRING", "VARCHAR", "TEXT") -%}
                    {{ get_binding_char() }}
                  {%- else -%}
                    cast({{ get_binding_char() }} as {{type}})
                  {%- endif -%}
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {# Return SQL so we can render it out into the compiled files #}
  {{ return(statements[0]) }}
{% endmacro %}
