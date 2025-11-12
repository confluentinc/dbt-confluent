{% macro create_schema(relation) -%}
  {{ return(adapter.dispatch('create_schema')(relation)) }}
{%- endmacro %}

{% macro check_schema_exists(relation) -%}
  {{ return(adapter.dispatch('check_schema_exists')(relation)) }}
{%- endmacro %}

{% macro drop_schema(relation) -%}
  {{ return(adapter.dispatch('drop_schema')(relation)) }}
{%- endmacro %}

{% macro drop_relation(relation) -%}
  {{ return(adapter.dispatch('drop_relation')(relation)) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ return(adapter.dispatch('generate_schema_name')(custom_schema_name, node)) }}
{%- endmacro %}

{% macro confluentcloud__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {% if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{% endmacro %}

{% macro confluentcloud__check_schema_exists(information_schema,schema) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__check_schema_exists: " ~ schema, info=True) }}
{% endmacro %}

{% macro confluentcloud__create_schema(relation) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__create_schema: " ~ relation.schema, info=True) }}
{% endmacro %}

{% macro confluentcloud__alter_column_type(relation,column_name,new_column_type) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__alter_column_type: " ~ relation, info=True) }}
{% endmacro %}

{% macro confluentcloud__drop_relation(relation) -%}
  {{ log("!!! EXECUTING confluentcloud__drop_relation: " ~ relation, info=True) }}
  DROP TABLE IF EXISTS {{ relation }}
{% endmacro %}

{% macro confluentcloud__drop_schema(relation) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__drop_schema: " ~ relation, info=True) }}
{% endmacro %}

{% macro confluentcloud__get_columns_in_relation(relation) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__get_columns_is_relation: " ~ relation, info=True) }}
{% endmacro %}

{% macro confluentcloud__list_relations_without_caching(schema_relation) -%}
  {{ log("!!! EXECUTING confluentcloud__list_relations_without_caching: " ~ schema_relation.schema, info=True) }}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    SELECT
      TABLE_CATALOG_ID as database,
      TABLE_SCHEMA as schema,
      TABLE_NAME as name,
      TABLE_TYPE as type
    FROM
      INFORMATION_SCHEMA.`TABLES`
    WHERE
      TABLE_CATALOG_ID = '{{ schema_relation.database }}'
      AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
      AND TABLE_SCHEMA = '{{ schema_relation.schema }}'
  {% endcall %}

  {{ log("!!! RESULT confluentcloud__list_relations_without_caching: " ~ schema_relation, info=True) }}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro confluentcloud__list_schemas(database) -%}
  {{ log("!!! EXECUTING confluentcloud__list_schemas: " ~ database, info=True) }}
  {% call statement('list_schemas', fetch_result=True) -%}
    SELECT
      TABLE_SCHEMA as schema
    FROM
      INFORMATION_SCHEMA.`TABLES`
    WHERE
      TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
      AND TABLE_CATALOG_ID = '{{ database }}'
  {% endcall %}

  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro confluentcloud__rename_relation(from_relation, to_relation) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__rename_relation", info=True) }}
{% endmacro %}

{% macro confluentcloud__truncate_relation(relation) -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__truncate_relation", info=True) }}
{% endmacro %}

{% macro confluentcloud__current_timestamp() -%}
  {{ log("!!! EXECUTING NOT IMPLEMENTED confluentcloud__current_timestamp", info=True) }}
{% endmacro %}

{% macro default__load_csv_rows(model, agate_table) -%}
  {# HERE FOR REFERENCE, REMOVE LATER #}
  {% set batch_size = get_batch_size() %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}
  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {{ get_binding_char() }}
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

{% macro confluentcloud__load_csv_rows(model, agate_table) -%}
  {% set batch_size = get_batch_size() %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}
  {% set statements = [] %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set bindings = [] %}

      {% for row in chunk %}
          {% do bindings.extend(row) %}
      {% endfor %}

      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {% do adapter.string_literal(value) %}
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
