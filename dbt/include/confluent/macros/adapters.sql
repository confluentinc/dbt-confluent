{% macro confluent__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {% if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{% endmacro %}

{% macro confluent__create_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support CREATE for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__drop_relation(relation) -%}
  {% if relation.is_table -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP TABLE IF EXISTS {{ relation }}
    {% endcall %}
  {%- elif relation.is_view -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP VIEW IF EXISTS {{ relation }}
    {% endcall %}
  {%- else -%}
    {% set msg %}
      This adapter (confluent) does not support DROP for {{ relation }}.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {%- endif -%}
{% endmacro %}

{% macro confluent__drop_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support DROP for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__list_relations_without_caching(schema_relation) -%}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    SELECT
      TABLE_CATALOG_ID as database,
      TABLE_NAME as name,
      TABLE_SCHEMA as schema,
      TABLE_TYPE as type
    FROM
      INFORMATION_SCHEMA.`TABLES`
    WHERE
      TABLE_CATALOG_ID = '{{ schema_relation.database }}'
      AND TABLE_SCHEMA = '{{ schema_relation.schema }}'
  {% endcall %}

  {# Transform Flink SQL type names to dbt-compatible lowercase types #}
  {# We have to do this in jinja because we can't use CASE on INFORMATION_SCHEMA for some reason. #}
  {% set result_table = load_result('list_relations_without_caching').table %}
  {% set rows = [] %}
  {% for row in result_table %}
    {% set type_mapping = {
      'BASE TABLE': 'table',
      'VIEW': 'view',
      'EXTERNAL TABLE': 'external',
      'SYSTEM TABLE': 'system_table'
    } %}
    {% set normalized_type = type_mapping.get(row['type'], row['type'] | lower) %}
    {% do rows.append((
      row['database'],
      row['name'],
      row['schema'],
      normalized_type
    )) %}
  {% endfor %}

  {{ return(rows) }}
{% endmacro %}

{% macro confluent__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True) -%}
    SELECT
      SCHEMA_NAME as schema
    FROM
      {{ database }}.`INFORMATION_SCHEMA`.`SCHEMATA`
    WHERE
      `SCHEMA_NAME` <> 'INFORMATION_SCHEMA'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro confluent__truncate_relation(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support TRUNCATE. Relation: {{ relation }}.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if | replace("!=", "<>") }} as should_warn,
      {{ fail_calc }} {{ error_if | replace("!=", "<>") }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}


{% macro confluent__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Flink SQL does not support TEMPORARY tables, so we ignore the temporary parameter
     and create a regular table with a temporary name instead #}
  create table
    {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
  {% endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}


{% macro confluent__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    SELECT
      COLUMN_NAME as column_name,
      DATA_TYPE as data_type,
      CAST(NULL AS INT) as character_maximum_length,
      CAST(NULL AS INT) as numeric_precision,
      CAST(NULL AS INT) as numeric_scale
    FROM
      INFORMATION_SCHEMA.`COLUMNS`
    WHERE
      TABLE_CATALOG_ID = '{{ relation.database }}'
      AND TABLE_SCHEMA = '{{ relation.schema }}'
      AND TABLE_NAME = '{{ relation.identifier }}'
      AND COLUMN_NAME <> '$rowtime'
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}


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
