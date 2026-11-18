{% macro confluent__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True, hidden=True) %}
    SELECT
      COLUMN_NAME as column_name,
      FULL_DATA_TYPE as data_type,
      CAST(NULL AS INT) as character_maximum_length,
      CAST(NULL AS INT) as numeric_precision,
      CAST(NULL AS INT) as numeric_scale
    FROM
      INFORMATION_SCHEMA.`COLUMNS`
    WHERE
      TABLE_CATALOG_ID = '{{ relation.database }}'
      AND TABLE_SCHEMA = '{{ relation.schema }}'
      AND TABLE_NAME = '{{ relation.identifier }}'
      AND IS_HIDDEN = 'NO'
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}


{% macro validate_column_data_types(columns) %}
  {#-- Validate that data_type fields don't contain constraint keywords.
       These keywords would cause SQL errors in CAST expressions.
       Constraints should be in the 'constraints' section instead. --#}
  {% set constraint_keywords = ['NOT NULL', 'VIRTUAL', 'METADATA'] %}

  {% for i in columns %}
    {% set col = columns[i] %}
    {% set data_type = col.get('data_type', '') | upper %}

    {% for keyword in constraint_keywords %}
      {% if keyword in data_type %}
        {% set msg %}
Column '{{ col['name'] }}' has constraint keyword in the data_type field.

Found:
  - name: {{ col['name'] }}
    data_type: {{ col['data_type'] }}

Constraint keywords like '{{ keyword }}' cannot appear in data type definitions.
Use the 'constraints' section instead to specify column constraints.
        {% endset %}
        {{ exceptions.raise_compiler_error(msg) }}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}


{% macro confluent__get_empty_schema_sql(columns) %}
  {#-- Validate column data types before generating SQL --#}
  {{ validate_column_data_types(columns) }}
  {#-- Delegate to default implementation --#}
  {{ return(default__get_empty_schema_sql(columns)) }}
{% endmacro %}


{% macro confluent__get_assert_columns_equivalent(sql) %}
  {#- A copy of dbt's assert_columns_equivalent (dbt-adapters
      relations/column/columns_spec_ddl.sql), with one added step: adapter.fit_contract_columns
      gives a SELECT column typed CHAR(n) or BINARY(n), like a string or binary literal, its yml
      column's type when that's STRING or BYTES.
      So the type comparison passes it, and a contract error raised for another column doesn't list
      it as a mismatch. -#}

  {#-- First ensure the user has defined 'columns' in yaml specification --#}
  {%- set user_defined_columns = model['columns'] -%}
  {%- if not user_defined_columns -%}
      {{ exceptions.raise_contract_error([], []) }}
  {%- endif -%}

  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql, config.get('sql_header', none)) -%}
  {#--Obtain the column schema provided by the schema file by generating an 'empty schema' query from the model's columns. #}
  {%- set schema_file_provided_columns = get_column_schema_from_query(get_empty_schema_sql(user_defined_columns)) -%}
  {%- set sql_file_provided_columns = adapter.fit_contract_columns(sql_file_provided_columns, schema_file_provided_columns) -%}

  {#-- create dictionaries with name and formatted data type and strings for exception #}
  {%- set sql_columns = format_columns(sql_file_provided_columns) -%}
  {%- set yaml_columns = format_columns(schema_file_provided_columns)  -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_columns -%}
    {%- set yaml_col = [] -%}
    {%- for this_col in yaml_columns -%}
      {%- if this_col['name'] == sql_col['name'] -%}
        {%- do yaml_col.append(this_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not yaml_col -%}
      {#-- Column with name not found in yaml #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
    {%- if sql_col['formatted'] != yaml_col[0]['formatted'] -%}
      {#-- Column data types don't match #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
  {%- endfor -%}

{% endmacro %}
