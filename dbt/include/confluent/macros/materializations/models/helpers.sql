{% macro skip_or_drop_existing(existing_relation, target_relation, has_select_query=true) %}
  {# If the relation already exists, either drop it (on --full-refresh) or skip.
     Returns true if the caller should skip (relation exists and no full refresh).
     Before skipping, checks for schema drift according to on_schema_change config.
     has_select_query: true if the model SQL is a SELECT (table, streaming_table),
                       false if it's column definitions (streaming_source). #}
  {% if existing_relation %}
    {% if should_full_refresh() %}
      {{ drop_relation_if_exists(existing_relation) }}
      {{ return(false) }}
    {% else %}
      {% set on_schema_change = config.get('on_schema_change', 'fail') %}
      {% if on_schema_change == 'ignore' %}
        {{ log("Relation " ~ existing_relation ~ " already exists. Skipping without drift check (on_schema_change='ignore').", info=True) }}
      {% elif on_schema_change == 'fail' %}
        {{ check_for_schema_drift(existing_relation, has_select_query) }}
        {{ log("Relation " ~ existing_relation ~ " already exists. Skipping. Use --full-refresh to recreate.", info=True) }}
      {% else %}
        {% set msg = "Invalid value for on_schema_change ('%s'). Expected 'ignore' or 'fail'." % on_schema_change %}
        {% do exceptions.raise_compiler_error(msg) %}
      {% endif %}
      {{ return(true) }}
    {% endif %}
  {% endif %}
  {{ return(false) }}
{% endmacro %}


{% macro check_for_schema_drift(existing_relation, has_select_query) %}
  {# Compare the existing table's columns and WITH options against what the model
     would produce. Raises a compilation error if there is any drift. #}

  {# -- Column comparison -- #}
  {% set existing_columns = get_existing_columns(existing_relation) %}

  {# Determine expected columns based on model type #}
  {% set expected_columns = none %}
  {% if has_select_query %}
    {% set expected_columns = get_expected_columns_from_query(sql) %}
  {% else %}
    {# For streaming_source, use column definitions from the model SQL #}
    {% set expected_columns = get_expected_columns_from_definition(sql) %}
  {% endif %}

  {% if expected_columns is not none %}
    {# Build maps for both name and type comparison #}
    {% set existing_col_map = {} %}
    {% for col in existing_columns %}
      {% do existing_col_map.update({col.column_name | lower: col.data_type | upper}) %}
    {% endfor %}

    {% set expected_col_map = {} %}
    {% for col in expected_columns %}
      {% do expected_col_map.update({col.column_name | lower: col.data_type | upper}) %}
    {% endfor %}

    {# Check if column names match #}
    {% set existing_col_names = existing_col_map.keys() | sort %}
    {% set expected_col_names = expected_col_map.keys() | sort %}

    {% if existing_col_names != expected_col_names %}
      {% set msg %}
        Schema drift detected for '{{ existing_relation }}'.
        Existing columns: {{ existing_col_names }}
        Expected columns: {{ expected_col_names }}
        Use --full-refresh to recreate the table.
      {% endset %}
      {% do exceptions.raise_compiler_error(msg) %}
    {% endif %}

    {# Check if data types match for each column #}
    {% for col_name, expected_type in expected_col_map.items() %}
      {% set existing_type = existing_col_map.get(col_name) %}
      {% if existing_type != expected_type %}
        {% set msg %}
          Schema drift detected for '{{ existing_relation }}'.
          Column '{{ col_name }}' type mismatch: existing='{{ existing_type }}', expected='{{ expected_type }}'.
          Use --full-refresh to recreate the table.
        {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
      {% endif %}
    {% endfor %}
  {% endif %}

  {# -- WITH options comparison -- #}
  {# Only verify that user-specified options exist with correct values.
     Allow additional options (connector defaults, system-generated) to exist. #}
  {% set expected_with = config.get('with', {}) %}
  {% if expected_with %}
    {% set existing_options = get_existing_table_options(existing_relation) %}
    {% for key, value in expected_with.items() %}
      {% if existing_options.get(key) != value %}
        {% set msg %}
          Table options drift detected for '{{ existing_relation }}'.
          Option '{{ key }}': existing='{{ existing_options.get(key, "<not set>") }}', expected='{{ value }}'.
          Use --full-refresh to recreate the table.
        {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}


{% macro get_existing_columns(relation) %}
  {# Fetch column name and type from INFORMATION_SCHEMA.COLUMNS for the given relation. #}
  {% call statement('get_existing_columns', fetch_result=True) %}
    SELECT
      COLUMN_NAME as column_name,
      FULL_DATA_TYPE as data_type
    FROM
      INFORMATION_SCHEMA.`COLUMNS`
    WHERE
      TABLE_CATALOG_ID = '{{ relation.database }}'
      AND TABLE_SCHEMA = '{{ relation.schema }}'
      AND TABLE_NAME = '{{ relation.identifier }}'
      AND IS_HIDDEN = 'NO'
  {% endcall %}
  {% set result = load_result('get_existing_columns') %}
  {{ return(result.table) }}
{% endmacro %}


{% macro get_expected_columns_from_query(model_sql) %}
  {# Get the columns that a SELECT query would produce by creating a temporary table
     and querying its schema from INFORMATION_SCHEMA. This gives us accurate data types.
     Returns a list of dicts with column_name and data_type. #}

  {# Create a unique temp table name #}
  {% set temp_table_name = '__dbt_tmp_schema_check_' ~ (modules.uuid.uuid4().hex) %}
  {% set temp_relation = adapter.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=temp_table_name,
    type='table'
  ) %}

  {# Create temp table from the query #}
  {% call statement('create_temp_table') %}
    CREATE TABLE {{ temp_relation }} AS
    SELECT * FROM (
      {{ model_sql }}
    ) WHERE FALSE
  {% endcall %}

  {# Query INFORMATION_SCHEMA for column names and types #}
  {% set expected_columns = get_existing_columns(temp_relation) %}

  {# Drop the temp table #}
  {% call statement('drop_temp_table') %}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {% endcall %}

  {{ return(expected_columns) }}
{% endmacro %}


{% macro get_expected_columns_from_definition(column_definitions) %}
  {# Get the columns from streaming_source column definitions by creating a temp table.
     The SQL column definitions are the source of truth, so we create a temporary table
     to let Flink parse the schema, then query INFORMATION_SCHEMA for normalized types.
     Returns a list of dicts with column_name and data_type. #}

  {# Create a unique temp table name #}
  {% set temp_table_name = '__dbt_tmp_schema_check_' ~ (modules.uuid.uuid4().hex) %}
  {% set temp_relation = adapter.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=temp_table_name,
    type='table'
  ) %}

  {# Create temp table from column definitions (without connector) #}
  {% call statement('create_temp_table') %}
    CREATE TABLE {{ temp_relation }} ( {{ column_definitions }} )
  {% endcall %}

  {# Query INFORMATION_SCHEMA for column names and types #}
  {% set expected_columns = get_existing_columns(temp_relation) %}

  {# Drop the temp table #}
  {% call statement('drop_temp_table') %}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {% endcall %}

  {{ return(expected_columns) }}
{% endmacro %}


{% macro get_existing_table_options(relation) %}
  {# Fetch WITH options from INFORMATION_SCHEMA.TABLE_OPTIONS for the given relation.
     Returns a dict of {option_key: option_value}. #}
  {% call statement('get_existing_table_options', fetch_result=True) %}
    SELECT
      OPTION_KEY,
      OPTION_VALUE
    FROM
      INFORMATION_SCHEMA.`TABLE_OPTIONS`
    WHERE
      TABLE_CATALOG_ID = '{{ relation.database }}'
      AND TABLE_SCHEMA = '{{ relation.schema }}'
      AND TABLE_NAME = '{{ relation.identifier }}'
  {% endcall %}
  {% set result = load_result('get_existing_table_options') %}
  {% set options = {} %}
  {% for row in result.table %}
    {% do options.update({row['OPTION_KEY']: row['OPTION_VALUE']}) %}
  {% endfor %}
  {{ return(options) }}
{% endmacro %}
