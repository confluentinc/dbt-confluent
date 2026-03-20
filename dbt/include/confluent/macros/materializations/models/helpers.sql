{% macro skip_or_drop_existing(existing_relation, target_relation, has_select_query=true) %}
  {# If the relation already exists, either drop it (on --full-refresh) or skip.
     Returns true if the caller should skip (relation exists and no full refresh).
     Before skipping, checks for schema drift and raises an error if detected.
     has_select_query: true if the model SQL is a SELECT (table, streaming_table),
                       false if it's column definitions (streaming_source). #}
  {% if existing_relation %}
    {% if should_full_refresh() %}
      {{ drop_relation_if_exists(existing_relation) }}
      {{ return(false) }}
    {% else %}
      {{ check_for_schema_drift(existing_relation, has_select_query) }}
      {{ log("Relation " ~ existing_relation ~ " already exists. Skipping. Use --full-refresh to recreate.", info=True) }}
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

  {% if has_select_query %}
    {% set expected_columns = get_expected_columns_from_query(sql) %}
    {% if expected_columns is not none %}
      {% set existing_col_list = [] %}
      {% for col in existing_columns %}
        {% do existing_col_list.append(col.column_name | lower) %}
      {% endfor %}

      {% set expected_col_list = [] %}
      {% for col in expected_columns %}
        {% do expected_col_list.append(col.column_name | lower) %}
      {% endfor %}

      {% if existing_col_list | sort != expected_col_list | sort %}
        {% set msg %}
          Schema drift detected for '{{ existing_relation }}'.
          Existing columns: {{ existing_col_list | sort }}
          Expected columns: {{ expected_col_list | sort }}
          Use --full-refresh to recreate the table.
        {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
      {% endif %}
    {% endif %}
  {% endif %}

  {# -- WITH options comparison -- #}
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
  {# Get the columns that a SELECT query would produce by running it with WHERE FALSE LIMIT 0.
     Returns an agate table with column_name rows, or none if the query fails. #}
  {% call statement('get_expected_columns', fetch_result=True) %}
    SELECT * FROM ({{ model_sql }}) WHERE FALSE LIMIT 0
  {% endcall %}
  {% set result = load_result('get_expected_columns') %}

  {# Build a list of dicts with column_name to match get_existing_columns format #}
  {% set expected = [] %}
  {% for col_name in result.table.column_names %}
    {% do expected.append({'column_name': col_name}) %}
  {% endfor %}
  {{ return(expected) }}
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
