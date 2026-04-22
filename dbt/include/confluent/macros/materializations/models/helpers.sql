{% macro validate_distributed_by_config() %}
  {# Validate the `distributed_by` config and return the user's dict (or none).
     Reused by both the renderer and the drift checker so they fail consistently
     before either reads any field beyond `columns` / `buckets`. #}
  {%- set dist = config.get('distributed_by') -%}
  {%- if dist is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if dist is not mapping -%}
    {% do exceptions.raise_compiler_error("'distributed_by' config must be a mapping with a non-empty 'columns' list") %}
  {%- endif -%}
  {%- set columns = dist.get('columns') -%}
  {%- if columns is string or columns is not sequence or not columns -%}
    {% do exceptions.raise_compiler_error("'distributed_by' config requires a non-empty 'columns' list") %}
  {%- endif -%}
  {%- for col in columns -%}
    {%- if col is not string or not col -%}
      {% do exceptions.raise_compiler_error("'distributed_by.columns' must contain only non-empty strings") %}
    {%- endif -%}
  {%- endfor -%}
  {{ return(dist) }}
{% endmacro %}


{% macro get_distributed_by_clause() %}
  {# Render `DISTRIBUTED BY HASH(col1, ...) [INTO N BUCKETS]` from the
     `distributed_by` config, or nothing if the config is unset.
     Flink only supports the HASH strategy today. #}
  {%- set dist = validate_distributed_by_config() -%}
  {%- if dist is not none -%}
    {%- set columns = dist.get('columns') -%}
    {%- set buckets = dist.get('buckets') -%}
    DISTRIBUTED BY HASH({% for col in columns %}`{{ col }}`{%- if not loop.last %}, {% endif -%}{% endfor %})
    {%- if buckets %} INTO {{ buckets }} BUCKETS{% endif -%}
  {%- endif -%}
{% endmacro %}


{% macro delete_statement_if_exists(statement_name) %}
  {# Delete an existing Flink statement by name so we can re-create it.
     No-op if the statement doesn't exist (confluent-sql handles 404). #}
  {% if execute %}
    {% do adapter.delete_statement(statement_name) %}
  {% endif %}
{% endmacro %}


{% macro skip_or_drop_existing(existing_relation, target_relation, has_select_query=true) %}
  {# If the relation already exists, either drop it (on --full-refresh) or skip.
     Returns true if the caller should skip (relation exists and no full refresh).
     Before skipping, checks for schema drift according to on_schema_drift config.
     has_select_query: true if the model SQL is a SELECT (table, streaming_table),
                       false if it's column definitions (streaming_source). #}
  {% if existing_relation %}
    {% if should_full_refresh() %}
      {{ delete_statement_if_exists(get_statement_name()) }}
      {{ delete_statement_if_exists(get_statement_name('-ddl')) }}
      {{ drop_relation_if_exists(existing_relation) }}
      {{ return(false) }}
    {% else %}
      {% set on_schema_drift = config.get('on_schema_drift', 'fail') %}
      {% if on_schema_drift == 'ignore' %}
        {{ log("Relation " ~ existing_relation ~ " already exists. Skipping without drift check (on_schema_drift='ignore').", info=True) }}
      {% elif on_schema_drift == 'fail' %}
        {{ check_for_schema_drift(existing_relation, has_select_query) }}
        {{ log("Relation " ~ existing_relation ~ " already exists. Skipping. Use --full-refresh to recreate.", info=True) }}
      {% else %}
        {% set msg = "Invalid value for on_schema_drift ('%s'). Expected 'ignore' or 'fail'." % on_schema_drift %}
        {% do exceptions.raise_compiler_error(msg) %}
      {% endif %}
      {{ return(true) }}
    {% endif %}
  {% else %}
    {# No relation exists, but orphaned Flink statements may linger if the table
       was dropped without deleting its statements (e.g. external cleanup).
       Delete them so the materialization can create fresh ones. #}
    {{ delete_statement_if_exists(get_statement_name()) }}
    {{ delete_statement_if_exists(get_statement_name('-ddl')) }}
  {% endif %}
  {{ return(false) }}
{% endmacro %}


{% macro check_for_schema_drift(existing_relation, has_select_query) %}
  {# Compare the existing table against what the model would produce and
     raise a compilation error on any drift (columns, types, WITH options,
     or DISTRIBUTED BY).

     Round-trips to Confluent are expensive, so we batch every metadata
     read into a single UNION ALL against INFORMATION_SCHEMA.  The temp
     table is the only way to get a normalized expected schema without
     parsing types ourselves.

     has_select_query: true if the model SQL is a SELECT (table,
                       streaming_table); false if it's column definitions
                       (streaming_source). #}

  {% set temp_table_name = adapter.generate_schema_check_temp_name(this.identifier, invocation_id) %}
  {% set temp_relation = adapter.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=temp_table_name,
    type='table'
  ) %}

  {% if has_select_query %}
    {% call statement('create_temp_table', hidden=True) %}
      CREATE TABLE {{ temp_relation }} AS
      SELECT * FROM (
        {{ sql }}
      ) WHERE FALSE
    {% endcall %}
  {% else %}
    {# streaming_source: column definitions are the source of truth. #}
    {% call statement('create_temp_table', hidden=True) %}
      CREATE TABLE {{ temp_relation }} ( {{ sql }} )
    {% endcall %}
  {% endif %}

  {{ get_drift_catalog(existing_relation, temp_relation) }}

  {% call statement('drop_temp_table', hidden=True) %}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {% endcall %}

  {% do adapter.check_schema_drift(
    existing_relation | string,
    existing_relation.identifier,
    temp_relation.identifier,
    load_result('get_drift_catalog').table,
    config.get('with', {}),
    validate_distributed_by_config()
  ) %}
{% endmacro %}


{% macro get_drift_catalog(existing_relation, temp_relation) %}
  {# Fetch every piece of metadata the drift check needs in one query.
     The result is a sparse table with a `section` discriminator and a
     `table_name` discriminator (existing vs temp for the COLUMNS section).
     Confluent's INFORMATION_SCHEMA only supports the primitives we use here:
     SELECT, WHERE with =/<>/IS NULL/IS NOT NULL/AND/OR, UNION ALL, AS,
     and CAST(NULL AS dt). #}
  {% call statement('get_drift_catalog', fetch_result=True, hidden=True) %}
    SELECT
      'COLUMNS' AS section,
      TABLE_NAME AS table_name,
      COLUMN_NAME AS col_name,
      FULL_DATA_TYPE AS data_type,
      DISTRIBUTION_ORDINAL_POSITION AS dist_position,
      CAST(NULL AS STRING) AS option_key,
      CAST(NULL AS STRING) AS option_value,
      CAST(NULL AS STRING) AS is_distributed,
      CAST(NULL AS STRING) AS dist_algorithm,
      CAST(NULL AS INT) AS dist_buckets
    FROM INFORMATION_SCHEMA.`COLUMNS`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND IS_HIDDEN = 'NO'
      AND (TABLE_NAME = '{{ existing_relation.identifier }}'
           OR TABLE_NAME = '{{ temp_relation.identifier }}')
    UNION ALL
    SELECT
      'TABLES' AS section,
      TABLE_NAME AS table_name,
      CAST(NULL AS STRING) AS col_name,
      CAST(NULL AS STRING) AS data_type,
      CAST(NULL AS INT) AS dist_position,
      CAST(NULL AS STRING) AS option_key,
      CAST(NULL AS STRING) AS option_value,
      IS_DISTRIBUTED AS is_distributed,
      DISTRIBUTION_ALGORITHM AS dist_algorithm,
      DISTRIBUTION_BUCKETS AS dist_buckets
    FROM INFORMATION_SCHEMA.`TABLES`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND TABLE_NAME = '{{ existing_relation.identifier }}'
    UNION ALL
    SELECT
      'TABLE_OPTIONS' AS section,
      TABLE_NAME AS table_name,
      CAST(NULL AS STRING) AS col_name,
      CAST(NULL AS STRING) AS data_type,
      CAST(NULL AS INT) AS dist_position,
      OPTION_KEY AS option_key,
      OPTION_VALUE AS option_value,
      CAST(NULL AS STRING) AS is_distributed,
      CAST(NULL AS STRING) AS dist_algorithm,
      CAST(NULL AS INT) AS dist_buckets
    FROM INFORMATION_SCHEMA.`TABLE_OPTIONS`
    WHERE TABLE_CATALOG_ID = '{{ existing_relation.database }}'
      AND TABLE_SCHEMA = '{{ existing_relation.schema }}'
      AND TABLE_NAME = '{{ existing_relation.identifier }}'
  {% endcall %}
{% endmacro %}
