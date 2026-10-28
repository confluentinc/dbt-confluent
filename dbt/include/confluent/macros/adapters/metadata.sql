{#
  Catalog rows for just the given relations. The adapter calls this once per small chunk
  of relations (see ConfluentAdapter._get_one_catalog_by_relations). The INFORMATION_SCHEMA
  backend handles `OR` predicates poorly, so each relation gets its own equality-only
  SELECT, combined with UNION ALL. The column projections live in two CTEs, so each
  relation only adds a short filtered SELECT per CTE rather than repeating the projections.
#}
{% macro confluent__get_catalog_relations(information_schema, relations) -%}
    {% set rows = [] %}
    {# With no relations there is nothing to select (the query would have no final SELECT). #}
    {% if relations %}
    {% call statement('get_catalog_relations', fetch_result=True, hidden=True) %}
    with tbl as (
        select
            TABLE_CATALOG_ID as table_database,
            TABLE_SCHEMA as table_schema,
            TABLE_NAME as table_name,
            TABLE_TYPE as table_type,
            `COMMENT` as table_comment,
            CAST(NULL as STRING) as table_owner,
            CAST(NULL as STRING) as column_name,
            CAST(NULL as INT) as column_index,
            CAST(NULL as STRING) as column_type,
            CAST(NULL as STRING) as column_comment
        from INFORMATION_SCHEMA.`TABLES`
        where TABLE_CATALOG_ID = '{{ information_schema.database }}'
    ), col as (
        select
            TABLE_CATALOG_ID as table_database,
            TABLE_SCHEMA as table_schema,
            TABLE_NAME as table_name,
            CAST(NULL as STRING) as table_type,
            CAST(NULL as STRING) as table_comment,
            CAST(NULL as STRING) as table_owner,
            COLUMN_NAME as column_name,
            ORDINAL_POSITION as column_index,
            DATA_TYPE as column_type,
            `COMMENT` as column_comment
        from INFORMATION_SCHEMA.`COLUMNS`
        where TABLE_CATALOG_ID = '{{ information_schema.database }}'
            and IS_HIDDEN = 'NO'
    )
    {% for relation in relations %}
    {% if not loop.first %}union all{% endif %}
    select * from tbl
    where table_schema = '{{ adapter.escape_string_literal(relation.schema) }}'
      and table_name = '{{ adapter.escape_string_literal(relation.identifier) }}'
    union all
    select * from col
    where table_schema = '{{ adapter.escape_string_literal(relation.schema) }}'
      and table_name = '{{ adapter.escape_string_literal(relation.identifier) }}'
    {% endfor %}
    {% endcall %}
    {% for result in load_result('get_catalog_relations').table %}
        {% set new_row = {} %}
        {% do new_row.update(result) %}
        {% do new_row.update({'table_type': map_table_type(result['table_type'])}) %}
        {% do rows.append(new_row) %}
    {% endfor %}
    {% endif %}
    {{ return(rows) }}
{%- endmacro %}

{% macro map_table_type(table_type) %}
  {% set type_mapping = {
    'BASE TABLE': 'table',
    'VIEW': 'view',
    'EXTERNAL TABLE': 'external',
    'SYSTEM TABLE': 'system_table'
  } %}
  {{ return(type_mapping.get(table_type, table_type)) }}
{% endmacro %}

{% macro confluent__list_relations_without_caching(schema_relation) -%}
  {% call statement('list_relations_without_caching', fetch_result=True, hidden=True) -%}
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
    {% set normalized_type = map_table_type(row['type']) %}
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
  {% call statement('list_schemas', fetch_result=True, hidden=True) -%}
    SELECT
      SCHEMA_NAME as schema
    FROM
      {{ database }}.`INFORMATION_SCHEMA`.`SCHEMATA`
    WHERE
      `SCHEMA_NAME` <> 'INFORMATION_SCHEMA'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}
