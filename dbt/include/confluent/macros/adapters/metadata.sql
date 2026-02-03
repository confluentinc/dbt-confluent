{% macro confluent__get_catalog(information_schema, schemas) -%}
    {% call statement('get_catalog', fetch_result=True) %}
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
        and TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
    union all
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
        and TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
        and IS_HIDDEN = 'NO'
    {% endcall %}
    {% set results = load_result('get_catalog').table %}
    {% set rows = [] %}
    {% for result in results %}
        {% set new_row = {} %}
        {% do new_row.update(result) %}
        {% do new_row.update({'table_type': map_table_type(result['table_type'])}) %}
        {% do rows.append(new_row) %}
    {% endfor %}
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
