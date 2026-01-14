{% macro confluent__get_catalog(information_schema, schemas) -%}
  {#
    This macro is not used by the Confluent adapter. Catalog generation is handled
    in Python via the _get_one_catalog() method in impl.py to work around
    Confluent Cloud's INFORMATION_SCHEMA limitations (no JOINs supported).

    This macro is kept as a fallback to satisfy dbt's macro resolution.
  #}
  {% set msg %}
    Catalog generation should be handled by the _get_one_catalog() Python method.
    This macro should not be called directly.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{%- endmacro %}

{% macro get_catalog_tables(information_schema, schemas) %}
    {% call statement('get_catalog_tables', fetch_result=True) %}
    select
        TABLE_CATALOG_ID as table_database,
        TABLE_SCHEMA as table_schema,
        TABLE_NAME as table_name,
        TABLE_TYPE as table_type
    from INFORMATION_SCHEMA.`TABLES`
    where TABLE_CATALOG_ID = '{{ information_schema.database }}'
        and TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
        and TABLE_TYPE <> 'SYSTEM TABLE'
    {% endcall %}
    {{ return(load_result('get_catalog_tables').table)}}
{% endmacro %}

{% macro get_catalog_columns(information_schema, schemas) %}
    {% call statement('get_catalog_columns', fetch_result=True) %}
    select
        TABLE_CATALOG_ID as table_database,
        TABLE_SCHEMA as table_schema,
        TABLE_NAME as table_name,
        COLUMN_NAME as column_name,
        ORDINAL_POSITION as column_index,
        DATA_TYPE as column_type
    from INFORMATION_SCHEMA.`COLUMNS`
    where TABLE_CATALOG_ID = '{{ information_schema.database }}'
        and TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
    {% endcall %}
    {{ return(load_result('get_catalog_columns').table)}}
{% endmacro %}
