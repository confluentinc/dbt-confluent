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
