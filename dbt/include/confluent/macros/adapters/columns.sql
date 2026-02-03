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
      AND IS_HIDDEN = 'NO'
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}
