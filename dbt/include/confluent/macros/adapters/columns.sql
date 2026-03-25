{% macro confluent__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
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
       Constraints should be in the 'constraints' section instead. --#}
  {% set constraint_keywords = ['NOT NULL', 'VIRTUAL', 'METADATA', 'NOT ENFORCED'] %}

  {% for i in columns %}
    {% set col = columns[i] %}
    {% set data_type = col.get('data_type', '') | upper %}

    {% for keyword in constraint_keywords %}
      {% if keyword in data_type %}
        {% set clean_type = col['data_type'] | replace(keyword, '') | replace(keyword | lower, '') | trim %}
        {% set msg %}
Column '{{ col['name'] }}' has constraint keywords in the data_type field.

Found:
  - name: {{ col['name'] }}
    data_type: {{ col['data_type'] }}

With this adapter, constraints must be specified separately in the 'constraints' section:
  - name: {{ col['name'] }}
    data_type: {{ clean_type }}
    constraints:
      - type: not_null  # or primary_key, etc.
        {% endset %}
        {{ exceptions.raise_compiler_error(msg) }}
      {% endif %}
    {% endfor %}
  {% endfor %}
{% endmacro %}


{% macro confluent__get_empty_schema_sql(columns) %}
  {#-- Validate column data types before proceeding --#}
  {{ validate_column_data_types(columns) }}

  {#-- Call the default implementation --#}
  {{ return(dbt.default__get_empty_schema_sql(columns)) }}
{% endmacro %}
