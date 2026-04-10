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
