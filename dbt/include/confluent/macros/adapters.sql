{% macro confluent__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {% if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{% endmacro %}

{% macro confluent__check_schema_exists(information_schema,schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
    SELECT count(*)
    FROM {{ information_schema }}.`TABLES`
    WHERE
      TABLE_SCHEMA == '{{ schema }}'
      AND TABLE_CATALOG_ID = '{{ information_schema.database }}'
  {% endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro confluent__create_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support CREATE for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__drop_relation(relation) -%}
  {% if relation.is_table -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP TABLE {{ relation }}
    {% endcall %}
  {%- elif relation.is_view -%}
    {% call statement('drop_relation', fetch_result=False) -%}
      DROP VIEW {{ relation }}
    {% endcall %}
  {%- else -%}
    {% set msg %}
      This adapter (confluent) does not support DROP for {{ relation }}.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {%- endif -%}
{% endmacro %}

{% macro confluent__drop_schema(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support DROP for schemas.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
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
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro confluent__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True) -%}
    SELECT
      TABLE_SCHEMA as schema
    FROM
      {{ database }}.`INFORMATION_SCHEMA`.`TABLES`
    WHERE
      TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro confluent__truncate_relation(relation) -%}
  {% set msg %}
    This adapter (confluent) does not support TRUNCATE. Relation: {{ relation }}.
  {% endset %}
  {% do exceptions.raise_compiler_error(msg) %}
{% endmacro %}

{% macro confluent__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if | replace("!=", "<>") }} as should_warn,
      {{ fail_calc }} {{ error_if | replace("!=", "<>") }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}
