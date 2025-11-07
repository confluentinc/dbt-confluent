{% macro create_schema(relation) -%}
  {{ return(adapter.dispatch('create_schema')(relation)) }}
{%- endmacro %}

{% macro check_schema_exists(relation) -%}
  {{ return(adapter.dispatch('check_schema_exists')(relation)) }}
{%- endmacro %}

{% macro drop_schema(relation) -%}
  {{ return(adapter.dispatch('drop_schema')(relation)) }}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ return(adapter.dispatch('generate_schema_name')(custom_schema_name, node)) }}
{%- endmacro %}

{% macro confluentcloud__generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {% if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{% endmacro %}

{% macro confluentcloud__check_schema_exists(information_schema,schema) -%}
  {{ return(default__check_schema_exists(information_schema, schema)) }}
{% endmacro %}

{% macro confluentcloud__create_schema(relation) -%}
  {%- set schema_name = relation.schema -%}
  
  {%- set error_msg -%}
    Adapter Error: The Kafka cluster (topic/namespace) '{{ schema_name }}' does not exist.
    This adapter (dbt-confluentcloud) is configured to not create clusters automatically.
    
    Please ensure the Kafka cluster '{{ schema_name }}' exists in your Confluent cloud
    or check your model's 'schema:' configuration for typos.
  {%- endset -%}
  
  {{ exceptions.raise_compiler_error(error_msg) }}
{% endmacro %}

{% macro confluentcloud__alter_column_type(relation,column_name,new_column_type) -%}
/*
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
*/
{% endmacro %}

{% macro confluentcloud__drop_relation(relation) -%}
{% endmacro %}

{% macro confluentcloud__drop_schema(relation) -%}
  {%- do log("dbt-confluentcloud: `drop_schema` called for " ~ relation.schema ~ ". This is a no-op.", info=True) -%}
  {{ return(None) }}
{% endmacro %}

{% macro confluentcloud__get_columns_in_relation(relation) -%}
'''Returns a list of Columns in a table.'''
{% endmacro %}

{% macro confluentcloud__list_relations_without_caching(schema_relation) -%}
{% endmacro %}

{% macro confluentcloud__list_schemas(database) -%}
{% endmacro %}

{% macro confluentcloud__rename_relation(from_relation, to_relation) -%}
{% endmacro %}

{% macro confluentcloud__truncate_relation(relation) -%}
{% endmacro %}

{% macro confluentcloud__current_timestamp() -%}
{# docs show not to be implemented currently. #}
{% endmacro %}
