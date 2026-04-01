{% materialization materialized_view, adapter='confluent' -%}
    {% set msg %}
      This adapter (confluent) does not support materialized_view materialization.
      Use 'table' instead — in Confluent Flink, materialized views are implemented as CTAS tables.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
{%- endmaterialization %}
