{% materialization incremental, adapter='confluent' -%}
    {% set msg %}
      This adapter (confluent) does not support incremental materialization.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
{%- endmaterialization %}
