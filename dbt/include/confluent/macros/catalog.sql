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
