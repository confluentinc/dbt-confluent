{#- `dbt run-operation sweep_function_artifacts`

   Lists this project's artifacts (the ones dbt uploaded for its functions) that no function uses.
   By default it only lists them, and prints the command that deletes them.

   With `list_file` (written by `list_function_artifacts`), only the artifacts in it are
   considered, and the outcome is reported for each of them. -#}
{%- macro sweep_function_artifacts(delete=false, list_file=none) -%}
  {%- do adapter.sweep_function_artifacts(delete, list_file) -%}
{%- endmacro -%}
