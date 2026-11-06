{#- `dbt run-operation sweep_function_artifacts`

   Lists this project's artifacts (the ones dbt uploaded for its functions) that no function uses.
   By default it only lists them, and prints the command that deletes them. -#}
{%- macro sweep_function_artifacts(delete=false) -%}
  {%- do adapter.sweep_function_artifacts(delete) -%}
{%- endmacro -%}
