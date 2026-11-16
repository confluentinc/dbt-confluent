{#- `dbt run-operation list_function_artifacts`

   Writes this project's artifacts (the ones dbt uploaded for its functions), used or not, and the
   functions using each, to a JSON file: `list_file`, by default `confluent_function_artifacts.json` in
   the target directory. Run it before a `dbt run`, then pass the file to `sweep_function_artifacts`
   as `list_file` afterwards, so that only artifacts that existed before the run are
   considered for deletion. -#}
{%- macro list_function_artifacts(list_file=none) -%}
  {%- do adapter.list_function_artifacts(list_file) -%}
{%- endmacro -%}
