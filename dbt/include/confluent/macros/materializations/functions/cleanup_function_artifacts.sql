{#- `dbt run-operation cleanup_function_artifacts`

   Lists this project's artifacts (the ones dbt uploaded for its functions) that no function uses.
   By default it only lists them, and prints the command that deletes them.

   Replacing a function never deletes its old artifact, because statements started before the
   change stay bound to it and can't restart once it is gone. Delete only once those statements
   have been recreated, and not while a `dbt run` is in progress: an artifact a run has uploaded
   but not yet created its function from looks unused. -#}
{%- macro cleanup_function_artifacts(delete=false) -%}
  {%- do adapter.cleanup_function_artifacts(delete) -%}
{%- endmacro -%}
