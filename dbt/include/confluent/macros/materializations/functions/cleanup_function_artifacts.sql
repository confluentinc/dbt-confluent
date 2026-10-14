{#- `dbt run-operation cleanup_function_artifacts`

   Lists this project's artifacts (the ones dbt uploaded for its functions) that no function uses.
   By default it only lists them, and prints the command that deletes them.

   Replacing a function never deletes its old artifact, because statements started before the
   change stay bound to it and can't restart once it is gone. Delete only once those statements
   have been recreated, and not while a `dbt run` is in progress: an artifact a run has uploaded
   but not yet created its function from looks unused. -#}
{%- macro cleanup_function_artifacts(delete=false) -%}
  {%- set unused = adapter.cleanup_function_artifacts(delete) -%}
  {%- set failed = [] -%}
  {% for result in unused %}
    {% if result.error %}
      {% do failed.append(result.artifact ~ ': ' ~ result.error) %}
    {% else %}
      {{ log(('Deleted ' if delete else 'Unused: ') ~ result.artifact, info=true) }}
    {% endif %}
  {% endfor %}
  {% if not unused %}
    {{ log('No unused artifacts of this project.', info=true) }}
  {% endif %}
  {% if unused and not delete %}
    {{ log("To delete the unused artifacts, once no statement depends on them: dbt run-operation cleanup_function_artifacts --args '{delete: true}'", info=true) }}
  {% endif %}
  {% if failed %}
    {% do exceptions.raise_compiler_error('Could not delete: ' ~ failed | join('; ')) %}
  {% endif %}
{%- endmacro -%}
