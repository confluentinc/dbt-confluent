{#- `dbt run-operation cleanup_function_artifacts`

   Lists this project's artifacts (the ones dbt uploaded for its functions) that no function uses.
   By default it only lists them, and prints the command that deletes them.

   Replacing a function never deletes its old artifact, because statements started before the
   change stay bound to it and can't restart once it is gone. Delete only once those statements
   have been recreated.

   *Unsettled* artifacts still carry the journal of a replacement that never finished (or that is
   running right now). They are listed apart, and `delete: true` leaves them alone; `delete_unsettled: true`
   deletes them. -#}
{%- macro cleanup_function_artifacts(delete=false, delete_unsettled=false) -%}
  {%- set found = adapter.cleanup_function_artifacts(delete, delete_unsettled) -%}
  {%- set failed = [] -%}
  {% for group, label, deleted in [(found.orphans, 'Unused', delete), (found.unsettled, 'Unsettled', delete_unsettled)] %}
    {% for result in group %}
      {% if result.error %}
        {% do failed.append(result.artifact ~ ': ' ~ result.error) %}
      {% else %}
        {{ log(('Deleted ' if deleted else label ~ ': ') ~ result.artifact, info=true) }}
      {% endif %}
    {% endfor %}
  {% endfor %}
  {% if not found.orphans and not found.unsettled %}
    {{ log('No unused artifacts of this project.', info=true) }}
  {% endif %}
  {% if found.orphans and not delete %}
    {{ log("To delete the unused artifacts, once no statement depends on them: dbt run-operation cleanup_function_artifacts --args '{delete: true}'", info=true) }}
  {% endif %}
  {% if found.unsettled and not delete_unsettled %}
    {{ log("Unsettled artifacts have an unfinished replacement's journal (or one is running now). To delete them: dbt run-operation cleanup_function_artifacts --args '{delete_unsettled: true}'", info=true) }}
  {% endif %}
  {% if failed %}
    {% do exceptions.raise_compiler_error('Could not delete: ' ~ failed | join('; ')) %}
  {% endif %}
{%- endmacro -%}
