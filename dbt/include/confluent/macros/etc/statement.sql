-- This macro is customized so we can add a `limit` at call site to call `fetchmany(limit)`
-- rather than `fetchall`, and also to get the `execution_mode` from the config, so that
-- we can instantiate a different cursor in `adapter.execute`
{%- macro statement(
  name=None,
  fetch_result=False,
  auto_begin=True,
  language='sql',
  limit=None,
  execution_mode=None,
  hidden=False
) -%}
  {%- if execute: -%}
    {%- set compiled_code = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime {} for node "{}"'.format(language, model['unique_id'])) }}
      {{ write(compiled_code) }}
    {%- endif -%}
    {%- if language == 'sql'-%}
      {% if not execution_mode %}
        {% set execution_mode = config.get("execution_mode", None) %}
      {% endif %}
      {%- set res, table = adapter.execute(compiled_code, auto_begin=auto_begin, fetch=fetch_result, execution_mode=execution_mode, limit=limit, hidden=hidden) -%}
    {%- elif language == 'python' -%}
      {%- set res = submit_python_job(model, compiled_code) -%}
      {#-- TODO: What should table be for python models? --#}
      {%- set table = None -%}
    {%- else -%}
      {% do exceptions.raise_compiler_error("statement macro didn't get supported language") %}
    {%- endif -%}

    {%- if name is not none -%}
      {{ store_result(name, response=res, agate_table=table) }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}
