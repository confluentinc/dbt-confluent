-- This macro is customized so we can add a `limit` at call site to call `fetchmany(limit)`
-- rather than `fetchall`, and also to get the `execution_mode` from the config, so that
-- we can instantiate a different cursor in `adapter.execute`.
-- `statement_name` allows materializations to pass a deterministic Flink statement name.
{%- macro statement(
  name=None,
  fetch_result=False,
  auto_begin=True,
  language='sql',
  limit=None,
  execution_mode=None,
  hidden=False,
  statement_name=None
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
      {%- set res, table = adapter.execute(compiled_code, auto_begin=auto_begin, fetch=fetch_result, execution_mode=execution_mode, limit=limit, hidden=hidden, statement_name=statement_name) -%}
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


{#-- Helper macro to derive a deterministic statement name from model context.
     Calls the Python adapter method for sanitization/validation.
     Pass a suffix (e.g. '-ddl') for secondary statements. --#}
{%- macro get_statement_name(suffix='') -%}
  {%- set custom_name = config.get('statement_name', none) -%}
  {{ adapter.get_statement_name(
       model_name=model.name,
       project_name=project_name,
       suffix=suffix,
       statement_name_override=custom_name
  ) | trim }}
{%- endmacro -%}
