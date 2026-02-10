{%- macro statement(name=None, fetch_result=False, auto_begin=True, language='sql') -%}
  {%- if execute: -%}
    {%- set compiled_code = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime {} for node "{}"'.format(language, model['unique_id'])) }}
      {{ write(compiled_code) }}
    {%- endif -%}
    {%- if language == 'sql'-%}
      {%- set res, table = adapter.execute(compiled_code, auto_begin=auto_begin, fetch=fetch_result) -%}
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
