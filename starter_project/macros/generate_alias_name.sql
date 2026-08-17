{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- set raw_user = env_var('BUGBASH_USER', env_var('USER', '')) -%}
    {%- if raw_user == '' -%}
        {{ exceptions.raise_compiler_error("Could not determine a bug bash username: neither BUGBASH_USER nor USER is set. Export one of them, e.g. `export BUGBASH_USER=yourname`.") }}
    {%- endif -%}
    {%- set user = raw_user | lower | replace('.', '_') | replace(' ', '_') -%}

    {%- if custom_alias_name is none -%}
        {{ user }}_{{ node.name }}
    {%- else -%}
        {{ user }}_{{ custom_alias_name | trim }}
    {%- endif -%}

{%- endmacro %}
