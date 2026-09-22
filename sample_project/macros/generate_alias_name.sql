{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- set raw_user = env_var('BUGBASH_USER', env_var('USER', '')) -%}
    {%- if raw_user == '' -%}
        {{ exceptions.raise_compiler_error("Could not determine a bug bash username: neither BUGBASH_USER nor USER is set. Export one of them, e.g. `export BUGBASH_USER=yourname`.") }}
    {%- endif -%}
    {%- set allowed = "abcdefghijklmnopqrstuvwxyz0123456789" -%}
    {%- set ns = namespace(out="") -%}
    {%- for ch in raw_user | lower -%}
        {%- set ns.out = ns.out ~ (ch if ch in allowed else "_") -%}
    {%- endfor -%}
    {%- set user = ns.out -%}

    {%- if custom_alias_name is none -%}
        {{ user }}_{{ node.name }}
    {%- else -%}
        {{ user }}_{{ custom_alias_name | trim }}
    {%- endif -%}

{%- endmacro %}
