{% macro confluent__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if | replace("!=", "<>") }} as should_warn,
      {{ fail_calc }} {{ error_if | replace("!=", "<>") }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}

