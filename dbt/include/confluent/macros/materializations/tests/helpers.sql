{# In Flink streaming mode, count(*) over an empty result set returns 0 rows
   instead of 1 row with value 0 (unlike batch SQL). dbt-core expects exactly
   1 row with 3 columns. We work around this by wrapping the aggregation in a
   UNION ALL with a zero-failures fallback row, then re-aggregating with SUM
   so exactly 1 row is always produced. #}
{% macro confluent__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      coalesce(sum(failures), 0) as failures,
      coalesce(sum(failures), 0) {{ warn_if | replace("!=", "<>") }} as should_warn,
      coalesce(sum(failures), 0) {{ error_if | replace("!=", "<>") }} as should_error
    from (
      select
        {{ fail_calc }} as failures
      from (
        {{ main_sql }}
        {{ "limit " ~ limit if limit != none }}
      ) dbt_internal_test
      union all
      select cast(0 as bigint) as failures
    ) dbt_internal_test_with_fallback
{%- endmacro %}

