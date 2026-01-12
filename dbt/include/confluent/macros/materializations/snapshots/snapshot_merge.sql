{#
    Confluent Flink SQL doesn't support MERGE, UPDATE with complex queries, or RENAME.
    Instead, we rebuild the entire snapshot table using DROP + CREATE.

    This macro builds the complete snapshot dataset combining:
    - Unchanged records from the existing table
    - Updated records with new dbt_valid_to timestamps
    - New snapshot records
#}
{% macro confluent__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    {#
        Build complete snapshot data:
        1. Existing records that haven't changed
        2. Existing records that changed (with updated dbt_valid_to)
        3. New snapshot records from staging
    #}
    with staging_data as (
        select * from {{ source }}
    ),

    -- Records that need their dbt_valid_to updated (old snapshot records being closed)
    updated_records as (
        select 
            {% for col in insert_cols %}
                {%- if col == columns.dbt_valid_to -%}
                    staging.{{ col }}
                {%- else -%}
                    existing.{{ col }}
                {%- endif -%}
                {%- if not loop.last %}, {% endif -%}
            {% endfor %}
        from {{ target.render() }} as existing
        inner join staging_data as staging
            on existing.{{ columns.dbt_scd_id }} = staging.{{ columns.dbt_scd_id }}
        where staging.dbt_change_type in ('update', 'delete')
        {% if config.get("dbt_valid_to_current") %}
            {%- set target_unique_key = config.get('dbt_valid_to_current') | trim -%}
            and (existing.{{ columns.dbt_valid_to }} = {{ target_unique_key }} or existing.{{ columns.dbt_valid_to }} is null)
        {% else %}
            and existing.{{ columns.dbt_valid_to }} is null
        {% endif %}
    ),

    -- Records that remain unchanged
    unchanged_records as (
        select existing.*
        from {{ target.render() }} as existing
        where existing.{{ columns.dbt_scd_id }} not in (
            select {{ columns.dbt_scd_id }}
            from staging_data
            where dbt_change_type in ('update', 'delete')
        )
    ),

    -- New snapshot records from staging
    new_records as (
        select {{ insert_cols_csv }}
        from staging_data
        where dbt_change_type = 'insert'
    )

    select * from unchanged_records
    union all
    select * from updated_records
    union all
    select * from new_records
{%- endmacro %}
