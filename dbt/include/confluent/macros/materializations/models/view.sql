{%- materialization view, adapter='confluent' -%}
  {# Pure config validation first, before load_cached_relation (which can be a
     real INFORMATION_SCHEMA round-trip if this schema's relation cache isn't
     already warm) -- it doesn't depend on the relation, so a config mistake
     should fail before any warehouse I/O, not after. #}
  {% do validate_materialization_config() %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {# Flink has no CREATE OR REPLACE VIEW. An existing view is updated in place
     with ALTER VIEW ... AS, which the server rejects without touching the view
     when the new SQL is invalid; dropping it first would leave no view at all.
     --full-refresh drops and recreates it instead, as every materialization's
     full refresh does. Anything else under this name (a table, when a model
     switches to `view`) is dropped and the view created. #}
  {%- set alter_existing_view = (existing_relation is not none and existing_relation.is_view
                                 and not should_full_refresh()) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# dbt's contract check, before any DDL when something already exists.
     CREATE VIEW runs it itself, but only after the drop below, and ALTER VIEW
     doesn't run it at all. Gated on `enforced`: unlike a `streaming_table`, a
     view needn't declare yml columns, and the check fails without them. The
     check runs the model's SQL, so it follows the pre-hooks, which may create
     something that SQL reads.
     On the drop-and-create paths (--full-refresh, or a non-view under this
     name) CREATE VIEW then runs the check a second time, a redundant
     schema-probe statement. It's deliberate: this early run is what fails the
     model before the drop, so the existing relation survives a contract
     mismatch. #}
  {% if existing_relation is not none and config.get('contract').enforced %}
    {% do get_assert_columns_equivalent(sql) %}
  {% endif %}

  -- delete the existing statement: the new one reuses its deterministic name,
  -- so it has to be freed first. This happens before the ALTER, so if the ALTER
  -- is rejected the old CREATE VIEW statement record is gone but the view itself
  -- is untouched and keeps serving. Only the record is lost, which is harmless.
  -- If there's no existing view (nothing there, or a table that a `view` model is
  -- replacing), this model's statement probably doesn't exist either, so suppress
  -- the loud warning the adapter emits for a pool-scoped 403.
  {{ delete_statement_if_exists(get_statement_name(),
                                expect_exists=(existing_relation is not none and existing_relation.is_view)) }}
  {% if not alter_existing_view %}
    {{ drop_relation_if_exists(existing_relation) }}
  {% endif %}

  -- build model
  {% call statement('main', statement_name=get_statement_name()) -%}
    {% if alter_existing_view %}
      {{ get_replace_view_sql(target_relation, sql) }}
    {% else %}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {% endif %}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
