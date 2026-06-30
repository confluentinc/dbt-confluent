"""Functional tests for adopting existing Flink resources into dbt (#38).

A model can be pointed at a table and a long-running statement that were
created outside dbt (e.g. by a previous tool or a manual deployment):

  - `alias` maps the model to an existing table name.
  - `statement_name` maps the model to an existing statement name.

Once mapped, dbt manages their lifecycle without recreating them: a healthy
(any non-terminal phase) statement is adopted as-is (skip), and a dead/missing
one is re-submitted under the same adopted name (restart) — the table is never
dropped.

Adoption is purely name-based: the adapter looks resources up by name and does
not track which tool created them. This test plants the INSERT out of band (raw
cursor, not dbt) under the custom name, so what dbt adopts on the next run is
demonstrably a statement it did not create.
"""

import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter._helpers import wait_for_absent, wait_for_running
from tests.functional.adapter.fixtures import ClassScopedCleanup

# Names a previously-deployed pipeline would already be using. They deliberately
# differ from dbt's defaults (model name / `dbt-...-<project>-<model>`) so the
# test proves the alias + statement_name mapping, not the default naming.
ALIAS = "adopted_orders"
STATEMENT_NAME_OVERRIDE = "adopted-orders-insert"

# Deliberately UNBOUNDED (no number-of-rows): the adopted INSERT must stay
# non-terminal so the re-run classifies it as healthy and skips. A bounded
# source would let the INSERT reach COMPLETED (terminal), and dbt would restart
# it instead of adopting it.
MY_STREAMING_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

ADOPTED_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    alias='adopted_orders',
    statement_name='adopted-orders-insert',
    with={'changelog.mode': 'append'},
) }}
select order_id, price from {{ ref('my_streaming_source') }}
"""

MODELS_YML = """
models:
  - name: adopted_streaming_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestAdoptExistingPipeline(ClassScopedCleanup):
    NAME = "adoptpipe"
    TABLES = [ALIAS, "my_streaming_source"]

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # The shared fixture sets +full_refresh: True, which makes every
        # `dbt run` behave like --full-refresh (should_full_refresh() reads the
        # config) — that always recreates and never adopts. Adoption is a
        # no-full-refresh behavior, so disable it here.
        return {
            "name": self.NAME,
            "models": {"+schema": unique_schema, "+full_refresh": False},
            "seeds": {"+schema": unique_schema},
            "tests": {"+schema": unique_schema},
        }

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "adopted_streaming_table.sql": ADOPTED_STREAMING_TABLE,
            "models.yml": MODELS_YML,
        }

    def _insert_name(self, adapter):
        """The statement name the model maps to via `statement_name`."""
        return adapter.get_statement_name(
            model_name="adopted_streaming_table",
            project_name=self.NAME,
            statement_name_override=STATEMENT_NAME_OVERRIDE,
        )

    def _default_insert_name(self, adapter):
        """The name the INSERT would have WITHOUT the statement_name override."""
        return adapter.get_statement_name(
            model_name="adopted_streaming_table",
            project_name=self.NAME,
        )

    def _get_statement(self, adapter, name):
        with adapter.connection_named("inspect"):
            conn = adapter.connections.get_thread_connection()
            return conn.handle.get_statement(name)

    def _statement_absent(self, adapter, name):
        """True if no statement exists under `name`. A pool-scoped 403 is
        treated as absent, mirroring the adapter's own classification."""
        try:
            self._get_statement(adapter, name)
            return False
        except StatementNotFoundError:
            return True
        except OperationalError as e:
            if getattr(e, "http_status_code", None) == 403:
                return True
            raise

    def test_adopts_existing_table_and_statement(self, project, dbt_profile_data):
        adapter = project.adapter
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        insert_name = self._insert_name(adapter)

        # Build the pipeline once to get a table under the ALIAS to insert into,
        # then verify the mapping: the table is reachable under the alias (not
        # the model name) and the INSERT exists under the custom name with NO
        # statement under the default name — so dbt routed by alias +
        # statement_name.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        adopted = relation_from_name(adapter, ALIAS)
        project.run_sql(f"select * from {adopted} limit 1", fetch="one")
        assert self._statement_absent(adapter, self._default_insert_name(adapter)), (
            "INSERT exists under the default name — statement_name mapping was not applied"
        )

        # Replace dbt's INSERT with one created out of band under the SAME custom
        # name, so what dbt adopts next is demonstrably not its own. Wait for the
        # name to free (delete is async) before the raw-cursor re-submit, which
        # has no 409-retry, then wait until it's RUNNING — a stably-healthy
        # state (the adapter adopts any non-terminal phase) so dbt skips it.
        with adapter.connection_named("simulate_external"):
            adapter.delete_statement(insert_name)
        wait_for_absent(adapter, insert_name)
        source = relation_from_name(adapter, "my_streaming_source")
        with adapter.connection_named("plant_external"):
            conn = adapter.connections.get_thread_connection()
            cursor = conn.handle.cursor(mode=ExecutionMode.STREAMING_QUERY)
            cursor.execute(
                f"INSERT INTO {adopted} SELECT order_id, price FROM {source}",
                statement_name=insert_name,
                statement_labels=[label],
            )
        planted_id = wait_for_running(adapter, insert_name).statement_id

        # Adopt the healthy external statement: re-run must skip and leave it
        # untouched (same statement_id), and must not recreate the table.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        adopted_stmt = self._get_statement(adapter, insert_name)
        assert adopted_stmt.statement_id == planted_id, (
            "dbt replaced the externally-created statement instead of adopting it"
        )
        assert not adopted_stmt.phase.is_terminal

        # Adopt + recover a dead statement: with the table intact, deleting the
        # INSERT and re-running must re-submit it under the same adopted name
        # (no --full-refresh), without dropping the table. Wait for the delete
        # to settle so the re-run classifies it as missing (not mid-teardown).
        with adapter.connection_named("kill_external"):
            adapter.delete_statement(insert_name)
        wait_for_absent(adapter, insert_name)
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        restarted = self._get_statement(adapter, insert_name)
        assert restarted.name == insert_name
        assert restarted.statement_id != planted_id
        assert not restarted.phase.is_terminal
