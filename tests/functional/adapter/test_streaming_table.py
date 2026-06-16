import time

import pytest
from confluent_sql.exceptions import StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter.fixtures import ConfluentFixtures

MY_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, price, order_time from {{ ref('my_streaming_source') }}
"""

MY_STREAMING_SOURCE = """
{{ config(
    materialized='streaming_source',
    connector='faker',
    with={
        'rows-per-second': '1',
        'number-of-rows': '100',
        'changelog.mode': 'append',
    }
) }}
order_id BIGINT,
price DECIMAL(10, 2),
order_time TIMESTAMP(3),
WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND,
PRIMARY KEY(`order_id`) NOT ENFORCED
"""

MY_CUSTOM_NAMED_TABLE = """
{{ config(
    materialized='streaming_table',
    statement_name='my-custom-insert',
    with={'changelog.mode': 'append'},
) }}
select order_id, price from {{ ref('my_streaming_source') }}
"""

MODELS_YML = """
models:
  - name: my_streaming_table
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: price
        data_type: decimal(10,2)
      - name: order_time
        data_type: timestamp(3)
  - name: my_custom_named_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestStreamingTable(ConfluentFixtures):
    NAME = "streaming"

    @pytest.fixture(scope="class")
    def run_dbt_results(self, project):
        return run_dbt(["run"])

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": MY_STREAMING_TABLE,
            "my_custom_named_table.sql": MY_CUSTOM_NAMED_TABLE,
            "models.yml": MODELS_YML,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """Override base clean_up so statements survive across tests in this class."""
        yield

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        """Delete statements and tables once after all tests in the class."""
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)
        project.run_sql("drop table if exists my_custom_named_table")
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_streaming_source")

    def test_materialized_source(self, project, run_dbt_results):
        result_names = {r.node.name for r in run_dbt_results}
        assert {
            "my_streaming_source",
            "my_streaming_table",
            "my_custom_named_table",
        } == result_names
        relation = relation_from_name(project.adapter, "my_streaming_table")
        result = project.run_sql(f"select * from {relation}", fetch="one")
        assert len(result[0]) == 3

        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 0

    def test_deterministic_statement_names(self, project, run_dbt_results):
        """After dbt run, the long-running streaming INSERT statement should
        exist with a deterministic name derived from project and model names.

        Only the streaming_table INSERT is guaranteed to still be RUNNING;
        the DDL statement and the streaming_source statement complete
        immediately and may be auto-cleaned by Flink before we check."""
        adapter = project.adapter
        name = adapter.get_statement_name(
            model_name="my_streaming_table",
            project_name=self.NAME,
        )

        with adapter.connection_named("check_statements"):
            conn = adapter.connections.get_thread_connection()
            try:
                stmt = conn.handle.get_statement(name)
            except StatementNotFoundError:
                pytest.fail(f"Expected Flink statement '{name}' to exist but it was not found")
            assert stmt is not None, f"Statement '{name}' should exist"

    def test_custom_statement_name(self, project, run_dbt_results):
        """Verify that config(statement_name='...') overrides the default naming.

        my_custom_named_table uses statement_name='my-custom-insert', so
        its long-running INSERT statement should exist under that name."""
        adapter = project.adapter
        expected_name = adapter.get_statement_name(
            model_name="my_custom_named_table",
            project_name=self.NAME,
            statement_name_override="my-custom-insert",
        )

        with adapter.connection_named("check_custom"):
            conn = adapter.connections.get_thread_connection()
            try:
                stmt = conn.handle.get_statement(expected_name)
            except StatementNotFoundError:
                pytest.fail(
                    f"Expected statement '{expected_name}' from config override, but not found"
                )
            assert stmt is not None


SIMPLE_STREAMING_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, price from {{ ref('my_streaming_source') }}
"""

SIMPLE_MODELS_YML = """
models:
  - name: my_streaming_table
    columns:
      - name: order_id
        data_type: bigint
      - name: price
        data_type: decimal(10,2)
"""


class TestFullRefreshRecreatesStatement(ConfluentFixtures):
    """A second `dbt run --full-refresh` must succeed against an existing
    deterministic statement: the old statement is deleted and a new one is
    submitted under the same name. Without delete-on-full-refresh the
    second run would fail with a name conflict."""

    NAME = "frrecreate"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_streaming_source")

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """Override per-test cleanup; class_clean_up handles teardown."""
        yield

    def _get_statement(self, project, model_name):
        name = project.adapter.get_statement_name(model_name=model_name, project_name=self.NAME)
        with project.adapter.connection_named(f"check_{model_name}"):
            conn = project.adapter.connections.get_thread_connection()
            return conn.handle.get_statement(name)

    def test_full_refresh_recreates_statement(self, project):
        # First run creates the table + INSERT statement.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        first = self._get_statement(project, "my_streaming_table")

        # --full-refresh must drop the table, delete the existing statement,
        # and submit a new one under the same deterministic name.
        results = run_dbt(["run", "--full-refresh"])
        assert all(r.status == "success" for r in results)
        second = self._get_statement(project, "my_streaming_table")

        assert second.name == first.name
        assert second.statement_id != first.statement_id


class TestOrphanStatementCleanup(ConfluentFixtures):
    """If a statement already exists under the deterministic name but the
    backing table doesn't (e.g. table dropped externally), `dbt run` must
    delete the orphaned statement before submitting its own. Without orphan
    cleanup the run would fail with a name conflict."""

    NAME = "orphancl"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_streaming_source")

    def test_orphan_statement_is_cleaned_up(self, project, dbt_profile_data):
        adapter = project.adapter
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        orphan_name = adapter.get_statement_name(
            model_name="my_streaming_table", project_name=self.NAME
        )

        # Plant an orphan statement under the deterministic name. Use
        # SHOW TABLES — it completes quickly and doesn't depend on
        # anything we'd later create.
        with adapter.connection_named("plant_orphan"):
            conn = adapter.connections.get_thread_connection()
            cursor = conn.handle.cursor(mode=ExecutionMode.STREAMING_QUERY)
            cursor.execute(
                "SHOW TABLES",
                statement_name=orphan_name,
                statement_labels=[label],
            )
            planted_id = conn.handle.get_statement(orphan_name).statement_id

        # No table exists, but the statement does. dbt run must clean up
        # the orphan and create its own statement under the same name.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)

        with adapter.connection_named("check_after"):
            conn = adapter.connections.get_thread_connection()
            current = conn.handle.get_statement(orphan_name)
        assert current.statement_id != planted_id


class TestCrashRecoveryRestart(ConfluentFixtures):
    """Crash recovery (#33): if the table exists but the long-running
    INSERT statement is gone (e.g. dbt was killed between DDL and DML, or
    the statement was deleted externally), `dbt run` without --full-refresh
    must re-submit the INSERT under the same deterministic name. The table
    is preserved (no topic state lost)."""

    NAME = "crashrec"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_streaming_source")

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """Override per-test cleanup; class_clean_up handles teardown."""
        yield

    def test_missing_insert_statement_is_resubmitted(self, project):
        adapter = project.adapter
        insert_name = adapter.get_statement_name(
            model_name="my_streaming_table", project_name=self.NAME
        )

        # First run creates the source, the table, and the long-running INSERT.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)
        with adapter.connection_named("snapshot_first"):
            conn = adapter.connections.get_thread_connection()
            first = conn.handle.get_statement(insert_name)

        # Simulate a crash between DDL and DML: delete only the INSERT
        # statement, leave the table in place. The streaming_source
        # statement is left alone so its source data continues to flow.
        with adapter.connection_named("simulate_crash"):
            adapter.delete_statement(insert_name)

        # `dbt run` without --full-refresh must detect the missing statement
        # and re-submit a new INSERT under the same deterministic name.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)

        with adapter.connection_named("snapshot_second"):
            conn = adapter.connections.get_thread_connection()
            second = conn.handle.get_statement(insert_name)

        assert second.name == first.name
        assert second.statement_id != first.statement_id


TERMINAL_PLANT_SQL = "SHOW TABLES"


class TestDeadStatementRestart(ConfluentFixtures):
    """Dead-statement recovery (half of #32): if the long-running INSERT
    statement is in a terminal phase (FAILED, STOPPED, COMPLETED, DELETED),
    `dbt run` without --full-refresh must replace it. The classifier doesn't
    distinguish terminal phases, so we plant a `SHOW TABLES` statement under
    the deterministic INSERT name — it reaches COMPLETED (terminal) within
    seconds, no external stop API or invalid-SQL trick needed (invalid SQL
    is rejected at validation time, before it can be persisted as FAILED)."""

    NAME = "deadstmt"

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

    @pytest.fixture(autouse=True, scope="class")
    def class_clean_up(self, project, dbt_profile_data):
        yield
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        with project.adapter.connection_named("cleanup"):
            conn = project.adapter.connections.get_thread_connection()
            for stmt in conn.handle.list_statements(label=label):
                project.adapter.delete_statement(stmt.name)
        project.run_sql("drop table if exists my_streaming_table")
        project.run_sql("drop table if exists my_streaming_source")

    @pytest.fixture(autouse=True)
    def clean_up(self, project, dbt_profile_data):
        """Override per-test cleanup; class_clean_up handles teardown."""
        yield

    def _wait_for_terminal(self, adapter, name, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with adapter.connection_named("phase_poll"):
                conn = adapter.connections.get_thread_connection()
                stmt = conn.handle.get_statement(name)
                if stmt.phase.is_terminal:
                    return stmt
            time.sleep(2)
        raise AssertionError(f"Statement {name} did not reach terminal state in {timeout}s")

    def _wait_for_absent(self, adapter, name, timeout=60):
        """Block until `name` is fully gone (get_statement 404s).

        adapter.delete_statement() does not await async teardown — the
        production restart path tolerates the lingering name via add_query's
        409-retry on CREATE. This test re-submits the same name through the raw
        cursor (no such retry), so it must wait for the name to actually free
        before planting, or it races the teardown and hits a 409 Conflict.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with adapter.connection_named("absence_poll"):
                conn = adapter.connections.get_thread_connection()
                try:
                    conn.handle.get_statement(name)
                except StatementNotFoundError:
                    return
            time.sleep(2)
        raise AssertionError(f"Statement {name} was not freed within {timeout}s of deletion")

    def test_terminal_insert_statement_is_resubmitted(self, project, dbt_profile_data):
        adapter = project.adapter
        label = dbt_profile_data["test"]["outputs"]["default"]["statement_label"]
        insert_name = adapter.get_statement_name(
            model_name="my_streaming_table", project_name=self.NAME
        )

        # First run creates everything cleanly.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)

        # Replace the live INSERT with a planted statement that will
        # finish on its own (COMPLETED is a terminal phase too). Delete the
        # live one first, then wait for the name to actually free — the raw
        # cursor re-submit below has no 409-retry, so it must not race the
        # async teardown.
        with adapter.connection_named("plant_terminal"):
            adapter.delete_statement(insert_name)
        self._wait_for_absent(adapter, insert_name)
        with adapter.connection_named("plant_terminal"):
            conn = adapter.connections.get_thread_connection()
            cursor = conn.handle.cursor(mode=ExecutionMode.STREAMING_QUERY)
            cursor.execute(
                TERMINAL_PLANT_SQL,
                statement_name=insert_name,
                statement_labels=[label],
            )
        terminal = self._wait_for_terminal(adapter, insert_name)
        planted_id = terminal.statement_id

        # `dbt run` without --full-refresh must detect the terminal statement,
        # delete it, and submit a new healthy INSERT under the same name.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)

        with adapter.connection_named("snapshot_after"):
            conn = adapter.connections.get_thread_connection()
            current = conn.handle.get_statement(insert_name)
        assert current.statement_id != planted_id
        assert not current.phase.is_terminal
