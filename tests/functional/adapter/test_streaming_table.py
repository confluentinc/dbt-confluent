import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter._helpers import wait_for_absent, wait_for_terminal
from tests.functional.adapter.fixtures import ClassScopedCleanup

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


class TestStreamingTable(ClassScopedCleanup):
    NAME = "streaming"
    TABLES = ["my_custom_named_table", "my_streaming_table", "my_streaming_source"]

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


class TestFullRefreshRecreatesStatement(ClassScopedCleanup):
    """A second `dbt run --full-refresh` must succeed against an existing
    deterministic statement: the old statement is deleted and a new one is
    submitted under the same name. Without delete-on-full-refresh the
    second run would fail with a name conflict."""

    NAME = "frrecreate"
    TABLES = ["my_streaming_table", "my_streaming_source"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

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


class TestOrphanStatementCleanup(ClassScopedCleanup):
    """If a statement already exists under the deterministic name but the
    backing table doesn't (e.g. table dropped externally), `dbt run` must
    delete the orphaned statement before submitting its own. Without orphan
    cleanup the run would fail with a name conflict."""

    NAME = "orphancl"
    TABLES = ["my_streaming_table", "my_streaming_source"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

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


class TestCrashRecoveryRestart(ClassScopedCleanup):
    """Crash recovery (#33): if the table exists but the long-running
    INSERT statement is gone (e.g. dbt was killed between DDL and DML, or
    the statement was deleted externally), `dbt run` without --full-refresh
    must re-submit the INSERT under the same deterministic name. The table
    is preserved (no topic state lost)."""

    NAME = "crashrec"
    TABLES = ["my_streaming_table", "my_streaming_source"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

    def _statement_id_or_none(self, adapter, name):
        """Return the statement_id for `name`, or None if it doesn't exist.

        The DDL statement completes immediately and may be auto-cleaned by
        Flink, so absence is a legitimate state — we treat it the same way the
        adapter does (404, or a pool-scoped 403, means "not there")."""
        with adapter.connection_named("ddl_snapshot"):
            conn = adapter.connections.get_thread_connection()
            try:
                return conn.handle.get_statement(name).statement_id
            except StatementNotFoundError:
                return None
            except OperationalError as e:
                if getattr(e, "http_status_code", None) == 403:
                    return None
                raise

    def test_missing_insert_statement_is_resubmitted(self, project):
        adapter = project.adapter
        insert_name = adapter.get_statement_name(
            model_name="my_streaming_table", project_name=self.NAME
        )
        ddl_name = adapter.get_statement_name(
            model_name="my_streaming_table", project_name=self.NAME, suffix="-ddl"
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

        # Snapshot the DDL statement just before the restart. The restart path
        # must NOT re-run the DDL (that would recreate the table and lose topic
        # state), so this id must be unchanged afterwards.
        ddl_before = self._statement_id_or_none(adapter, ddl_name)

        # `dbt run` without --full-refresh must detect the missing statement
        # and re-submit a new INSERT under the same deterministic name.
        results = run_dbt(["run"])
        assert all(r.status == "success" for r in results)

        with adapter.connection_named("snapshot_second"):
            conn = adapter.connections.get_thread_connection()
            second = conn.handle.get_statement(insert_name)

        assert second.name == first.name
        assert second.statement_id != first.statement_id

        # Table preserved: the DDL was not re-submitted. A regression that
        # recreated the table would submit a fresh DDL statement under the
        # same name (new statement_id); a skipped DDL leaves the id unchanged,
        # or absent if Flink auto-cleaned the completed statement.
        ddl_after = self._statement_id_or_none(adapter, ddl_name)
        assert ddl_after is None or ddl_after == ddl_before


TERMINAL_PLANT_SQL = "SHOW TABLES"


class TestDeadStatementRestart(ClassScopedCleanup):
    """Dead-statement recovery (half of #32): if the long-running INSERT
    statement is in a terminal phase (FAILED, STOPPED, COMPLETED, DELETED),
    `dbt run` without --full-refresh must replace it. The classifier doesn't
    distinguish terminal phases, so we plant a `SHOW TABLES` statement under
    the deterministic INSERT name — it reaches COMPLETED (terminal) within
    seconds, no external stop API or invalid-SQL trick needed (invalid SQL
    is rejected at validation time, before it can be persisted as FAILED)."""

    NAME = "deadstmt"
    TABLES = ["my_streaming_table", "my_streaming_source"]

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "my_streaming_table.sql": SIMPLE_STREAMING_TABLE,
            "models.yml": SIMPLE_MODELS_YML,
        }

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
        wait_for_absent(adapter, insert_name)
        with adapter.connection_named("plant_terminal"):
            conn = adapter.connections.get_thread_connection()
            cursor = conn.handle.cursor(mode=ExecutionMode.STREAMING_QUERY)
            cursor.execute(
                TERMINAL_PLANT_SQL,
                statement_name=insert_name,
                statement_labels=[label],
            )
        terminal = wait_for_terminal(adapter, insert_name)
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
