import pytest
from confluent_sql.exceptions import OperationalError, StatementNotFoundError
from confluent_sql.execution_mode import ExecutionMode

from dbt.tests.util import relation_from_name, run_dbt, run_dbt_and_capture, write_file
from tests.functional.adapter._helpers import (
    get_result_by_name,
    relation,
    wait_for_absent,
    wait_for_snapshot_rows,
    wait_for_terminal,
)
from tests.functional.adapter.fixtures import ClassScopedCleanup


# The shared ConfluentFixtures sets +full_refresh: True so the inherited dbt
# standard tests recreate on re-run. That makes should_full_refresh() true for
# every run, which forces the 'create' path and bypasses the skip/restart logic.
# Tests that exercise re-run behavior must drop it; this override keeps the
# project name + schema but leaves full_refresh unset.
def _no_full_refresh_config(name, unique_schema):
    return {
        "name": name,
        "models": {"+schema": unique_schema},
        "seeds": {"+schema": unique_schema},
        "tests": {"+schema": unique_schema},
    }


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

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Restart only happens without --full-refresh; drop the shared
        # +full_refresh: True (see _no_full_refresh_config).
        return _no_full_refresh_config(self.NAME, unique_schema)

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
        # and re-submit a new INSERT under the same deterministic name. The
        # restart path logs a distinct line; assert on it so a regression that
        # silently full-refreshes (which would also change statement_id) fails
        # loudly here instead of passing for the wrong reason.
        results, logs = run_dbt_and_capture(["run"])
        assert all(r.status == "success" for r in results)
        assert "Re-submitting (no full refresh required)." in logs, (
            "streaming_table did not take the restart path (it full-refreshed instead)"
        )

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

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Restart only happens without --full-refresh; drop the shared
        # +full_refresh: True (see _no_full_refresh_config).
        return _no_full_refresh_config(self.NAME, unique_schema)

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
        # delete it, and submit a new healthy INSERT under the same name. Assert
        # on the restart log line so a silent full-refresh regression (which
        # would also change statement_id) fails loudly here.
        results, logs = run_dbt_and_capture(["run"])
        assert all(r.status == "success" for r in results)
        assert "Re-submitting (no full refresh required)." in logs, (
            "streaming_table did not take the restart path (it full-refreshed instead)"
        )

        with adapter.connection_named("snapshot_after"):
            conn = adapter.connections.get_thread_connection()
            current = conn.handle.get_statement(insert_name)
        assert current.statement_id != planted_id
        assert not current.phase.is_terminal


# -- The INSERT writes by name (#184) --

# The yml lists (order_id, id_str, tag), and the SELECT returns tag and id_str the other way round.
# Both are STRING, so an INSERT without a column list wrote each into the other's column. Each run
# tags its rows through the `tag` var, so a test can tell its own rows from earlier ones.
BY_NAME_TABLE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, cast('{{ var("tag", "v1") }}' as string) as tag,
    cast(order_id as string) as id_str
from {{ ref('my_streaming_source') }}
"""

BY_NAME_TABLE_IGNORE = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
    on_schema_drift='ignore',
) }}
select order_id, cast('{{ var("tag", "v1") }}' as string) as tag,
    cast(order_id as string) as id_str
from {{ ref('my_streaming_source') }}
"""

# The tag column is the reserved word Order, quoted in the yml, so get_column_names backticks it.
BY_NAME_QUOTED = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'append'},
) }}
select order_id, cast('{{ var("tag", "v1") }}' as string) as `Order`,
    cast(order_id as string) as id_str
from {{ ref('my_streaming_source') }}
"""

# An updating stream: the GROUP BY makes Flink upsert by order_id, the table's primary key. The
# wrapper around the SELECT only projects its columns, so the upsert key must still reach the sink.
# count(*) is BIGINT NOT NULL against the yml's bigint; dbt's contract check ignores nullability.
BY_NAME_UPSERT = """
{{ config(
    materialized='streaming_table',
    with={'changelog.mode': 'upsert'},
) }}
select order_id, cast('{{ var("tag", "v1") }}' as string) as tag,
    cast(order_id as string) as id_str, count(*) as cnt
from {{ ref('my_streaming_source') }}
group by order_id
"""

BY_NAME_MODELS_YML = """
models:
  - name: by_name_table
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
  - name: by_name_table_ignore
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
  - name: by_name_quoted
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: Order
        data_type: string
        quote: true
  - name: by_name_upsert
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
      - name: cnt
        data_type: bigint
"""

# by_name_table's yml reordered to match its SELECT. Its table stays (order_id, id_str, tag).
BY_NAME_MODELS_YML_REORDERED = """
models:
  - name: by_name_table
    columns:
      - name: order_id
        data_type: bigint
      - name: tag
        data_type: string
      - name: id_str
        data_type: string
  - name: by_name_table_ignore
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
  - name: by_name_quoted
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: Order
        data_type: string
        quote: true
  - name: by_name_upsert
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
      - name: cnt
        data_type: bigint
"""

# by_name_table_ignore's yml drops tag. Its SELECT and its table still have it.
BY_NAME_MODELS_YML_TAG_DROPPED = """
models:
  - name: by_name_table
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
  - name: by_name_table_ignore
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
  - name: by_name_quoted
    columns:
      - name: order_id
        data_type: bigint
      - name: id_str
        data_type: string
      - name: Order
        data_type: string
        quote: true
  - name: by_name_upsert
    columns:
      - name: order_id
        data_type: bigint
        constraints:
          - type: not_null
          - type: primary_key
            expression: "not enforced"
      - name: id_str
        data_type: string
      - name: tag
        data_type: string
      - name: cnt
        data_type: bigint
"""


class TestInsertWritesByName(ClassScopedCleanup):
    """streaming_table's INSERT names the yml columns and re-projects the SELECT into them (#184),
    so values land by name on create and on restart, and dbt's contract check guards the restart.

    The tests run in file order and share the class's tables. Each restart test first deletes the
    running INSERTs and waits until they're gone, so its run takes the restart path, and restores
    models.yml in a finally.
    """

    NAME = "byname"
    MODELS = ["by_name_table", "by_name_table_ignore", "by_name_quoted", "by_name_upsert"]
    TABLES = [*MODELS, "my_streaming_source"]

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        # Restart only happens without --full-refresh; drop the shared
        # +full_refresh: True (see _no_full_refresh_config).
        return _no_full_refresh_config(self.NAME, unique_schema)

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_streaming_source.sql": MY_STREAMING_SOURCE,
            "by_name_table.sql": BY_NAME_TABLE,
            "by_name_table_ignore.sql": BY_NAME_TABLE_IGNORE,
            "by_name_quoted.sql": BY_NAME_QUOTED,
            "by_name_upsert.sql": BY_NAME_UPSERT,
            "models.yml": BY_NAME_MODELS_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def first_run(self, project):
        # Build the tables and their INSERTs once; class_clean_up tears down.
        run_dbt(["run"])
        yield

    def _delete_inserts(self, project):
        """Delete each model's INSERT and wait until it's gone, so the next run restarts it. A
        statement still being deleted counts as healthy, and the run would skip instead."""
        adapter = project.adapter
        for model in self.MODELS:
            name = adapter.get_statement_name(model_name=model, project_name=self.NAME)
            with adapter.connection_named("delete_insert"):
                adapter.delete_statement(name)
            wait_for_absent(adapter, name)

    def _assert_rows_by_name(self, project, model, tag, tag_column="tag"):
        """Wait for rows tagged `tag` in either STRING column, then check each landed by name: the
        tag in `tag_column`, and order_id's string in id_str."""
        rows = wait_for_snapshot_rows(
            project,
            f"select order_id, id_str, `{tag_column}` from {relation(project, model)} "
            f"where `{tag_column}` = '{tag}' or id_str = '{tag}'",
            timeout=60,
        )
        assert len(rows) > 0, f"no rows tagged {tag!r} landed in {model}"
        for order_id, id_str, row_tag in rows:
            assert (id_str, row_tag) == (str(int(order_id)), tag), (
                f"{model} was written by position, not by name: "
                f"order_id={order_id}, id_str={id_str!r}, {tag_column}={row_tag!r}"
            )

    def test_create_writes_by_name(self, project):
        """The create-path bug (probe 71ca3b81): each table is built in yml order, and the SELECT's
        swapped columns still land by name, including the quoted `Order` (probe d2392b5a) and the
        upsert stream through the wrapper."""
        self._assert_rows_by_name(project, "by_name_table", "v1")
        self._assert_rows_by_name(project, "by_name_table_ignore", "v1")
        self._assert_rows_by_name(project, "by_name_quoted", "v1", tag_column="Order")
        self._assert_rows_by_name(project, "by_name_upsert", "v1")

    def test_restart_writes_by_name(self, project):
        """The restart bug (probe dfee9a07): each re-submitted INSERT goes into the existing table,
        whose order (the yml's) differs from the SELECT's, under 'fail' and 'ignore'."""
        self._delete_inserts(project)
        results, logs = run_dbt_and_capture(["run", "--vars", "{tag: v2}"])
        assert all(r.status == "success" for r in results)
        assert "Re-submitting (no full refresh required)." in logs, (
            "streaming_table did not take the restart path"
        )
        self._assert_rows_by_name(project, "by_name_table", "v2")
        self._assert_rows_by_name(project, "by_name_table_ignore", "v2")
        self._assert_rows_by_name(project, "by_name_quoted", "v2", tag_column="Order")
        self._assert_rows_by_name(project, "by_name_upsert", "v2")

    def test_restart_after_yml_reorder_writes_by_name(self, project):
        """A yml-only reorder: the yml now lists (order_id, tag, id_str), the SELECT's order, but
        the table is still (order_id, id_str, tag). The re-projection alone would write in yml
        order by position; the column list maps each value by name (probe 0a2858ad, R2 and R3)."""
        yml_path = (project.project_root, "models", "models.yml")
        self._delete_inserts(project)
        write_file(BY_NAME_MODELS_YML_REORDERED, *yml_path)
        try:
            results = run_dbt(["run", "--select", "by_name_table", "--vars", "{tag: v3}"])
            assert all(r.status == "success" for r in results)
            self._assert_rows_by_name(project, "by_name_table", "v3")
        finally:
            write_file(BY_NAME_MODELS_YML, *yml_path)

    def test_restart_fails_when_yml_drops_a_selected_column(self, project):
        """The yml drops tag, but the SELECT and the table still have it, so the drift check
        (SELECT vs table) passes, even its columns-only form under 'ignore'. An INSERT naming only
        the yml columns would fill tag with NULL, so dbt's contract check, which now runs on
        restart too, fails the model instead."""
        yml_path = (project.project_root, "models", "models.yml")
        self._delete_inserts(project)
        write_file(BY_NAME_MODELS_YML_TAG_DROPPED, *yml_path)
        try:
            results = run_dbt(["run", "--select", "by_name_table_ignore"], expect_pass=False)
            result = get_result_by_name(results, "by_name_table_ignore")
            assert result is not None, "by_name_table_ignore not found in results"
            assert result.status.name == "Error", result.message
            assert "enforced contract that failed" in result.message, result.message
        finally:
            write_file(BY_NAME_MODELS_YML, *yml_path)
