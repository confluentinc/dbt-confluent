"""Unit tests for the `check_for_schema_drift` Jinja wiring.

Renders the real helpers.sql with plain jinja2 and stand-ins for the dbt context the macro
touches, then checks which statements it would submit and what it hands the adapter. Driven
through `check_for_schema_drift`, since `make_module` doesn't export the underscore macros.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import jinja2
import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter, ConfluentRelation, DryRunColumns
from tests.unit._helpers import relation

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)
MODEL_SQL = "select id from src"
# Opaque stand-ins: the macro hands the resolver's result on as is.
DRY_RUN = DryRunColumns(existing={"id": "existing type"}, expected={"id": "expected type"})
TEMP_NAME = "__dbt_tmp_schema_check_my_model"
COLUMNS_FROM = "FROM INFORMATION_SCHEMA.`COLUMNS`"
DISTRIBUTION_FILTER = "DISTRIBUTION_ORDINAL_POSITION IS NOT NULL"


class _Config:
    """Stand-in for dbt's `config`, backed by a dict of model config."""

    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _Harness:
    """Renders check_for_schema_drift and records what it did."""

    def __init__(self, config, dry_run_result=None, leaked=None):
        self.statements: list[tuple[str, str]] = []
        # One (statement name, stand-in table) per load_result call, in order.
        self.loaded = []
        self.adapter = MagicMock()
        self.adapter.Relation = ConfluentRelation
        self.adapter.generate_schema_check_temp_name.side_effect = lambda identifier: (
            ConfluentAdapter.generate_schema_check_temp_name(
                ConfluentAdapter.__new__(ConfluentAdapter), identifier
            )
        )
        self.adapter.get_columns_from_dry_run.return_value = dry_run_result
        self.adapter.get_relation.return_value = leaked
        self.existing = relation("my_model")

        env = jinja2.Environment(extensions=["jinja2.ext.do"])
        env.globals.update(
            adapter=self.adapter,
            config=_Config(config),
            this=self.existing,
            sql=MODEL_SQL,
            statement=self._statement,
            load_result=self._load_result,
        )
        self.module = env.from_string(MACRO_FILE.read_text()).make_module(vars=env.globals)

    def _statement(self, name, fetch_result=False, hidden=False, caller=None):
        self.statements.append((name, " ".join(caller().split())))
        return ""

    def _load_result(self, name):
        assert name in ("get_drift_metadata", "get_drift_columns")
        assert self.names[-1] == name, f"{name} loaded before its query ran"
        self.loaded.append((name, object()))
        return SimpleNamespace(table=self.loaded[-1][1])

    def run(self, has_select_query=True, enforce="all"):
        self.module.check_for_schema_drift(self.existing, has_select_query, enforce)

    @property
    def names(self):
        return [name for name, _ in self.statements]

    def sqls(self, name):
        return [sql for statement, sql in self.statements if statement == name]

    def sql(self, name):
        (sql,) = self.sqls(name)
        return sql

    def table(self, name):
        (table,) = [table for loaded, table in self.loaded if loaded == name]
        return table

    @property
    def drift_call(self):
        self.adapter.check_schema_drift.assert_called_once()
        return self.adapter.check_schema_drift.call_args


def _temp_relation():
    return relation(TEMP_NAME)


@pytest.mark.parametrize(
    "materialized,mode",
    [("table", "snapshot"), ("streaming_table", "streaming_query")],
)
def test_dry_run_path_creates_nothing(materialized, mode):
    harness = _Harness({"materialized": materialized}, dry_run_result=DRY_RUN)

    harness.run()

    # No DROP, no CREATE, no COLUMNS read: only the metadata query, over TABLES and
    # TABLE_OPTIONS.
    assert harness.names == ["get_drift_metadata"]
    metadata = harness.sql("get_drift_metadata")
    assert COLUMNS_FROM not in metadata
    assert "FROM INFORMATION_SCHEMA.`TABLES`" in metadata
    assert "FROM INFORMATION_SCHEMA.`TABLE_OPTIONS`" in metadata
    assert metadata.count("UNION ALL") == 1
    metadata_table = harness.table("get_drift_metadata")
    harness.adapter.get_columns_from_dry_run.assert_called_once_with(
        harness.existing, metadata_table, MODEL_SQL, execution_mode=mode, compute_pool_id=None
    )
    args, kwargs = harness.drift_call
    assert args[0] == harness.existing
    assert args[1] is None
    assert args[2] is metadata_table
    assert kwargs == {"dry_run_columns": DRY_RUN, "drift_columns": None}
    harness.adapter.defer_drop.assert_not_called()


def test_metadata_runs_before_the_dry_runs():
    """The resolver checks the metadata for a materialized or missing table before it
    dry-runs, so the metadata query must already have run when it's called."""
    harness = _Harness({"materialized": "table"}, dry_run_result=DRY_RUN)
    names_at_call = []
    harness.adapter.get_columns_from_dry_run.side_effect = lambda *args, **kwargs: (
        names_at_call.append(list(harness.names)) or DRY_RUN
    )

    harness.run()

    assert names_at_call == [["get_drift_metadata"]]


@pytest.mark.parametrize("model_mode", ["snapshot_ddl", "streaming_ddl", "streaming_query"])
@pytest.mark.parametrize(
    "materialized,mode",
    [("table", "snapshot"), ("streaming_table", "streaming_query")],
)
def test_materialization_mode_wins_over_configured_mode(materialized, mode, model_mode):
    """The mapping pins the mode, so a DDL-mode model config can't reach the dry-run. The
    explicit mode also means the dry-run never falls back to a DDL-mode profile."""
    harness = _Harness(
        {"materialized": materialized, "execution_mode": model_mode},
        dry_run_result=DRY_RUN,
    )
    harness.run()
    _, kwargs = harness.adapter.get_columns_from_dry_run.call_args
    assert kwargs["execution_mode"] == mode


@pytest.mark.parametrize("model_mode", [None, "snapshot"])
def test_other_materializations_keep_configured_mode(model_mode):
    """A materialization outside the mapping keeps the temp-table CTAS's old resolution:
    the model's execution_mode, else None (the profile's)."""
    config = {"materialized": "custom_select"}
    if model_mode is not None:
        config["execution_mode"] = model_mode
    harness = _Harness(config, dry_run_result=DRY_RUN)
    harness.run()
    _, kwargs = harness.adapter.get_columns_from_dry_run.call_args
    assert kwargs["execution_mode"] == model_mode


def test_compute_pool_passed_to_dry_run():
    harness = _Harness(
        {"materialized": "table", "compute_pool_id": "lfcp-1"}, dry_run_result=DRY_RUN
    )
    harness.run()
    _, kwargs = harness.adapter.get_columns_from_dry_run.call_args
    assert kwargs["compute_pool_id"] == "lfcp-1"


def test_dry_run_path_reclaims_leaked_temp_table():
    """The fallback's DROP IF EXISTS doesn't run on the dry-run path, so a temp table
    leaked by an earlier hard-killed run is found in the relation cache and deferred."""
    leaked = _temp_relation()
    harness = _Harness({"materialized": "table"}, dry_run_result=DRY_RUN, leaked=leaked)

    harness.run()

    harness.adapter.get_relation.assert_called_once_with("env-1", "cluster-a", TEMP_NAME)
    harness.adapter.defer_drop.assert_called_once_with(leaked)
    assert harness.names == ["get_drift_metadata"]


def test_fallback_path_reads_columns_after_the_temp_table():
    """The resolver returned None: the temp table is dropped, deferred, created from the
    wrapped SELECT and read back by the columns query. The metadata isn't read again.
    check_schema_drift gets the metadata and the columns."""
    harness = _Harness({"materialized": "table"}, dry_run_result=None)

    harness.run()

    assert harness.names == [
        "get_drift_metadata",
        "drop_leaked_temp_table",
        "create_temp_table",
        "get_drift_columns",
    ]
    temp = _temp_relation()
    assert harness.sql("drop_leaked_temp_table") == f"DROP TABLE IF EXISTS {temp}"
    assert harness.sql("create_temp_table") == (
        f"CREATE TABLE {temp} AS SELECT * FROM ( {MODEL_SQL} ) WHERE FALSE"
    )
    assert COLUMNS_FROM not in harness.sql("get_drift_metadata")
    columns = harness.sql("get_drift_columns")
    assert columns.count(COLUMNS_FROM) == 2
    assert "TABLE_NAME = 'my_model'" in columns
    assert f"TABLE_NAME = '{TEMP_NAME}'" in columns
    assert "INFORMATION_SCHEMA.`TABLES`" not in columns
    assert "INFORMATION_SCHEMA.`TABLE_OPTIONS`" not in columns
    harness.adapter.defer_drop.assert_called_once_with(temp)
    harness.adapter.get_relation.assert_not_called()
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert args[2] is harness.table("get_drift_metadata")
    assert kwargs == {
        "dry_run_columns": None,
        "drift_columns": harness.table("get_drift_columns"),
    }


def test_streaming_source_skips_dry_run():
    """Column definitions aren't a SELECT: no dry-run. The metadata query runs first, as on
    every path, and the columns query after the DDL temp table."""
    harness = _Harness(
        {"materialized": "streaming_source", "connector": "faker", "with": {"a": "b"}}
    )

    harness.run(has_select_query=False)

    harness.adapter.get_columns_from_dry_run.assert_not_called()
    assert harness.names == [
        "get_drift_metadata",
        "drop_leaked_temp_table",
        "create_temp_table",
        "get_drift_columns",
    ]
    temp = _temp_relation()
    assert harness.sql("create_temp_table") == f"CREATE TABLE {temp} ( {MODEL_SQL} )"
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert args[2] is harness.table("get_drift_metadata")
    assert args[3:] == ({"a": "b"}, None, "all", "faker")
    assert kwargs == {
        "dry_run_columns": None,
        "drift_columns": harness.table("get_drift_columns"),
    }


def test_enforce_passes_through_on_dry_run_path():
    harness = _Harness({"materialized": "streaming_table"}, dry_run_result=DRY_RUN)
    harness.run(enforce="columns")
    args, _ = harness.drift_call
    assert args[5] == "columns"


@pytest.mark.parametrize(
    "materialized,has_select_query,dry_run_result",
    [("table", True, DRY_RUN), ("table", True, None), ("streaming_source", False, None)],
    ids=["dry-run", "fallback", "streaming_source"],
)
def test_distributed_by_reads_distribution_columns(materialized, has_select_query, dry_run_result):
    """With distributed_by, the metadata query also reads the existing table's distribution
    columns, and only those: COLUMNS rows with a distribution position, under their own
    section."""
    harness = _Harness(
        {"materialized": materialized, "distributed_by": {"columns": ["id"]}},
        dry_run_result=dry_run_result,
    )

    harness.run(has_select_query=has_select_query)

    metadata = harness.sql("get_drift_metadata")
    assert metadata.count(COLUMNS_FROM) == 1
    assert "'DISTRIBUTION' AS section" in metadata
    assert DISTRIBUTION_FILTER in metadata
    assert "TABLE_NAME = 'my_model'" in metadata
    assert f"TABLE_NAME = '{TEMP_NAME}'" not in metadata
    assert metadata.count("UNION ALL") == 2


def test_enforce_columns_skips_distribution_columns():
    """enforce='columns' never checks distribution, so the metadata query doesn't read it,
    even with distributed_by set."""
    harness = _Harness(
        {"materialized": "streaming_table", "distributed_by": {"columns": ["id"]}},
        dry_run_result=DRY_RUN,
    )

    harness.run(enforce="columns")

    assert COLUMNS_FROM not in harness.sql("get_drift_metadata")
