"""Unit tests for the `check_for_schema_drift` Jinja wiring (GH-118).

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
        # One stand-in table per load_result call, in order.
        self.catalog_tables: list[object] = []
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
        assert name == "get_drift_catalog"
        assert self.names[-1] == "get_drift_catalog", "loaded before the catalog query ran"
        self.catalog_tables.append(object())
        return SimpleNamespace(table=self.catalog_tables[-1])

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

    # No DROP, no CREATE: only the catalog query, with the existing table's COLUMNS alone.
    assert harness.names == ["get_drift_catalog"]
    catalog = harness.sql("get_drift_catalog")
    assert catalog.count(COLUMNS_FROM) == 1
    assert f"TABLE_NAME = '{TEMP_NAME}'" not in catalog
    assert catalog.count("UNION ALL") == 2
    (catalog_table,) = harness.catalog_tables
    harness.adapter.get_columns_from_dry_run.assert_called_once_with(
        harness.existing, catalog_table, MODEL_SQL, execution_mode=mode, compute_pool_id=None
    )
    args, kwargs = harness.drift_call
    assert args[0] == harness.existing
    assert args[1] is None
    assert args[2] is catalog_table
    assert kwargs == {"dry_run_columns": DRY_RUN}
    harness.adapter.defer_drop.assert_not_called()


def test_catalog_runs_before_the_dry_runs():
    """The resolver checks the catalog for a materialized or unreadable table before it
    dry-runs, so the catalog query must already have run when it's called."""
    harness = _Harness({"materialized": "table"}, dry_run_result=DRY_RUN)
    names_at_call = []
    harness.adapter.get_columns_from_dry_run.side_effect = lambda *args, **kwargs: (
        names_at_call.append(list(harness.names)) or DRY_RUN
    )

    harness.run()

    assert names_at_call == [["get_drift_catalog"]]


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
    assert harness.names == ["get_drift_catalog"]


def test_fallback_path_rereads_the_catalog():
    """The resolver returned None: the temp table is dropped, deferred, created from the
    wrapped SELECT and read back, as before GH-118, by a second catalog query that covers it.
    check_schema_drift gets that second catalog."""
    harness = _Harness({"materialized": "table"}, dry_run_result=None)

    harness.run()

    assert harness.names == [
        "get_drift_catalog",
        "drop_leaked_temp_table",
        "create_temp_table",
        "get_drift_catalog",
    ]
    temp = _temp_relation()
    assert harness.sql("drop_leaked_temp_table") == f"DROP TABLE IF EXISTS {temp}"
    assert harness.sql("create_temp_table") == (
        f"CREATE TABLE {temp} AS SELECT * FROM ( {MODEL_SQL} ) WHERE FALSE"
    )
    first, second = harness.sqls("get_drift_catalog")
    assert first.count(COLUMNS_FROM) == 1
    assert f"TABLE_NAME = '{TEMP_NAME}'" not in first
    assert second.count(COLUMNS_FROM) == 2
    assert f"TABLE_NAME = '{TEMP_NAME}'" in second
    harness.adapter.defer_drop.assert_called_once_with(temp)
    harness.adapter.get_relation.assert_not_called()
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert args[2] is harness.catalog_tables[1]
    assert kwargs == {"dry_run_columns": None}


def test_streaming_source_skips_dry_run():
    """Column definitions aren't a SELECT: no dry-run, and one catalog query after the DDL
    temp table, as before."""
    harness = _Harness(
        {"materialized": "streaming_source", "connector": "faker", "with": {"a": "b"}}
    )

    harness.run(has_select_query=False)

    harness.adapter.get_columns_from_dry_run.assert_not_called()
    assert harness.names == ["drop_leaked_temp_table", "create_temp_table", "get_drift_catalog"]
    temp = _temp_relation()
    assert harness.sql("create_temp_table") == f"CREATE TABLE {temp} ( {MODEL_SQL} )"
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert args[2] is harness.catalog_tables[0]
    assert args[3:] == ({"a": "b"}, None, "all", "faker")
    assert kwargs == {"dry_run_columns": None}


def test_enforce_passes_through_on_dry_run_path():
    harness = _Harness({"materialized": "streaming_table"}, dry_run_result=DRY_RUN)
    harness.run(enforce="columns")
    args, _ = harness.drift_call
    assert args[5] == "columns"
