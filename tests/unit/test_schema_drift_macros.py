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

from dbt.adapters.confluent.impl import ConfluentAdapter, ConfluentRelation
from tests.unit._helpers import relation

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)
MODEL_SQL = "select id from src"
EXPECTED = {"id": "BIGINT"}
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
        self.catalog_table = object()
        self.adapter = MagicMock()
        self.adapter.Relation = ConfluentRelation
        self.adapter.generate_schema_check_temp_name.side_effect = lambda identifier: (
            ConfluentAdapter.generate_schema_check_temp_name(
                ConfluentAdapter.__new__(ConfluentAdapter), identifier
            )
        )
        self.adapter.get_expected_columns_from_dry_run.return_value = dry_run_result
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
        return SimpleNamespace(table=self.catalog_table)

    def run(self, has_select_query=True, enforce="all"):
        self.module.check_for_schema_drift(self.existing, has_select_query, enforce)

    @property
    def names(self):
        return [name for name, _ in self.statements]

    def sql(self, name):
        (sql,) = [sql for statement, sql in self.statements if statement == name]
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
    harness = _Harness({"materialized": materialized}, dry_run_result=EXPECTED)

    harness.run()

    # No DROP, no CREATE: only the catalog query, with the existing table's COLUMNS alone.
    assert harness.names == ["get_drift_catalog"]
    catalog = harness.sql("get_drift_catalog")
    assert catalog.count(COLUMNS_FROM) == 1
    assert f"TABLE_NAME = '{TEMP_NAME}'" not in catalog
    assert catalog.count("UNION ALL") == 2
    harness.adapter.get_expected_columns_from_dry_run.assert_called_once_with(
        harness.existing, MODEL_SQL, execution_mode=mode, compute_pool_id=None
    )
    args, kwargs = harness.drift_call
    assert args[0] == harness.existing
    assert args[1] is None
    assert args[2] is harness.catalog_table
    assert kwargs == {"expected_columns": EXPECTED}
    harness.adapter.defer_drop.assert_not_called()


@pytest.mark.parametrize("model_mode", ["snapshot_ddl", "streaming_ddl", "streaming_query"])
@pytest.mark.parametrize(
    "materialized,mode",
    [("table", "snapshot"), ("streaming_table", "streaming_query")],
)
def test_materialization_mode_wins_over_configured_mode(materialized, mode, model_mode):
    """The mapping pins the mode, so a DDL-mode model config can't reach the dry-run. The
    explicit mode also means add_query never falls back to a DDL-mode profile."""
    harness = _Harness(
        {"materialized": materialized, "execution_mode": model_mode},
        dry_run_result=EXPECTED,
    )
    harness.run()
    _, kwargs = harness.adapter.get_expected_columns_from_dry_run.call_args
    assert kwargs["execution_mode"] == mode


@pytest.mark.parametrize("model_mode", [None, "snapshot"])
def test_other_materializations_keep_configured_mode(model_mode):
    """A materialization outside the mapping keeps the temp-table CTAS's old resolution:
    the model's execution_mode, else None (the profile's)."""
    config = {"materialized": "custom_select"}
    if model_mode is not None:
        config["execution_mode"] = model_mode
    harness = _Harness(config, dry_run_result=EXPECTED)
    harness.run()
    _, kwargs = harness.adapter.get_expected_columns_from_dry_run.call_args
    assert kwargs["execution_mode"] == model_mode


def test_compute_pool_passed_to_dry_run():
    harness = _Harness(
        {"materialized": "table", "compute_pool_id": "lfcp-1"}, dry_run_result=EXPECTED
    )
    harness.run()
    _, kwargs = harness.adapter.get_expected_columns_from_dry_run.call_args
    assert kwargs["compute_pool_id"] == "lfcp-1"


def test_dry_run_path_reclaims_leaked_temp_table():
    """The fallback's DROP IF EXISTS doesn't run on the dry-run path, so a temp table
    leaked by an earlier hard-killed run is found in the relation cache and deferred."""
    leaked = _temp_relation()
    harness = _Harness({"materialized": "table"}, dry_run_result=EXPECTED, leaked=leaked)

    harness.run()

    harness.adapter.get_relation.assert_called_once_with("env-1", "cluster-a", TEMP_NAME)
    harness.adapter.defer_drop.assert_called_once_with(leaked)
    assert harness.names == ["get_drift_catalog"]


def test_fallback_path_unchanged():
    """The resolver returned None: the temp table is dropped, deferred, created from the
    wrapped SELECT and read back, exactly as before GH-118."""
    harness = _Harness({"materialized": "table"}, dry_run_result=None)

    harness.run()

    assert harness.names == ["drop_leaked_temp_table", "create_temp_table", "get_drift_catalog"]
    temp = _temp_relation()
    assert harness.sql("drop_leaked_temp_table") == f"DROP TABLE IF EXISTS {temp}"
    assert harness.sql("create_temp_table") == (
        f"CREATE TABLE {temp} AS SELECT * FROM ( {MODEL_SQL} ) WHERE FALSE"
    )
    catalog = harness.sql("get_drift_catalog")
    assert catalog.count(COLUMNS_FROM) == 2
    assert f"TABLE_NAME = '{TEMP_NAME}'" in catalog
    harness.adapter.defer_drop.assert_called_once_with(temp)
    harness.adapter.get_relation.assert_not_called()
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert kwargs == {"expected_columns": None}


def test_streaming_source_skips_dry_run():
    """Column definitions aren't a SELECT: no dry-run, the DDL temp table as before."""
    harness = _Harness(
        {"materialized": "streaming_source", "connector": "faker", "with": {"a": "b"}}
    )

    harness.run(has_select_query=False)

    harness.adapter.get_expected_columns_from_dry_run.assert_not_called()
    temp = _temp_relation()
    assert harness.sql("create_temp_table") == f"CREATE TABLE {temp} ( {MODEL_SQL} )"
    args, kwargs = harness.drift_call
    assert args[1] == temp
    assert args[3:] == ({"a": "b"}, None, "all", "faker")
    assert kwargs == {"expected_columns": None}


def test_enforce_passes_through_on_dry_run_path():
    harness = _Harness({"materialized": "streaming_table"}, dry_run_result=EXPECTED)
    harness.run(enforce="columns")
    args, _ = harness.drift_call
    assert args[5] == "columns"
