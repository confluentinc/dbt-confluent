"""Unit tests for the SQL logic check: `check_logic_drift`'s error, and its `decide_action` wiring.

The wiring tests render the real helpers.sql with plain jinja2 and stand-ins for the dbt context
the macros touch (as test_schema_drift_macros.py does), then check which adapter methods
`decide_action` calls. Driven through the public `decide_action`, since `make_module` doesn't
export the underscore macros.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import jinja2
import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter, ConfluentRelation
from dbt.exceptions import CompilationError
from tests.unit._helpers import relation

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)
MODEL_SQL = "select id, price from orders where region = 'US'"
STORED_SQL = (
    "INSERT INTO `env-1`.`cluster-a`.`my_model` (id, price)\n    select id, price\n    from (\n"
    f"        {MODEL_SQL}\n    ) as model_subq"
)
STATEMENT_NAME = "dbt-p-my_model"


class TestCheckLogicDrift:
    def _check(self, statement_sql, model_sql=MODEL_SQL):
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        adapter.check_logic_drift(relation("my_model"), STATEMENT_NAME, statement_sql, model_sql)

    def test_match_does_not_raise(self):
        self._check(STORED_SQL, "SELECT id,\n  price FROM orders -- note\nWHERE region = 'US'")

    def test_no_stored_sql_does_not_raise(self):
        self._check(None)

    def test_mismatch_raises_with_both_sql_texts_and_ways_out(self):
        changed = MODEL_SQL.replace("'US'", "'EU'")
        with pytest.raises(CompilationError) as error:
            self._check(STORED_SQL, changed)
        message = str(error.value)
        assert "SQL logic drift detected for `env-1`.`cluster-a`.`my_model`" in message
        assert f"statement '{STATEMENT_NAME}'" in message
        assert "region = 'US'" in message
        assert "region = 'EU'" in message
        assert "--full-refresh" in message
        assert f"stop or delete statement '{STATEMENT_NAME}'" in message
        # Deliberately not offered as a way out: it would switch off every drift check.
        assert "on_schema_drift" not in message

    def test_long_sql_is_cut(self):
        long_model = "select " + ", ".join(f"c{i}" for i in range(5_000)) + " from t"
        with pytest.raises(CompilationError) as error:
            self._check(STORED_SQL, long_model)
        message = str(error.value)
        assert f"... ({len(long_model) - 10_000} more characters)" in message
        assert long_model not in message


class _MacroReturn(Exception):
    """dbt's return(): raised by the `return` stand-in, caught at the macro-call boundary."""

    def __init__(self, value):
        super().__init__()
        self.value = value


def _raise_return(value):
    raise _MacroReturn(value)


@pytest.fixture(autouse=True)
def dbt_return(monkeypatch):
    """Make every jinja2 macro call catch _MacroReturn and return its value, as dbt's macro
    wrapper does, so `{{ return(x) }}` exits only the current macro."""
    invoke = jinja2.runtime.Macro._invoke

    def _invoke(self, arguments, autoescape):
        try:
            return invoke(self, arguments, autoescape)
        except _MacroReturn as returned:
            return returned.value

    monkeypatch.setattr(jinja2.runtime.Macro, "_invoke", _invoke)


class _Config:
    """Stand-in for dbt's `config`, backed by a dict of model config."""

    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


def _render_decide_action(
    *,
    config,
    existing,
    inspected,
    recoverable=True,
    full_refresh=False,
    empty=False,
    invocation_args=None,
):
    """Render helpers.sql and run decide_action. Returns (action, adapter mock)."""
    adapter = MagicMock()
    adapter.Relation = ConfluentRelation
    adapter.inspect_statement.return_value = inspected
    adapter.generate_schema_check_temp_name.return_value = "__dbt_tmp_schema_check_my_model"
    # Opaque stand-in: the dry-run path, so check_for_schema_drift creates no temp table.
    adapter.get_columns_from_dry_run.return_value = object()
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.globals.update(
        adapter=adapter,
        config=_Config(config),
        this=relation("my_model"),
        sql=MODEL_SQL,
        should_full_refresh=lambda: full_refresh,
        get_statement_name=lambda suffix="": STATEMENT_NAME + suffix,
        log=lambda message, info=False: "",
        statement=lambda name, fetch_result=False, hidden=False, caller=None: "",
        load_result=lambda name: SimpleNamespace(table=object()),
        exceptions=SimpleNamespace(raise_compiler_error=CompilationError),
        flags=SimpleNamespace(EMPTY=empty),
        invocation_args_dict=invocation_args or {},
        execute=True,
    )
    env.globals["return"] = _raise_return
    # Macros from other files that the _prepare_create path calls.
    stubs = (
        "{% macro disable_old_tableflow_before_drop(relation) %}{% endmacro %}\n"
        "{% macro drop_relation_if_exists(relation) %}{% endmacro %}\n"
    )
    module = env.from_string(stubs + MACRO_FILE.read_text()).make_module(vars=env.globals)
    action = module.decide_action(existing, has_select_query=True, recoverable=recoverable)
    return action, adapter


HEALTHY = {"needs_restart": False, "sql": STORED_SQL}
TERMINAL = {"needs_restart": True, "sql": STORED_SQL}
MISSING = {"needs_restart": True, "sql": None}


@pytest.mark.parametrize(
    "kwargs,expected_action,looked_up,logic_checked",
    [
        pytest.param({}, "skip", True, True, id="healthy-fail-checks"),
        pytest.param(
            {"config": {"on_schema_drift": "ignore"}}, "skip", True, False, id="healthy-ignore"
        ),
        pytest.param({"empty": True}, "skip", True, False, id="healthy-empty-run"),
        pytest.param(
            {"invocation_args": {"sample": "3 days"}}, "skip", True, False, id="healthy-sample-run"
        ),
        pytest.param({"inspected": TERMINAL}, "restart", True, False, id="terminal-restarts"),
        pytest.param({"inspected": MISSING}, "restart", True, False, id="missing-restarts"),
        pytest.param({"existing": None}, "create", False, False, id="no-relation"),
        pytest.param({"full_refresh": True}, "create", False, False, id="full-refresh"),
        pytest.param({"recoverable": False}, "skip", False, False, id="not-recoverable"),
    ],
)
def test_decide_action_wiring(kwargs, expected_action, looked_up, logic_checked):
    arguments = {
        "config": {"on_schema_drift": "fail"},
        "existing": relation("my_model"),
        "inspected": HEALTHY,
        **kwargs,
    }

    action, adapter = _render_decide_action(**arguments)

    assert action == expected_action
    calls = [name for name, _, _ in adapter.method_calls]
    # One lookup at most: the logic check reuses the restart check's get_statement.
    assert calls.count("inspect_statement") == (1 if looked_up else 0)
    if looked_up:
        adapter.inspect_statement.assert_called_once_with(STATEMENT_NAME)
    if logic_checked:
        adapter.check_logic_drift.assert_called_once_with(
            arguments["existing"], STATEMENT_NAME, STORED_SQL, MODEL_SQL
        )
        # Schema first, so a column change keeps its more specific error.
        assert calls.index("check_schema_drift") < calls.index("check_logic_drift")
    else:
        adapter.check_logic_drift.assert_not_called()
    assert "statement_needs_restart" not in calls
