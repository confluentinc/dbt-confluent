"""Unit tests for the `decide_action` Jinja wiring with logic drift checks.

Renders the real helpers.sql with plain jinja2 and stand-ins for the dbt context the macro
touches, then checks which adapter methods it calls and in what order. Driven through the
public `decide_action`, since `make_module` doesn't export the underscore macros.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import jinja2
import pytest

from dbt.adapters.confluent.impl import ConfluentRelation
from dbt.exceptions import CompilationError
from tests.unit._helpers import relation


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


MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)
MODEL_SQL = "select id, price from orders where region = 'US'"
STORED_SQL = "INSERT INTO tbl (id, price)\n    select id, price\n    from (\n        "
STORED_SQL += MODEL_SQL
STORED_SQL += "\n    ) as model_subq"


class _Config:
    """Stand-in for dbt's `config`, backed by a dict of model config."""

    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


class _Harness:
    """Renders decide_action and records what it called."""

    def __init__(
        self,
        config,
        existing_relation=None,
        should_full_refresh_value=False,
        inspect_statement_result=None,
        flags_empty=False,
        invocation_args=None,
    ):
        self.existing = existing_relation
        self.adapter = MagicMock()
        self.adapter.Relation = ConfluentRelation
        # Convert dict to SimpleNamespace so Jinja can access attributes with dot notation
        if inspect_statement_result is not None and isinstance(inspect_statement_result, dict):
            self.adapter.inspect_statement.return_value = SimpleNamespace(
                **inspect_statement_result
            )
        else:
            self.adapter.inspect_statement.return_value = inspect_statement_result

        # For recording call order: attach the methods we care about to a parent mock.
        self.call_parent = MagicMock()
        self.adapter.attach_mock(self.adapter.inspect_statement, "inspect_statement")
        self.adapter.attach_mock(self.adapter.check_logic_drift, "check_logic_drift")
        self.adapter.attach_mock(self.adapter.delete_statement, "delete_statement")

        # Stand-ins for check_for_schema_drift's dependencies: it calls
        # generate_schema_check_temp_name, get_columns_from_dry_run, partition_drift_metadata,
        # check_schema_drift, defer_drop. We stub them all to return opaque values.
        self.adapter.generate_schema_check_temp_name.return_value = relation("__dbt_tmp")
        self.adapter.get_columns_from_dry_run.return_value = object()
        self.adapter.partition_drift_metadata.return_value = object()
        self.adapter.check_schema_drift.return_value = None
        self.adapter.defer_drop.return_value = None

        self.logs = []
        self.statements_run = []

        env = jinja2.Environment(extensions=["jinja2.ext.do"])
        env.globals.update(
            adapter=self.adapter,
            config=_Config(config),
            this=self.existing,
            sql=MODEL_SQL,
            should_full_refresh=lambda: should_full_refresh_value,
            get_statement_name=lambda suffix="": "dbt-p-my_model" + suffix,
            log=self._log,
            statement=self._statement,
            load_result=self._load_result,
            exceptions=SimpleNamespace(raise_compiler_error=self._raise_compiler_error),
            flags=SimpleNamespace(EMPTY=flags_empty),
            invocation_args_dict=invocation_args or {},
            execute=True,
        )
        env.globals["return"] = _raise_return
        env.tests["none"] = lambda x: x is None

        # Prepend stub macros that the _prepare_* helpers call
        template_with_stubs = """
{% macro disable_old_tableflow_before_drop(relation) %}{% endmacro %}
{% macro drop_relation_if_exists(relation) %}{% endmacro %}
{% macro check_for_schema_drift(relation, has_select_query, enforce='all') %}{% endmacro %}
""" + MACRO_FILE.read_text()
        self.module = env.from_string(template_with_stubs).make_module(vars=env.globals)

    def _log(self, message, info=False):
        self.logs.append((message, info))
        return ""

    def _statement(self, name, fetch_result=False, hidden=False, caller=None):
        if caller:
            sql = caller()
            self.statements_run.append((name, " ".join(sql.split())))
        return ""

    def _load_result(self, name):
        return SimpleNamespace(table=object())

    def _raise_compiler_error(self, message):
        raise CompilationError(message)

    def run(self, has_select_query=True, recoverable=False):
        result = self.module.decide_action(
            self.existing, has_select_query=has_select_query, recoverable=recoverable
        )
        return result

    @property
    def method_calls(self):
        """Return the names of adapter methods called, in order."""
        return [name for name, args, kwargs in self.adapter.method_calls]


def test_no_relation_creates_without_lookup():
    """Create path: no relation, so inspect_statement is never called."""
    harness = _Harness(config={}, existing_relation=None)

    action = harness.run(recoverable=True)

    assert action == "create"
    assert "inspect_statement" not in harness.method_calls
    assert "check_logic_drift" not in harness.method_calls


def test_full_refresh_creates_without_lookup():
    """Full refresh: inspect_statement is never called."""
    harness = _Harness(
        config={},
        existing_relation=relation("my_model"),
        should_full_refresh_value=True,
    )

    action = harness.run(recoverable=True)

    assert action == "create"
    assert "inspect_statement" not in harness.method_calls


def test_not_recoverable_skips_without_lookup():
    """Non-recoverable materialization: inspect_statement and check_logic_drift not called."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": STORED_SQL},
    )

    action = harness.run(recoverable=False)

    assert action == "skip"
    assert "inspect_statement" not in harness.method_calls
    assert "check_logic_drift" not in harness.method_calls


def test_healthy_fail_checks_logic():
    """Healthy statement with on_schema_drift='fail': check_logic_drift is called."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": STORED_SQL},
    )

    action = harness.run(recoverable=True)

    assert action == "skip"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    harness.adapter.check_logic_drift.assert_called_once_with(
        harness.existing, "dbt-p-my_model", STORED_SQL, MODEL_SQL
    )


def test_healthy_ignore_no_check():
    """Healthy with on_schema_drift='ignore': check_logic_drift not called."""
    harness = _Harness(
        config={"on_schema_drift": "ignore"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": STORED_SQL},
    )

    action = harness.run(recoverable=True)

    assert action == "skip"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    assert "check_logic_drift" not in harness.method_calls


def test_healthy_without_sql_no_check():
    """Healthy statement with sql=None: check_logic_drift not called."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": None},
    )

    action = harness.run(recoverable=True)

    assert action == "skip"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    assert "check_logic_drift" not in harness.method_calls


def test_terminal_fail_checks_before_delete():
    """Terminal statement: check_logic_drift is called before delete_statement."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": True, "sql": STORED_SQL},
    )

    action = harness.run(recoverable=True)

    assert action == "restart"
    harness.adapter.check_logic_drift.assert_called_once_with(
        harness.existing, "dbt-p-my_model", STORED_SQL, MODEL_SQL
    )
    # Check call order: inspect_statement, check_logic_drift, delete_statement
    method_names = harness.method_calls
    assert "inspect_statement" in method_names
    assert "check_logic_drift" in method_names
    assert "delete_statement" in method_names
    inspect_index = method_names.index("inspect_statement")
    logic_index = method_names.index("check_logic_drift")
    delete_index = method_names.index("delete_statement")
    assert inspect_index < logic_index < delete_index


def test_terminal_drift_keeps_statement():
    """Terminal statement, check_logic_drift raises: error propagates, delete not called."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": True, "sql": STORED_SQL},
    )
    harness.adapter.check_logic_drift.side_effect = CompilationError("SQL logic drift detected")

    with pytest.raises(CompilationError, match="SQL logic drift detected"):
        harness.run(recoverable=True)

    harness.adapter.check_logic_drift.assert_called_once()
    assert "delete_statement" not in harness.method_calls


def test_missing_restarts_without_check():
    """Missing statement (sql=None): restart without check_logic_drift; delete is called."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": True, "sql": None},
    )

    action = harness.run(recoverable=True)

    assert action == "restart"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    assert "check_logic_drift" not in harness.method_calls
    harness.adapter.delete_statement.assert_called()


def test_terminal_ignore_no_check():
    """Terminal with on_schema_drift='ignore': no check_logic_drift."""
    harness = _Harness(
        config={"on_schema_drift": "ignore"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": True, "sql": STORED_SQL},
    )

    action = harness.run(recoverable=True)

    assert action == "restart"
    assert "check_logic_drift" not in harness.method_calls


def test_empty_run_no_check():
    """Healthy, flags.EMPTY=True: no check_logic_drift."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": STORED_SQL},
        flags_empty=True,
    )

    action = harness.run(recoverable=True)

    assert action == "skip"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    assert "check_logic_drift" not in harness.method_calls


def test_sample_run_no_check():
    """Healthy, invocation_args_dict has 'sample': no check_logic_drift."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": False, "sql": STORED_SQL},
        invocation_args={"sample": object()},
    )

    action = harness.run(recoverable=True)

    assert action == "skip"
    harness.adapter.inspect_statement.assert_called_once_with("dbt-p-my_model")
    assert "check_logic_drift" not in harness.method_calls


def test_empty_run_restart_no_check():
    """Terminal, flags.EMPTY=True: restart without check_logic_drift."""
    harness = _Harness(
        config={"on_schema_drift": "fail"},
        existing_relation=relation("my_model"),
        inspect_statement_result={"needs_restart": True, "sql": STORED_SQL},
        flags_empty=True,
    )

    action = harness.run(recoverable=True)

    assert action == "restart"
    assert "check_logic_drift" not in harness.method_calls
