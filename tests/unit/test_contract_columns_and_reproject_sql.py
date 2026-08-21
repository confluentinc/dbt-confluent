"""Unit tests for the `render_contract_columns_and_reproject_sql` Jinja macro.

Shared by `table` (relations/table/create.sql) and `materialized_table`, this
macro is what makes an enforced contract's declared column order win over the
model SQL's select order in the emitted DDL. materialized_table's own copy of
this logic once omitted the re-projection step (PR #84 review) -- exercising
that requires a live Flink cluster to observe (a positional column mismatch),
so this test instead renders the real macro source directly and asserts on
its return value: was `sql` actually re-projected, and were the underlying
dbt-core contract macros (get_assert_columns_equivalent,
get_table_columns_and_constraints, get_select_subquery) all invoked -- fast,
no cluster required.
"""

from pathlib import Path

import jinja2

MACRO_FILE = (
    Path(__file__).resolve().parents[2]
    / "dbt/include/confluent/macros/materializations/models/helpers.sql"
)


class _Config:
    """Stand-in for dbt's `config`; only `contract` is read by the macro."""

    def __init__(self, contract):
        self._contract = contract

    def get(self, key, default=None):
        return self._contract if key == "contract" else default


class _MacroReturn(Exception):
    """Stand-in for dbt's `return()` global (dbt_common.clients.jinja), which
    is a dbt-injected macro, not a Jinja builtin: it raises to unwind the
    macro's normal string-output rendering and hand back a value of any
    type -- required here since the macro returns a dict, not a string."""

    def __init__(self, value):
        self.value = value


def _render(sql, contract):
    """Render the real macro against fake dbt-core contract macros, tracking
    which of them were invoked and with what `sql`."""
    calls = []

    def get_assert_columns_equivalent(sql):
        calls.append(("assert_columns_equivalent", sql))
        return ""

    def get_table_columns_and_constraints():
        calls.append(("table_columns_and_constraints",))
        return "(fake column defs)"

    def get_select_subquery(sql):
        calls.append(("select_subquery", sql))
        return f"REPROJECTED({sql})"

    def _return(value):
        raise _MacroReturn(value)

    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.globals["config"] = _Config(contract)
    env.globals["get_assert_columns_equivalent"] = get_assert_columns_equivalent
    env.globals["get_table_columns_and_constraints"] = get_table_columns_and_constraints
    env.globals["get_select_subquery"] = get_select_subquery
    env.globals["return"] = _return
    module = env.from_string(MACRO_FILE.read_text()).make_module(vars=env.globals)
    try:
        module.render_contract_columns_and_reproject_sql(sql)
        raise AssertionError("macro did not call return()")
    except _MacroReturn as e:
        result = e.value
    return result, calls


class TestRenderContractColumnsAndReprojectSql:
    def test_enforced_contract_reprojects_sql_and_renders_columns(self):
        sql = "select b, a from t"
        result, calls = _render(sql, {"enforced": True})

        assert result["sql"] == f"REPROJECTED({sql})", (
            "sql must be re-projected via get_select_subquery when the "
            "contract is enforced -- this is the exact step PR #84's review "
            "found missing from materialized_table's copy of this logic"
        )
        assert "(fake column defs)" in result["ddl"]
        assert calls == [
            ("assert_columns_equivalent", sql),
            ("table_columns_and_constraints",),
            ("select_subquery", sql),
        ]

    def test_unenforced_contract_leaves_sql_untouched(self):
        sql = "select b, a from t"
        result, calls = _render(sql, {"enforced": False})

        assert result == {"ddl": "", "sql": sql}
        assert calls == []

    def test_unset_contract_leaves_sql_untouched(self):
        sql = "select b, a from t"
        result, calls = _render(sql, None)

        assert result == {"ddl": "", "sql": sql}
        assert calls == []
