"""Unit tests for contract check helpers: fit_contract_columns, get_column_schema_from_query, and
the confluent__get_assert_columns_equivalent macro."""

import copy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest
from confluent_sql.types import ColumnTypeDefinition

from dbt.adapters.confluent import ConfluentColumn, ConfluentConnectionManager
from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.exceptions import ContractError

MAX = 2147483647


def col(name, dtype, char_size=None):
    return ConfluentColumn(name, dtype, char_size=char_size)


# --- U2: fit_contract_columns: (case id, sql column, yml column, fitted?) ---
FIT_CASES = [
    ("C1-char-string", col("c", "CHAR", 3), col("c", "VARCHAR", MAX), True),
    ("C2-char0-string", col("c", "CHAR", 0), col("c", "VARCHAR", MAX), True),
    ("C3-binary-bytes", col("c", "BINARY", 2), col("c", "VARBINARY", MAX), True),
    ("C4-binary0-bytes", col("c", "BINARY", 0), col("c", "VARBINARY", MAX), True),
    ("C5-char-short-varchar", col("c", "CHAR", 3), col("c", "VARCHAR", 10), False),
    ("C6-binary-short-varbinary", col("c", "BINARY", 1), col("c", "VARBINARY", 10), False),
    ("C7-varchar-into-char", col("c", "VARCHAR", MAX), col("c", "CHAR", 3), False),
    ("C8-varbinary-into-binary", col("c", "VARBINARY", MAX), col("c", "BINARY", 2), False),
    ("C9-char-char", col("c", "CHAR", 3), col("c", "CHAR", 5), False),
    ("C10-char-bytes", col("c", "CHAR", 3), col("c", "VARBINARY", MAX), False),
    ("C10-binary-string", col("c", "BINARY", 2), col("c", "VARCHAR", MAX), False),
    ("C11-varchar-string", col("c", "VARCHAR", 3), col("c", "VARCHAR", MAX), False),
    ("C12-int-bigint", col("c", "INTEGER"), col("c", "BIGINT"), False),
    ("C13-array", col("c", "ARRAY"), col("c", "ARRAY"), False),
    ("C14-name-case-differs", col("C", "CHAR", 3), col("c", "VARCHAR", MAX), False),
    ("C15-yml-length-unknown", col("c", "CHAR", 3), col("c", "VARCHAR"), False),
]


@pytest.mark.parametrize(
    "sql_column,yml_column,fitted", [c[1:] for c in FIT_CASES], ids=[c[0] for c in FIT_CASES]
)
def test_fit_contract_columns(sql_column, yml_column, fitted):
    """U2: fit_contract_columns exact output for C1-C15."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    result = adapter.fit_contract_columns([sql_column], [yml_column])
    expected = replace(sql_column, dtype=yml_column.dtype, char_size=yml_column.char_size)
    assert result == [expected if fitted else sql_column]


def test_fit_contract_columns_keeps_order_and_unmatched_and_inputs():
    """U2: order kept, unmatched columns kept, inputs not modified."""
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    sql = [col("b", "CHAR", 1), col("extra", "CHAR", 2), col("a", "INTEGER")]
    yml = [col("a", "INTEGER"), col("b", "VARCHAR", MAX)]
    before = copy.deepcopy((sql, yml))
    assert adapter.fit_contract_columns(sql, yml) == [
        col("b", "VARCHAR", MAX),
        col("extra", "CHAR", 2),
        col("a", "INTEGER"),
    ]
    assert (sql, yml) == before


# --- U3: get_column_schema_from_query ---
class _Connections:
    def __init__(self, schema_columns):
        self.sql = None
        self._cursor = SimpleNamespace(
            statement=SimpleNamespace(schema=SimpleNamespace(columns=schema_columns))
        )

    def add_select_query(self, sql):
        self.sql = sql
        return None, self._cursor

    data_type_code_to_name = staticmethod(ConfluentConnectionManager.data_type_code_to_name)


def _schema_column(name, data):
    return SimpleNamespace(name=name, type=ColumnTypeDefinition.from_response(data))


def test_get_column_schema_from_query_adds_lengths():
    """U3: get_column_schema_from_query passes sql to add_select_query and returns exact (column,
    dtype, char_size) for all types."""
    connections = _Connections(
        [
            _schema_column("s", {"type": "CHAR", "nullable": False, "length": 3}),
            _schema_column("t", {"type": "VARCHAR", "nullable": True, "length": MAX}),
            _schema_column("b", {"type": "VARBINARY", "nullable": True, "length": MAX}),
            _schema_column("i", {"type": "INTEGER", "nullable": True}),
            _schema_column(
                "d", {"type": "DECIMAL", "nullable": True, "precision": 10, "scale": 2}
            ),
            _schema_column(
                "a",
                {
                    "type": "ARRAY",
                    "nullable": True,
                    "element_type": {"type": "CHAR", "nullable": False, "length": 1},
                },
            ),
        ]
    )
    adapter = ConfluentAdapter.__new__(ConfluentAdapter)
    adapter.connections = connections
    result = adapter.get_column_schema_from_query("SQL")
    assert connections.sql == "SQL"
    assert [(c.column, c.dtype, c.char_size) for c in result] == [
        ("s", "CHAR", 3),
        ("t", "VARCHAR", MAX),
        ("b", "VARBINARY", MAX),
        ("i", "INTEGER", None),
        ("d", "DECIMAL", None),
        ("a", "ARRAY", None),
    ]


# --- U4: confluent__get_assert_columns_equivalent, rendered ---
MACRO_FILE = (
    Path(__file__).resolve().parents[2] / "dbt/include/confluent/macros/adapters/columns.sql"
)


class _ContractRaised(Exception):
    def __init__(self, yaml_columns, sql_columns):
        self.yaml_columns, self.sql_columns = yaml_columns, sql_columns


def _format_columns(columns):
    # dbt's format_columns with default__format_column
    return [
        {"name": c.name, "data_type": c.dtype, "formatted": f"{c.column.lower()} {c.dtype}"}
        for c in columns
    ]


def _assert(sql_columns, yml_columns, fit=None):
    if fit is None:
        adapter = ConfluentAdapter.__new__(ConfluentAdapter)
        fit = adapter.fit_contract_columns

    def raise_contract_error(yaml_columns, sql_columns):
        raise _ContractRaised(yaml_columns, sql_columns)

    def get_column_schema_from_query(sql, header=None):
        return {"MODEL": sql_columns, "YML": yml_columns}[sql]

    env = jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
    env.globals.update(
        model={"columns": {c.name: {} for c in yml_columns}},
        config=SimpleNamespace(get=lambda key, default=None: default),
        exceptions=SimpleNamespace(raise_contract_error=raise_contract_error),
        get_column_schema_from_query=get_column_schema_from_query,
        get_empty_schema_sql=lambda columns: "YML",
        format_columns=_format_columns,
        adapter=SimpleNamespace(fit_contract_columns=fit),
    )
    module = env.from_string(MACRO_FILE.read_text()).make_module(vars=env.globals)
    return module.confluent__get_assert_columns_equivalent("MODEL")


def _mismatches(error):
    table = ContractError(error.yaml_columns, error.sql_columns).get_mismatches()
    return [tuple(row) for row in table.rows]


def test_M1_literals_pass():
    """U4-M1: literals pass."""
    out = _assert(
        [col("s", "CHAR", 3), col("b", "BINARY", 2), col("n", "INTEGER")],
        [col("n", "INTEGER"), col("s", "VARCHAR", MAX), col("b", "VARBINARY", MAX)],
    )
    assert out.strip() == ""


def test_M2_literal_short_varchar_still_fails_with_todays_message():
    """U4-M2: short varchar fails with ContractError mismatch exactly [("s", "CHAR", "VARCHAR",
    "data type mismatch")]."""
    with pytest.raises(_ContractRaised) as raised:
        _assert([col("s", "CHAR", 3)], [col("s", "VARCHAR", 10)])
    assert _mismatches(raised.value) == [("s", "CHAR", "VARCHAR", "data type mismatch")]


def test_M3_fitted_column_not_listed_when_another_column_fails():
    """U4-M3: a fitted column plus a real mismatch, only the real one listed."""
    with pytest.raises(_ContractRaised) as raised:
        _assert(
            [col("s", "CHAR", 3), col("n", "INTEGER")],
            [col("s", "VARCHAR", MAX), col("n", "BIGINT")],
        )
    assert _mismatches(raised.value) == [("n", "INTEGER", "BIGINT", "data type mismatch")]


def test_M4_no_yml_columns():
    """U4-M4: no yml columns."""
    with pytest.raises(_ContractRaised) as raised:
        _assert([col("s", "CHAR", 3)], [])
    assert (raised.value.yaml_columns, raised.value.sql_columns) == ([], [])


def test_M5_count_mismatch():
    """U4-M5: count mismatch."""
    with pytest.raises(_ContractRaised) as raised:
        _assert([col("s", "CHAR", 3), col("x", "INTEGER")], [col("s", "VARCHAR", MAX)])
    assert _mismatches(raised.value) == [("x", "INTEGER", "", "missing in contract")]


def test_M6_reverse_still_fails():
    """U4-M6: reverse direction fails."""
    with pytest.raises(_ContractRaised) as raised:
        _assert([col("s", "VARCHAR", MAX)], [col("s", "CHAR", 3)])
    assert _mismatches(raised.value) == [("s", "VARCHAR", "CHAR", "data type mismatch")]
