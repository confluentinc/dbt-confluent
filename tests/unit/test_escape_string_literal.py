"""Unit tests for `ConfluentAdapter.escape_string_literal`.

The streaming materializations embed user config (`with` option keys/values,
`connector`) inside single-quoted string literals in generated DDL. Flink SQL
escapes a quote inside a literal by doubling it; anything less lets a value
like a faker expression containing `'` break the statement — or terminate the
literal and inject clauses into the DDL.
"""

import pytest

from dbt.adapters.confluent.impl import ConfluentAdapter


@pytest.fixture
def adapter():
    return ConfluentAdapter.__new__(ConfluentAdapter)


def test_plain_string_unchanged(adapter):
    assert adapter.escape_string_literal("append") == "append"


def test_single_quote_doubled(adapter):
    assert adapter.escape_string_literal("it's") == "it''s"


def test_multiple_quotes_all_doubled(adapter):
    assert adapter.escape_string_literal("a'b'c") == "a''b''c"


def test_already_doubled_quotes_doubled_again(adapter):
    # A pre-doubled value must not be "recognized" as escaped: config holds
    # raw values, so each raw quote is doubled independently.
    assert adapter.escape_string_literal("''") == "''''"


def test_injection_attempt_stays_inside_literal(adapter):
    value = "x') WITH ('connector' = 'evil"
    assert adapter.escape_string_literal(value) == "x'') WITH (''connector'' = ''evil"


def test_non_string_values_are_stringified(adapter):
    assert adapter.escape_string_literal(10) == "10"
    assert adapter.escape_string_literal(1.5) == "1.5"
