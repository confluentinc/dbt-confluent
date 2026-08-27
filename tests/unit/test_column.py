"""Regression tests for #92: ConfluentColumn inherited dbt-core's default
TYPE_LABELS = {"STRING": "TEXT"}, so translate_type("string") produced TEXT --
a type Flink SQL doesn't understand -- for any contract-enforced model with a
string column (dbt-core's ctx_model rewrites data_type through translate_type
before the schema-probe query and the rendered column DDL; see providers.py).
"""

from dbt.adapters.confluent.column import ConfluentColumn


def test_translate_type_does_not_apply_dbt_core_default_string_to_text_mapping():
    # Both casings: dtype.upper() is what actually gets looked up in
    # TYPE_LABELS, so "string" and "STRING" are the same lookup and must
    # behave identically -- neither should come back as TEXT.
    assert ConfluentColumn.translate_type("string") == "string"
    assert ConfluentColumn.translate_type("STRING") == "STRING"

    # Control: bigint was never mapped by dbt-core's default TYPE_LABELS
    # either, so it already passed through unchanged before this fix. Same
    # result here confirms the fix didn't change translate_type's behavior
    # in general -- just removed the one bad STRING entry.
    assert ConfluentColumn.translate_type("bigint") == "bigint"
