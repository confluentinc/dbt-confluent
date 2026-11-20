"""Validation and DDL rendering for the `watermark` model config."""

from typing import Any

from dbt_common.exceptions import CompilationError

# Materializations whose CREATE statement is `... AS SELECT`, so Flink infers the table's columns
# from the query. They don't need a contract to declare columns: a column block holding only a
# watermark is enough.
COLUMN_INFERRING_MATERIALIZATIONS = frozenset({"table", "materialized_table"})

_ALLOWED_KEYS = frozenset({"column", "expression"})


def validate_watermark_config(watermark: object, model_config: Any) -> None:
    """Raise CompilationError if the `watermark` config is malformed or can't be rendered.

    Allowed shape: a dict with a non-empty string `column` (no backticks) and a non-empty
    string `expression`, and no other keys. The watermark is rendered into the table's
    column block, so a `streaming_table` (whose CREATE TABLE has no AS SELECT to infer the
    other columns from) must have an enforced contract. A `table` or `materialized_table`
    needn't: their other columns are inferred from the query. `model_config` is the Jinja config
    object (anything with `.get`).
    """
    if watermark is None:
        return
    shape_error = (
        "'watermark' config must be a mapping with non-empty string 'column' and 'expression' keys"
    )
    if not isinstance(watermark, dict):
        raise CompilationError(shape_error)
    for key in ("column", "expression"):
        value = watermark.get(key)
        if not isinstance(value, str) or not value.strip():
            raise CompilationError(shape_error)
    if "`" in watermark["column"]:
        raise CompilationError(
            f"'watermark.column' must not contain backtick characters; got: {watermark['column']}"
        )
    unknown = set(watermark) - _ALLOWED_KEYS
    if unknown:
        raise CompilationError(
            f"'watermark' has unknown key '{sorted(unknown)[0]}'. "
            "Allowed keys: 'column', 'expression'"
        )
    contract = model_config.get("contract")
    if model_config.get("materialized") not in COLUMN_INFERRING_MATERIALIZATIONS and not (
        contract and contract.enforced
    ):
        raise CompilationError(
            "'watermark' requires an enforced contract (config(contract={'enforced': true})) "
            "on this materialization, because the watermark is declared inside the table's "
            "column definitions, which the contract provides."
        )


def append_watermark(columns_ddl: str, watermark: dict[str, str]) -> str:
    """Add `WATERMARK FOR <column> AS <expression>` as the last entry of a parenthesized
    column/constraint block, such as dbt's `get_table_columns_and_constraints()`. An empty
    `columns_ddl` (no contract) yields a block holding only the watermark."""
    entry = f"WATERMARK FOR `{watermark['column']}` AS {watermark['expression']}"
    body = columns_ddl.rstrip()
    if not body:
        return f"(\n  {entry}\n)"
    if not body.endswith(")"):
        raise CompilationError(
            "Cannot add a watermark: the contract's column definitions are not a "
            f"parenthesized block: {columns_ddl!r}"
        )
    return f"{body[:-1].rstrip()},\n  {entry}\n)"
