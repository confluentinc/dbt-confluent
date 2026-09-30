"""Render confluent-sql result-schema column types as INFORMATION_SCHEMA FULL_DATA_TYPE strings.

The drift check compares an existing table's `INFORMATION_SCHEMA.COLUMNS.FULL_DATA_TYPE` values
against the columns the model would produce. A `sql.dry-run` of the model's SELECT reports
those as structured `ColumnTypeDefinition`s, which spell some types differently (`INTEGER` vs
`INT`, `TIMESTAMP_WITHOUT_TIME_ZONE` vs `TIMESTAMP(p)`) and carry top-level nullability that
FULL_DATA_TYPE leaves out. `render_full_data_type` bridges the two.

The renderer is an allow-list of what the GH-118 probes (issue #118) observed, down to
parameters and nesting. Unless noted, each shape was seen in a table created with
`CREATE TABLE ... AS SELECT` from the same query, the path the drift check replaces (runs
995e2382, 0484bde2, 6b109689 and 11017c00):

- INT, BIGINT, TINYINT, SMALLINT, BOOLEAN, DOUBLE, DATE, and FLOAT (declared table only).
- CHAR(n), VARCHAR(n), BINARY(n) and VARBINARY(n) with n >= 1, top-level and nested: the table
  keeps the length. CHAR(0), the type of the literal '', can't be stored.
- DECIMAL(p, s); TIME(p) for p in 0-3 (Flink caps TIME at 3); TIMESTAMP(p) and
  TIMESTAMP(p) WITH LOCAL TIME ZONE for p in 0-6 (Avro can't store 7-9).
- ARRAY and MULTISET, with the element's NOT NULL kept.
- MAP. A CHAR or VARCHAR key is always stored as VARCHAR(2147483647) NOT NULL, whatever its
  length or nullability. Any other key keeps its type and its own nullability (INT, BIGINT,
  DATE, DECIMAL and VARBINARY keys observed).
- ROW with named fields, at any depth.

Anything else raises `UnverifiedTypeError`: other type names (INTERVAL and VARIANT, which a
table can't store, and anything unknown), other parameters (TIMESTAMP(9), CHAR(0), a missing
length or precision), and ROWs with no fields, backticks in field names, or field
descriptions. The caller then falls back to the temp-table drift check rather than risk
reporting false drift. To widen the allow-list, verify the shape against a real CTAS table
first.
"""

from confluent_sql.types import ColumnTypeDefinition

# VARCHAR(2147483647) is STRING and VARBINARY(2147483647) is BYTES.
_MAX_LENGTH = 2147483647

# Spelled identically in the dry-run schema and in FULL_DATA_TYPE.
_SAME_NAME = frozenset({"BIGINT", "BOOLEAN", "DATE", "DOUBLE", "FLOAT", "SMALLINT", "TINYINT"})

# Rendered as NAME(length), at any depth.
_LENGTH_TYPES = frozenset({"BINARY", "CHAR", "VARBINARY", "VARCHAR"})

# MAP key types a table stores as VARCHAR(2147483647) NOT NULL.
_STRING_KEY_TYPES = frozenset({"CHAR", "VARCHAR"})

# Dry-run name -> (FULL_DATA_TYPE name, suffix after the precision, verified precisions).
_TEMPORAL_TYPES: dict[str, tuple[str, str, frozenset[int]]] = {
    "TIME_WITHOUT_TIME_ZONE": ("TIME", "", frozenset(range(4))),
    "TIMESTAMP_WITHOUT_TIME_ZONE": ("TIMESTAMP", "", frozenset(range(7))),
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": ("TIMESTAMP", " WITH LOCAL TIME ZONE", frozenset(range(7))),
}


class UnverifiedTypeError(Exception):
    """A dry-run column type with no verified FULL_DATA_TYPE rendering."""


def render_full_data_type(column_type: ColumnTypeDefinition) -> str:
    """Render a top-level column type the way INFORMATION_SCHEMA FULL_DATA_TYPE spells it.

    Raises:
        UnverifiedTypeError: if the type, or any type nested in it, has no verified rendering.
    """
    return _render(column_type, nested=False)


def _render(column_type: ColumnTypeDefinition, *, nested: bool) -> str:
    name = column_type.type
    if name == "INTEGER":
        text = "INT"
    elif name in _SAME_NAME:
        text = name
    elif name in _LENGTH_TYPES:
        length = _required(column_type.length, f"{name} length")
        if length < 1:
            raise UnverifiedTypeError(f"{name}({length})")
        text = f"{name}({length})"
    elif name == "DECIMAL":
        precision = _required(column_type.precision, "DECIMAL precision")
        scale = _required(column_type.scale, "DECIMAL scale")
        text = f"DECIMAL({precision}, {scale})"
    elif name in _TEMPORAL_TYPES:
        base, suffix, verified_precisions = _TEMPORAL_TYPES[name]
        if column_type.precision not in verified_precisions:
            raise UnverifiedTypeError(f"{name} with precision {column_type.precision}")
        text = f"{base}({column_type.precision}){suffix}"
    elif name in ("ARRAY", "MULTISET"):
        element = _render(_child(column_type.element_type, f"{name} element"), nested=True)
        text = f"{name}<{element}>"
    elif name == "MAP":
        text = _render_map(column_type)
    elif name == "ROW":
        text = _render_row(column_type)
    else:
        raise UnverifiedTypeError(name)
    # FULL_DATA_TYPE carries NOT NULL only inside composite types; top-level nullability lives
    # in the separate IS_NULLABLE column.
    if nested and not column_type.nullable:
        text += " NOT NULL"
    return text


def _render_map(column_type: ColumnTypeDefinition) -> str:
    key_type = _child(column_type.key_type, "MAP key")
    if key_type.type in _STRING_KEY_TYPES:
        # The table widens a CHAR or VARCHAR key to STRING and makes it NOT NULL, whatever the
        # query says (Avro map keys are non-null strings).
        key = f"VARCHAR({_MAX_LENGTH}) NOT NULL"
    else:
        key = _render(key_type, nested=True)
    value = _render(_child(column_type.value_type, "MAP value"), nested=True)
    return f"MAP<{key}, {value}>"


def _render_row(column_type: ColumnTypeDefinition) -> str:
    if not column_type.fields:
        raise UnverifiedTypeError("ROW with no fields")
    rendered = []
    for field in column_type.fields:
        if "`" in field.name:
            raise UnverifiedTypeError(f"ROW field name {field.name!r} contains a backtick")
        if field.description:
            raise UnverifiedTypeError(f"ROW field {field.name!r} has a description")
        rendered.append(f"`{field.name}` {_render(field.field_type, nested=True)}")
    return f"ROW<{', '.join(rendered)}>"


def _child(child_type: ColumnTypeDefinition | None, what: str) -> ColumnTypeDefinition:
    if child_type is None:
        raise UnverifiedTypeError(f"{what} type missing")
    return child_type


def _required(value: int | None, what: str) -> int:
    if value is None:
        raise UnverifiedTypeError(f"{what} missing")
    return value
