"""Render confluent-sql result-schema column types as INFORMATION_SCHEMA FULL_DATA_TYPE strings.

The drift check compares an existing table's `INFORMATION_SCHEMA.COLUMNS.FULL_DATA_TYPE` values
against the columns the model would produce. A `sql.dry-run` of the model's SELECT reports
those as structured `ColumnTypeDefinition`s, which spell some types differently (`INTEGER` vs
`INT`, `TIMESTAMP_WITHOUT_TIME_ZONE` vs `TIMESTAMP(p)`) and carry top-level nullability that
FULL_DATA_TYPE leaves out. `render_full_data_type` bridges the two.

The renderer is an allow-list of what the GH-118 probes (issue #118) observed, down to
parameters and nesting:

- CTAS evidence (run 995e2382): a table created with `CREATE TABLE ... AS SELECT` from the same
  query reported the rendered string. This is the path the drift check replaces. Observed:
  INT, BIGINT, BOOLEAN, DOUBLE, DATE, VARCHAR(n), top-level CHAR(n), DECIMAL(p, s),
  TIMESTAMP(3), TIMESTAMP(6), TIMESTAMP(3) WITH LOCAL TIME ZONE, ARRAY<INT NOT NULL>, and a
  ROW of nullable fields.
- Declared evidence (run 09dcd5b0): a `CAST(NULL AS <type>)` dry-run reported the input shape,
  and a table declared with that type reported the rendered string. No CTAS was observed, and
  a CTAS can coerce types on the way into the table (it stored a CHAR(1) MAP key as
  VARCHAR(2147483647)). Observed: FLOAT, TIME(0), VARBINARY(2147483647), CHAR(5), ARRAY<INT>,
  ARRAY<VARCHAR(2147483647) NOT NULL>, and MAP<VARCHAR(2147483647) NOT NULL, INT>.

Anything else raises `UnverifiedTypeError`: other type names (TINYINT, SMALLINT, BINARY,
MULTISET, INTERVAL, VARIANT, ...), other parameters (TIME(3), TIMESTAMP(9), VARBINARY(16), a
missing length or precision), CHAR/BINARY/VARBINARY(n) inside a composite type, MAP keys other
than VARCHAR(2147483647), and ROWs with no fields, backticks in field names, or field
descriptions. The caller then falls back to the temp-table drift check rather than risk
reporting false drift. To widen the allow-list, verify the shape against a real CTAS table
first.
"""

from confluent_sql.types import ColumnTypeDefinition

# VARCHAR(2147483647) is STRING and VARBINARY(2147483647) is BYTES.
_MAX_LENGTH = 2147483647

# Spelled identically in the dry-run schema and in FULL_DATA_TYPE.
_SAME_NAME = frozenset({"BIGINT", "BOOLEAN", "DATE", "DOUBLE", "FLOAT"})

# Dry-run name -> (FULL_DATA_TYPE name, suffix after the precision, verified precisions).
_TEMPORAL_TYPES: dict[str, tuple[str, str, frozenset[int]]] = {
    "TIME_WITHOUT_TIME_ZONE": ("TIME", "", frozenset({0})),
    "TIMESTAMP_WITHOUT_TIME_ZONE": ("TIMESTAMP", "", frozenset({3, 6})),
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": ("TIMESTAMP", " WITH LOCAL TIME ZONE", frozenset({3})),
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
    elif name == "VARCHAR":
        text = f"VARCHAR({_required(column_type.length, 'VARCHAR length')})"
    elif name == "CHAR":
        text = _render_char(column_type, nested=nested)
    elif name == "VARBINARY":
        if column_type.length != _MAX_LENGTH:
            raise UnverifiedTypeError(f"VARBINARY({column_type.length})")
        text = f"VARBINARY({_MAX_LENGTH})"
    elif name == "DECIMAL":
        precision = _required(column_type.precision, "DECIMAL precision")
        scale = _required(column_type.scale, "DECIMAL scale")
        text = f"DECIMAL({precision}, {scale})"
    elif name in _TEMPORAL_TYPES:
        base, suffix, verified_precisions = _TEMPORAL_TYPES[name]
        if column_type.precision not in verified_precisions:
            raise UnverifiedTypeError(f"{name} with precision {column_type.precision}")
        text = f"{base}({column_type.precision}){suffix}"
    elif name == "ARRAY":
        element = _render(_child(column_type.element_type, "ARRAY element"), nested=True)
        text = f"ARRAY<{element}>"
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


def _render_char(column_type: ColumnTypeDefinition, *, nested: bool) -> str:
    # Only top-level CHAR was observed in a table, and a CTAS widened a CHAR MAP key to
    # VARCHAR, so a nested CHAR falls back. CHAR(0) is the type of the literal ''.
    if nested:
        raise UnverifiedTypeError("CHAR inside a composite type")
    length = _required(column_type.length, "CHAR length")
    if length < 1:
        raise UnverifiedTypeError(f"CHAR({length})")
    return f"CHAR({length})"


def _render_map(column_type: ColumnTypeDefinition) -> str:
    key_type = _child(column_type.key_type, "MAP key")
    # Only STRING keys were observed in a table, and tables always store MAP keys as NOT NULL,
    # whatever the query says.
    if key_type.type != "VARCHAR" or key_type.length != _MAX_LENGTH:
        raise UnverifiedTypeError(
            f"MAP key {key_type.type} (only VARCHAR({_MAX_LENGTH}) keys are verified)"
        )
    value = _render(_child(column_type.value_type, "MAP value"), nested=True)
    return f"MAP<VARCHAR({_MAX_LENGTH}) NOT NULL, {value}>"


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
