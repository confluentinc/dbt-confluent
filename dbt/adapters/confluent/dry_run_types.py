"""Compare and display the column types a Flink `sql.dry-run` reports.

The schema drift check dry-runs both the model's SELECT and `SELECT * FROM` the existing table,
and compares the two result schemas as confluent-sql `ColumnTypeDefinition`s. A stored table's
dry-run schema matches the SELECT it was created from, except for two differences:

- Top-level nullability, which `comparable_type` drops from both sides; its docstring says why.
  Nullability inside an ARRAY, MULTISET, MAP or ROW is kept.
- String MAP keys and MULTISET elements. An Avro or JSON table, the default, stores a CHAR or
  VARCHAR key or element, at any depth, as VARCHAR(2147483647) NOT NULL, regardless of its
  length/nullability in the query. A Protobuf table keeps the declared type. So `types_match`
  lets a model's key or element pass if it fits in the table's, whatever the table's format.
  Any other key or element must match exactly, nullability included.

`types_match` is what the drift check calls for each column; it allows for both differences.

`display_type` spells a type for drift messages the way FULL_DATA_TYPE does, so messages read
the same on the dry-run and temp-table paths.
"""

from dataclasses import replace

from confluent_sql.types import ColumnTypeDefinition, RowColumn

# MAP key and MULTISET element types a table may store wider than the query declares them.
_STRING_KEY_TYPES = frozenset({"CHAR", "VARCHAR"})

# Dry-run type name -> (FULL_DATA_TYPE name, suffix after the parameters). Any other name is
# spelled the same way in both.
_DISPLAY_NAMES: dict[str, tuple[str, str]] = {
    "INTEGER": ("INT", ""),
    "TIME_WITHOUT_TIME_ZONE": ("TIME", ""),
    "TIMESTAMP_WITHOUT_TIME_ZONE": ("TIMESTAMP", ""),
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": ("TIMESTAMP", " WITH LOCAL TIME ZONE"),
    "TIMESTAMP_WITH_TIME_ZONE": ("TIMESTAMP", " WITH TIME ZONE"),
}


def comparable_type(column_type: ColumnTypeDefinition) -> ColumnTypeDefinition:
    """Return a copy of a top-level column type to compare for drift, with top-level
    nullability dropped. The input isn't modified.

    Top-level nullability can differ without the model changing, and the temp-table check never
    compared it (FULL_DATA_TYPE leaves it out). A table built from yml columns (a
    `streaming_table`, or a `table` with an enforced contract) takes it from the yml's `not_null`
    and primary key constraints, not from the SELECT. So a nullable source column can feed a
    `not_null` column, and an expression that can't be null, like `CAST(1 AS INT)`, can feed a
    nullable one. Comparing it would report drift on every run, and `--full-refresh` wouldn't clear
    it, since it rebuilds the table from the same yml.
    """
    return replace(column_type, nullable=True)


def types_match(existing_type: ColumnTypeDefinition, expected_type: ColumnTypeDefinition) -> bool:
    """Return whether expected_type, the model's column type from its SELECT dry-run, matches
    existing_type, the existing table's type for the same column, for the drift check.

    They match when they're equal apart from top-level nullability (see `comparable_type`) and
    any string MAP key or MULTISET element, at any depth, that fits in the table's. A key or
    element fits when it's a CHAR(n) or VARCHAR(n) and the table's is a VARCHAR(m) with n <= m,
    or the same CHAR(n); nullability is ignored. Every one fits in the VARCHAR(2147483647) NOT
    NULL an Avro or JSON table stores. On a Protobuf table, which keeps the declared type, a
    longer one or a VARCHAR into a CHAR doesn't match. A shorter one does: the table still holds
    every value the model can produce, and `--full-refresh` rebuilds it with the new type.
    """
    return _match(comparable_type(existing_type), comparable_type(expected_type))


def _match(existing_type: ColumnTypeDefinition, expected_type: ColumnTypeDefinition) -> bool:
    if _without_children(existing_type) != _without_children(expected_type):
        return False
    if expected_type.type == "MULTISET":
        elements_match = _member_match(existing_type.element_type, expected_type.element_type)
    else:
        elements_match = _child_match(existing_type.element_type, expected_type.element_type)
    return (
        elements_match
        and _member_match(existing_type.key_type, expected_type.key_type)
        and _child_match(existing_type.value_type, expected_type.value_type)
        and _fields_match(existing_type.fields, expected_type.fields)
    )


def _without_children(column_type: ColumnTypeDefinition) -> ColumnTypeDefinition:
    # The type with the types inside it left out; _match compares those one by one.
    return replace(column_type, key_type=None, value_type=None, element_type=None, fields=None)


def _child_match(
    existing_type: ColumnTypeDefinition | None, expected_type: ColumnTypeDefinition | None
) -> bool:
    if existing_type is None or expected_type is None:
        return existing_type is expected_type
    return _match(existing_type, expected_type)


def _member_match(
    existing_type: ColumnTypeDefinition | None, expected_type: ColumnTypeDefinition | None
) -> bool:
    # A MAP key or MULTISET element: it matches if the model's fits in the table's, or if the
    # two match like any other type.
    if (
        existing_type is not None
        and expected_type is not None
        and _fits(expected_type, existing_type)
    ):
        return True
    return _child_match(existing_type, expected_type)


def _fields_match(
    existing_fields: list[RowColumn] | None, expected_fields: list[RowColumn] | None
) -> bool:
    if existing_fields is None or expected_fields is None:
        return existing_fields is expected_fields
    return len(existing_fields) == len(expected_fields) and all(
        existing_field.name == expected_field.name
        and existing_field.description == expected_field.description
        and _match(existing_field.field_type, expected_field.field_type)
        for existing_field, expected_field in zip(existing_fields, expected_fields, strict=True)
    )


def _fits(expected_type: ColumnTypeDefinition, existing_type: ColumnTypeDefinition) -> bool:
    if expected_type.type not in _STRING_KEY_TYPES:
        return False
    if expected_type.length is None or existing_type.length is None:
        return False
    if existing_type.type == "VARCHAR":
        return expected_type.length <= existing_type.length
    return existing_type.type == expected_type.type == "CHAR" and (
        expected_type.length == existing_type.length
    )


def display_type(column_type: ColumnTypeDefinition) -> str:
    """Spell a top-level column type for drift messages only, the way FULL_DATA_TYPE does.

    Drift is only decided by comparing `comparable_type` results, never these strings, but two
    types a dry run returns that compare differently always spell differently. A type without a
    FULL_DATA_TYPE spelling here (INTERVAL, RAW, a structured type, anything newer) is spelled from
    its name and every parameter it has, which keeps it distinct but doesn't match FULL_DATA_TYPE.
    """
    return _display(column_type, nested=False)


def _display(column_type: ColumnTypeDefinition, *, nested: bool) -> str:
    if column_type.element_type is not None:
        element = _display(column_type.element_type, nested=True)
        text = f"{column_type.type}{_parameters(column_type)}<{element}>"
    elif column_type.key_type is not None and column_type.value_type is not None:
        key = _display(column_type.key_type, nested=True)
        value = _display(column_type.value_type, nested=True)
        text = f"{column_type.type}{_parameters(column_type)}<{key}, {value}>"
    elif column_type.fields is not None:
        fields = (
            _display_field(field.name, field.field_type, field.description)
            for field in column_type.fields
        )
        text = f"{column_type.type}{_parameters(column_type)}<{', '.join(fields)}>"
    else:
        name, suffix = _DISPLAY_NAMES.get(column_type.type, (column_type.type, ""))
        text = f"{name}{_parameters(column_type)}{suffix}"
    # FULL_DATA_TYPE carries NOT NULL only inside composite types; top-level nullability lives
    # in the separate IS_NULLABLE column.
    if nested and not column_type.nullable:
        text += " NOT NULL"
    return text


def _parameters(column_type: ColumnTypeDefinition) -> str:
    """Spell length, precision and scale the way FULL_DATA_TYPE does, then any interval resolution,
    fractional precision or class name, labeled."""
    parameters = [
        str(value)
        for value in (column_type.length, column_type.precision, column_type.scale)
        if value is not None
    ]
    details = {
        "resolution": column_type.resolution,
        "fractional_precision": column_type.fractional_precision,
        "class_name": column_type.class_name,
    }
    parameters.extend(f"{label}={value}" for label, value in details.items() if value is not None)
    return f"({', '.join(parameters)})" if parameters else ""


def _display_field(name: str, field_type: ColumnTypeDefinition, description: str | None) -> str:
    # FULL_DATA_TYPE doubles a backtick inside a field name, so a name can't close its quotes.
    text = f"`{name.replace('`', '``')}` {_display(field_type, nested=True)}"
    if description is not None:
        text += " '" + description.replace("'", "''") + "'"
    return text
