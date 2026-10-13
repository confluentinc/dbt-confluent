"""Compare and display the column types a Flink `sql.dry-run` reports.

The schema drift check dry-runs both the model's SELECT and `SELECT * FROM` the existing table,
and compares the two result schemas as confluent-sql `ColumnTypeDefinition`s. A stored table's
dry-run schema matches the SELECT it was created from, except for two differences:

- Top-level nullability, which `comparable_type` drops from both sides; its docstring says why.
  Nullability inside an ARRAY, MULTISET, MAP or ROW is kept.
- String MAP keys and MULTISET elements, which `stored_type` applies to the model's side. An
  Avro or JSON table, the default, stores a CHAR or VARCHAR key or element, at any depth, as
  VARCHAR(2147483647) NOT NULL, regardless of its length/nullability in the query. Any other key
  or element keeps its type and its own nullability. A Protobuf table keeps the declared type
  instead, so a string key or element drifts there, as it did with the temp table.

`display_type` spells a type for drift messages the way FULL_DATA_TYPE does, so messages read
the same on the dry-run and temp-table paths.
"""

from dataclasses import replace

from confluent_sql.types import ColumnTypeDefinition

# VARCHAR(2147483647) is STRING.
_MAX_LENGTH = 2147483647

# MAP key and MULTISET element types an Avro or JSON table stores as VARCHAR(2147483647) NOT NULL.
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


def stored_type(column_type: ColumnTypeDefinition) -> ColumnTypeDefinition:
    """Return a copy of a model's column type as an Avro or JSON table stores it: every string
    MAP key and MULTISET element, at any depth, as VARCHAR(2147483647) NOT NULL. The input
    isn't modified."""
    return _with_stored_map_keys(column_type)


def _with_stored_map_keys(column_type: ColumnTypeDefinition) -> ColumnTypeDefinition:
    key_type = column_type.key_type
    if key_type is not None:
        if key_type.type in _STRING_KEY_TYPES:
            key_type = _stored_string()
        else:
            key_type = _with_stored_map_keys(key_type)
    element_type = column_type.element_type
    if column_type.type == "MULTISET" and element_type is not None:
        if element_type.type in _STRING_KEY_TYPES:
            element_type = _stored_string()
        else:
            element_type = _with_stored_map_keys(element_type)
    else:
        element_type = _optional(element_type)
    fields = column_type.fields
    if fields is not None:
        fields = [
            replace(field, field_type=_with_stored_map_keys(field.field_type)) for field in fields
        ]
    return replace(
        column_type,
        key_type=key_type,
        value_type=_optional(column_type.value_type),
        element_type=element_type,
        fields=fields,
    )


def _stored_string() -> ColumnTypeDefinition:
    return ColumnTypeDefinition(type="VARCHAR", nullable=False, length=_MAX_LENGTH)


def _optional(column_type: ColumnTypeDefinition | None) -> ColumnTypeDefinition | None:
    return None if column_type is None else _with_stored_map_keys(column_type)


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
