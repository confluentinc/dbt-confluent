"""Compare and display the column types a Flink `sql.dry-run` reports.

The schema drift check dry-runs both the model's SELECT and `SELECT * FROM` the existing table,
and compares the two result schemas as confluent-sql `ColumnTypeDefinition`s. A stored table's
dry-run schema matches the SELECT it was created from, except for two differences:

- Top-level nullability, which `fitted_type` takes from the table; its docstring says why.
  Nullability inside an ARRAY, MULTISET, MAP or ROW is kept.
- String MAP keys and MULTISET elements. An Avro or JSON table, the default, stores a CHAR or
  VARCHAR key or element, at any depth, as VARCHAR(2147483647) NOT NULL, regardless of its
  length/nullability in the query. A Protobuf table keeps the declared type. So `fitted_type` takes
  the table's key or element wherever the model's fits in it, whatever the table's format. Any
  other key or element must match exactly, nullability included.

`fitted_type` is what the drift check calls for each column; it allows for both differences.

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


def fitted_type(
    existing_type: ColumnTypeDefinition, expected_type: ColumnTypeDefinition
) -> ColumnTypeDefinition:
    """Return expected_type, the model's column type from its SELECT dry-run, with the parts the
    drift check lets differ taken from existing_type, the existing table's type for the same
    column. The column has drifted if the result isn't equal to existing_type, and the drift
    message shows the result as the expected type, so it only shows differences that count. The
    inputs aren't modified.

    Top-level nullability is taken from the table. It can differ without the model changing, and
    the temp-table check never compared it (FULL_DATA_TYPE leaves it out). A table built from yml
    columns (a `streaming_table`, or a `table` with an enforced contract) takes it from the yml's
    `not_null` and primary key constraints, not from the SELECT. So a nullable source column can
    feed a `not_null` column, and an expression that can't be null, like `CAST(1 AS INT)`, can feed
    a nullable one. Comparing it would report drift on every run, and `--full-refresh` wouldn't
    clear it, since it rebuilds the table from the same yml.

    Any string MAP key or MULTISET element, at any depth, that fits in the table's is taken from
    the table too. A key or element fits when it's a CHAR(n) or VARCHAR(n) and the table's is a
    VARCHAR(m) with n <= m, or the same CHAR(n); nullability is ignored. Every one fits in the
    VARCHAR(2147483647) NOT NULL an Avro or JSON table stores. On a Protobuf table, which keeps the
    declared type, a longer one or a VARCHAR into a CHAR is kept as the model declares it, so it
    drifts. A shorter one fits: the table still holds every value the model can produce, and
    `--full-refresh` rebuilds it with the new type.
    """
    return replace(_fit(existing_type, expected_type), nullable=existing_type.nullable)


def _fit(
    existing_type: ColumnTypeDefinition, expected_type: ColumnTypeDefinition
) -> ColumnTypeDefinition:
    if existing_type.type == expected_type.type == "MULTISET":
        element_type = _fit_member(existing_type.element_type, expected_type.element_type)
    else:
        element_type = _fit_child(existing_type.element_type, expected_type.element_type)
    return replace(
        expected_type,
        key_type=_fit_member(existing_type.key_type, expected_type.key_type),
        value_type=_fit_child(existing_type.value_type, expected_type.value_type),
        element_type=element_type,
        fields=_fit_fields(existing_type.fields, expected_type.fields),
    )


def _fit_child(
    existing_type: ColumnTypeDefinition | None, expected_type: ColumnTypeDefinition | None
) -> ColumnTypeDefinition | None:
    if existing_type is None or expected_type is None:
        return expected_type
    return _fit(existing_type, expected_type)


def _fit_member(
    existing_type: ColumnTypeDefinition | None, expected_type: ColumnTypeDefinition | None
) -> ColumnTypeDefinition | None:
    # A MAP key or MULTISET element: the table's if the model's fits in it, or else fitted like any
    # other type.
    if (
        existing_type is not None
        and expected_type is not None
        and _fits(expected_type, existing_type)
    ):
        return existing_type
    return _fit_child(existing_type, expected_type)


def _fit_fields(
    existing_fields: list[RowColumn] | None, expected_fields: list[RowColumn] | None
) -> list[RowColumn] | None:
    # Fields are fitted by position. A different number of fields is drift whatever their types, so
    # they're kept as the model declares them.
    if existing_fields is None or expected_fields is None:
        return expected_fields
    if len(existing_fields) != len(expected_fields):
        return expected_fields
    return [
        replace(
            expected_field, field_type=_fit(existing_field.field_type, expected_field.field_type)
        )
        for existing_field, expected_field in zip(existing_fields, expected_fields, strict=True)
    ]


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

    Drift is only decided by comparing a `fitted_type` result with the table's type, never these
    strings, but the two always spell differently when they differ, since the result has the
    table's top-level nullability. A type without a FULL_DATA_TYPE spelling here (INTERVAL, RAW, a
    structured type, anything newer) is spelled from its name and every parameter it has, which
    keeps it distinct but doesn't match FULL_DATA_TYPE.
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
