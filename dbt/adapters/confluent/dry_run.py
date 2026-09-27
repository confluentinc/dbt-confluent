import uuid
from dataclasses import dataclass, replace
from typing import NoReturn, cast

from confluent_sql import HIDDEN_LABEL
from confluent_sql.execution_mode import ExecutionMode
from confluent_sql.statement import Column, Statement
from confluent_sql.types import ColumnTypeDefinition
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.events.logging import AdapterLogger

from .column import ConfluentColumn

logger = AdapterLogger("Confluent")

# Types with no Flink DDL spelling at all - confirmed live against a
# Confluent Cloud Flink SQL Gateway, so get_ddl_type raises for these no
# matter what it's being used for (there is nothing to reconstruct).
#
# RAW: "The use of RAW types is not supported. Use standard SQL types ...
# Or use BYTES ...".
#
# TIMESTAMP_WITH_TIME_ZONE: Confluent's parser rejects the bare
# `TIMESTAMP ... WITH TIME ZONE` form outright ("Was expecting LOCAL ..."),
# so only TIMESTAMP_LTZ (WITH LOCAL TIME ZONE) is ever constructable. This
# type should therefore never actually appear in a real dry run response,
# but it's rejected here rather than silently mishandled if it somehow does.
UNREPRESENTABLE_TYPES = frozenset({"RAW", "TIMESTAMP_WITH_TIME_ZONE"})

# Types get_ddl_type can reconstruct fine (a table column can genuinely hold
# one, or a query can project one), but that try_get_castable_type still
# rejects: no YAML-spellable unit test fixture value dbt-core's fixture
# rendering (format_row) can ever produce actually CASTs into one, confirmed
# live.
#
# ARRAY/MULTISET/MAP/ROW: format_row splices a YAML list/dict fixture value
# in via bare `str()` (Jinja has no other rendering for a non-string,
# non-None value), giving e.g. `CAST([1, 2, 3] AS ARRAY<INT>)` - and Flink's
# CAST doesn't accept Python-literal bracket/brace syntax at all
# ("Encountered '[' ..."/"Encountered '{' ...").
#
# INTERVAL_YEAR_MONTH/INTERVAL_DAY_TIME: constructable as a query projection
# (though not as a table column - Confluent's CREATE TABLE grammar rejects
# INTERVAL outright), but Flink requires its own interval literal syntax
# (`INTERVAL '1-2' YEAR TO MONTH`) - a plain string CAST fails ("Unsupported
# cast from 'CHAR(n)' to 'INTERVAL ...'"), and a plain string/number/bool is
# the only literal form a YAML fixture value ever renders as.
NOT_CASTABLE_TYPES = frozenset(
    {"ARRAY", "MULTISET", "MAP", "ROW", "INTERVAL_YEAR_MONTH", "INTERVAL_DAY_TIME"}
)

# ColumnTypeDefinition.type reports these two by Flink's internal/full type
# name (confirmed against a live dry run), but CAST(x AS ...) only accepts
# the short keyword form - "Unknown identifier 'TIMESTAMP_WITHOUT_TIME_ZONE'"
# otherwise. Every other type's dry-run name already matches its CAST keyword
# (including INTEGER, which - unlike these two - Flink accepts as-is).
#
# TIME_WITHOUT_TIME_ZONE has no such alias: unlike TIMESTAMP, Flink's grammar
# defines no "TIME WITHOUT TIME ZONE" spelling at all (only bare TIME/TIME(p)),
# so it's mapped straight down to the short form instead of space-joined.
CAST_TYPE_ALIASES = {
    "TIME_WITHOUT_TIME_ZONE": "TIME",
    "TIMESTAMP_WITHOUT_TIME_ZONE": "TIMESTAMP",
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE": "TIMESTAMP_LTZ",
}


def _raise_unsupported(kind: str) -> NoReturn:
    raise DbtDatabaseError(
        f"Cannot determine a unit test's expected-row types from a dry run for a "
        f"'{kind}' column - this type isn't supported without an enforced contract. "
        "Either declare `contract: {enforced: true}` with explicit column data_types "
        "on the tested model, or `dbt run` it first."
    )


def get_ddl_type(type_def: ColumnTypeDefinition) -> str:
    """A Flink DDL type string for a dry run's ColumnTypeDefinition
    (confluent_sql.types) - suitable for a CREATE TABLE column, or any other
    context that just needs a faithful spelling of the type itself (e.g.
    recreating an equivalent table, or comparing two dry runs structurally).

    Always succeeds except for UNREPRESENTABLE_TYPES, which have no Flink
    DDL spelling at all. In particular, this succeeds for every type
    try_get_castable_type rejects (NOT_CASTABLE_TYPES) - those types can be
    spelled in DDL just fine, they just can't accept a CAST from a YAML
    fixture value. Use try_get_castable_type instead of this when that's
    what you need.

    Passes the driver's own type name straight through - see the
    thin-passthrough precedent in ConfluentColumn.TYPE_LABELS - appending
    whichever of length/precision/scale the type itself actually carries.
    Flink only ever populates these fields when they're meaningful for the
    reported type (CHAR/VARCHAR/BINARY/VARBINARY -> length; TIME/TIMESTAMP/
    TIMESTAMP_LTZ/DECIMAL -> precision; DECIMAL -> scale too), so their
    presence alone decides the form - no separate per-type allowlist to keep
    in sync with which types take which parameter.

    ARRAY/MULTISET/MAP/ROW recurse into their element/key/value/field types
    through this same function, so an arbitrarily nested constructed type
    round-trips correctly (verified live: ARRAY<ROW<...>>, ROW<a ARRAY<...>>,
    etc) - including each level's own nullability (`ARRAY<INT NOT NULL>`
    is a different type from `ARRAY<INT>` in Flink).

    INTERVAL_YEAR_MONTH/INTERVAL_DAY_TIME are mapped to a fixed keyword form
    rather than reconstructed from `.resolution`/`.fractional_precision`:
    confirmed live, Confluent's Flink SQL Gateway normalizes every
    year-month interval form (YEAR, YEAR(p), YEAR TO MONTH, MONTH) to the
    same reported shape (precision=2, resolution='MONTH'), and every
    day-time interval form (DAY, DAY TO SECOND, DAY TO SECOND(p2), SECOND,
    SECOND(p2), ...) to the same reported shape (precision=2,
    fractional_precision=3, resolution='SECOND') regardless of what was
    requested - so there is currently only one real form of each to
    reconstruct, and general resolution-keyword parsing would be unreachable
    dead code.

    Appends `NOT NULL` when `type_def.nullable` is False, so the DDL this
    returns actually matches the schema it was reconstructed from - a NOT
    NULL/PRIMARY KEY source column reconstructed without it would silently
    produce a *different*, more permissive table. try_get_castable_type
    deliberately does NOT want this (a CAST target can't carry a
    constraint), so it strips nullability before delegating here instead of
    passing type_def straight through.
    """
    kind = CAST_TYPE_ALIASES.get(type_def.type, type_def.type)
    if kind in UNREPRESENTABLE_TYPES:
        _raise_unsupported(kind)
    if kind in ("ARRAY", "MULTISET"):
        base = f"{kind}<{get_ddl_type(type_def.element_type)}>"
    elif kind == "MAP":
        base = f"MAP<{get_ddl_type(type_def.key_type)}, {get_ddl_type(type_def.value_type)}>"
    elif kind == "ROW":
        fields = ", ".join(f"{field.name} {get_ddl_type(field.type)}" for field in type_def.fields)
        base = f"ROW<{fields}>"
    elif kind == "INTERVAL_YEAR_MONTH":
        base = "INTERVAL YEAR TO MONTH"
    elif kind == "INTERVAL_DAY_TIME":
        base = "INTERVAL DAY TO SECOND"
    elif type_def.precision is not None and type_def.scale is not None:
        base = f"{kind}({type_def.precision}, {type_def.scale})"
    elif type_def.length is not None:
        base = f"{kind}({type_def.length})"
    elif type_def.precision is not None:
        base = f"{kind}({type_def.precision})"
    else:
        base = kind
    return base if type_def.nullable else f"{base} NOT NULL"


def is_not_castable(type_def: ColumnTypeDefinition) -> bool:
    """Whether `type_def`'s kind (after CAST_TYPE_ALIASES normalization) is
    one try_get_castable_type would reject - see NOT_CASTABLE_TYPES.

    Split out from try_get_castable_type so a caller that just needs to know
    *whether* a column is a problem (e.g. to explain an unrelated failure
    that's plausibly caused by one) doesn't have to catch-and-discard the
    DbtDatabaseError try_get_castable_type raises for one.
    """
    return CAST_TYPE_ALIASES.get(type_def.type, type_def.type) in NOT_CASTABLE_TYPES


def try_get_castable_type(type_def: ColumnTypeDefinition) -> str:
    """A Flink DDL type string, suitable for `CAST(<a unit test fixture
    value> AS ...)`, for a dry run's ColumnTypeDefinition.

    Unlike get_ddl_type, this additionally raises for NOT_CASTABLE_TYPES -
    types whose DDL form get_ddl_type happily reconstructs, but that no
    YAML-spellable fixture value can actually be CAST into (see its comment
    for the live-confirmed reason per type). This is the function
    get_tested_model_columns (impl.py) needs: it's not enough for the type
    name to be valid DDL, the actual CAST dbt-core's unit test materialization
    builds from a fixture value has to succeed too.

    Always ignores type_def.nullable, unlike get_ddl_type: a CAST target
    can't carry a NOT NULL constraint (that's a column-declaration concept,
    not a type-expression one), so a NOT NULL/PRIMARY KEY source column -
    entirely ordinary in a real model - must not turn into
    `CAST(x AS BIGINT NOT NULL)`, which Flink would reject.
    """
    if is_not_castable(type_def):
        _raise_unsupported(CAST_TYPE_ALIASES.get(type_def.type, type_def.type))
    if type_def.nullable:
        return get_ddl_type(type_def)
    return get_ddl_type(replace(type_def, nullable=True))


def _dry_run_statement(connection: Connection, sql: str) -> Statement:
    """Submit `sql` as a Flink dry run (sql.dry-run) on `connection` and
    return the resulting Statement, which validates and compiles the query
    without executing it.
    """
    statement_name = f"{connection.credentials.statement_name_prefix}{uuid.uuid4()}"
    logger.info(f"Dry running SQL to infer schema: {sql}")
    response = connection.handle._execute_statement(
        sql,
        ExecutionMode.SNAPSHOT,
        statement_name,
        [connection.credentials.statement_label, HIDDEN_LABEL],
        {"sql.dry-run": "true"},
        compute_pool_id=None,
    )
    statement = Statement.from_response(connection.handle, response)
    if statement.is_failed:
        raise DbtDatabaseError(
            f"Dry run failed for tested model query: {statement.status.get('detail', '')}"
        )
    return statement


def get_raw_columns(connection: Connection, sql: str) -> list[Column]:
    """`sql`'s result columns via a Flink dry run, as the driver's own
    untranslated Column/ColumnTypeDefinition objects - i.e. without running
    them through get_ddl_type or try_get_castable_type.

    Exposed (rather than folded into get_schema) so a caller can inspect a
    query's columns and choose per-column which translation it needs (or
    none at all) - get_schema always applies one translation to every column
    and raises on the first one that doesn't support it.
    """
    statement = _dry_run_statement(connection, sql)
    return statement.schema.columns if statement.has_schema() else []


@dataclass(frozen=True)
class DryRunSchema:
    """The parts of a dry run's response get_schema currently resolves."""

    columns: list[ConfluentColumn]


def get_schema(connection: Connection, sql: str) -> DryRunSchema:
    """Resolve `sql`'s schema via a Flink dry run (sql.dry-run), which
    validates and compiles the query without executing it, on the given
    confluent_sql `connection`.

    Uses get_ddl_type (not try_get_castable_type) to translate each column,
    so this always succeeds except for UNREPRESENTABLE_TYPES - it's meant
    for faithfully reconstructing what a dry run reported (e.g. recreating
    an equivalent table, or a future structural drift comparison), not for
    building a CAST target for a fixture value. See get_tested_model_columns
    (impl.py) for the fixture-casting case, which uses try_get_castable_type
    directly instead of this.
    """
    return DryRunSchema(
        columns=[
            # ConfluentColumn.create is inherited from dbt-core's Column,
            # typed to return Column - it always returns cls's own type
            # (ConfluentColumn here) at runtime.
            cast(
                ConfluentColumn,
                ConfluentColumn.create(column.name, get_ddl_type(column.type)),
            )
            for column in get_raw_columns(connection, sql)
        ]
    )
