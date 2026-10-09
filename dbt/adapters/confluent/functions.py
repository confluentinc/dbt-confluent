"""Config validation and state comparison for the `function` materialization (#179).

A Confluent UDF is registered from an artifact (a Java JAR or Python ZIP) that was already
uploaded to Confluent Cloud, so a function's config must name the artifact and the class to bind.
Functions are immutable in Flink (no ALTER / CREATE OR REPLACE), so re-running the materialization
compares the live function (via `DESCRIBE FUNCTION`) to the config: identical is a no-op, anything
else is a drop and re-create.
"""

import re
from collections.abc import Callable
from typing import Any, NamedTuple, cast

from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base import BaseRelation

# Flink artifact IDs look like `cfa-xxxxxx`.
_ARTIFACT_ID_RE = re.compile(r"cfa-[A-Za-z0-9]+")

_FUNCTION_LANGUAGES = ("java", "python")


class QualifiedName(NamedTuple):
    """A `catalog.database.name` reference, e.g. to a connection."""

    catalog: str
    database: str
    name: str

    def render(self) -> str:
        return ".".join("`" + part.replace("`", "``") + "`" for part in self)


class FunctionState(NamedTuple):
    """The facets of a function that identify what it was created from."""

    class_name: str
    language: str
    artifact_id: str
    connections: frozenset[QualifiedName]


def _split_outside_backticks(text: str, sep: str) -> list[str]:
    """Split on `sep`, ignoring separators inside backtick-quoted identifiers."""
    parts, current, quoted = [], [], False
    for char in text:
        if char == "`":
            quoted = not quoted
        if char == sep and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def _unquote(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part.startswith("`") and part.endswith("`"):
        return part[1:-1].replace("``", "`")
    return part


def parse_qualified_name(text: str, catalog: str, database: str) -> QualifiedName:
    """Parse `name`, `database.name` or `catalog.database.name` (parts optionally backtick-quoted).

    Missing leading parts default to the given `catalog` and `database`. Raises ValueError for
    empty parts or more than three parts.
    """
    parts = [_unquote(p) for p in _split_outside_backticks(text, ".")]
    if not 1 <= len(parts) <= 3 or not all(parts):
        raise ValueError(f"{text!r} is not a valid [[catalog.]database.]name")
    defaults = [catalog, database]
    return QualifiedName(*(defaults[: 3 - len(parts)] + parts))


def render_identifier(text: str) -> str:
    """Render `name`, `database.name` or `catalog.database.name` as backtick-quoted identifiers.

    Parts are emitted as configured (unqualified names stay unqualified). Flink's
    `USING CONNECTIONS (...)` takes identifiers; a string literal is a parse error.
    """
    parts = [_unquote(p) for p in _split_outside_backticks(text, ".")]
    return ".".join("`" + part.replace("`", "``") + "`" for part in parts)


def validate_function_config(model_config: Any, catalog: str, database: str) -> dict[str, Any]:
    """Validate a `function` node's config and return the values the DDL needs.

    The config must name the `language` (java/python), the `artifact_id` (`cfa-...`) and the
    `class` to bind (a Python module path for python). `connections` is an optional list of
    connection names; a name that isn't fully qualified is resolved against `catalog` and
    `database` (the function's own). Only scalar functions are supported. Collects every
    problem into one error. `model_config` is the Jinja config object (anything with `.get`).

    Returns `language`, `artifact_id`, `class_name`, `connections` (a list of `QualifiedName`,
    for comparison) and `connection_names` (the names as configured, rendered as identifiers, for the DDL).
    """
    language = str(model_config.get("language") or "").lower()
    artifact_id = model_config.get("artifact_id")
    class_name = model_config.get("class")
    raw_connections = model_config.get("connections") or []
    function_type = str(model_config.get("type") or "scalar").lower()

    problems = []
    if language not in _FUNCTION_LANGUAGES:
        problems.append(
            f"'language' must be one of {', '.join(_FUNCTION_LANGUAGES)} "
            f"(got {model_config.get('language')!r})"
        )
    if not isinstance(artifact_id, str) or not _ARTIFACT_ID_RE.fullmatch(artifact_id):
        problems.append(
            f"'artifact_id' must be an artifact ID of the form 'cfa-...' (got {artifact_id!r})"
        )
    if not isinstance(class_name, str) or not class_name.strip():
        problems.append("'class' must be a non-empty string")
    connections: list[QualifiedName] = []
    if isinstance(raw_connections, str) or not all(
        isinstance(c, str) and c for c in raw_connections
    ):
        problems.append("'connections' must be a list of connection names")
    else:
        for raw in raw_connections:
            try:
                connections.append(parse_qualified_name(raw, catalog, database))
            except ValueError as e:
                problems.append(f"'connections': {e}")
    if function_type != "scalar":
        problems.append(f"only scalar functions are supported (got type '{function_type}')")
    if problems:
        raise CompilationError(
            "Invalid config for the 'function' materialization:\n  - " + "\n  - ".join(problems)
        )
    return {
        "language": language,
        "artifact_id": artifact_id,
        "class_name": class_name.strip(),
        "connections": connections,
        "connection_names": [render_identifier(raw) for raw in raw_connections],
    }


def parse_describe_function(rows: Any, catalog: str, database: str) -> FunctionState:
    """Build a FunctionState from the (name, value) rows of `DESCRIBE FUNCTION`.

    The `connections` row is only present when the function has connections, and holds a
    bracketed list of qualified names, e.g. "[`env`.`db`.`conn`]".
    """
    info = {str(name): str(value) for name, value in rows}
    raw_connections = info.get("connections", "[]").strip()
    if raw_connections.startswith("[") and raw_connections.endswith("]"):
        raw_connections = raw_connections[1:-1]
    connections = frozenset(
        parse_qualified_name(item, catalog, database)
        for item in _split_outside_backticks(raw_connections, ",")
        if item.strip()
    )
    return FunctionState(
        class_name=info.get("class name", ""),
        language=info.get("function language", "").lower(),
        artifact_id=info.get("plugin id", ""),
        connections=connections,
    )


def desired_function_state(udf: dict[str, Any]) -> FunctionState:
    """The FunctionState a validated config (see `validate_function_config`) asks for."""
    return FunctionState(
        class_name=udf["class_name"],
        language=udf["language"],
        artifact_id=udf["artifact_id"],
        connections=frozenset(udf["connections"]),
    )


def diff_function_state(existing: FunctionState, desired: FunctionState) -> list[str]:
    """Describe every way `existing` differs from `desired`; empty when they match."""
    changes = []
    for field in ("class_name", "language", "artifact_id"):
        old, new = getattr(existing, field), getattr(desired, field)
        if old != new:
            changes.append(f"{field}: {old!r} -> {new!r}")
    if existing.connections != desired.connections:

        def show(connections: frozenset[QualifiedName]) -> str:
            return "[" + ", ".join(sorted(c.render() for c in connections)) + "]"

        changes.append(f"connections: {show(existing.connections)} -> {show(desired.connections)}")
    return changes


def plan_function_change(
    execute: Callable[..., Any], relation: BaseRelation, udf: dict[str, Any]
) -> list[str] | None:
    """Compare the live function at `relation` to the validated config `udf`.

    `execute` is the adapter's `execute` (called as `execute(sql, fetch=True)`, returning
    `(response, table)`). Returns None when the function doesn't exist, otherwise the (possibly
    empty) list of differences; empty means the function is already what the config asks for.
    """
    database, schema = cast(str, relation.database), cast(str, relation.schema)
    # DESCRIBE alone tells us both whether the function exists and what it is, saving an
    # INFORMATION_SCHEMA round-trip on every run. The driver has no typed "function not
    # found" error, so absence is recognized by the server's message
    # ("Function with the identifier '`name`' doesn't exist."). That wording isn't a
    # documented contract, but if it changes this fails closed: the DESCRIBE error is
    # re-raised and the build fails, rather than wrongly creating or dropping anything.
    try:
        _, described = execute(f"DESCRIBE FUNCTION {relation.render()}", fetch=True)
    except DbtDatabaseError as e:
        message = str(e)
        if "doesn't exist" in message and f"`{relation.identifier}`" in message:
            return None
        raise
    existing = parse_describe_function(described.rows, database, schema)
    return diff_function_state(existing, desired_function_state(udf))


class FunctionPlan(NamedTuple):
    """What the materialization should do. `message` is the warning (replace, keep) or error
    (fail) text to surface, or None."""

    action: str  # create | unchanged | replace | keep | fail
    message: str | None = None


def plan_function_action(
    function: str, changes: list[str] | None, on_configuration_change: str
) -> FunctionPlan:
    """Decide what to do given `plan_function_change`'s result and dbt's
    `on_configuration_change` setting (apply | continue | fail).

    `function` is the rendered function name, for messages. When the function differs from its
    config, `apply` drops and re-creates it, `continue` leaves it in place, and `fail` errors.
    """
    if changes is None:
        return FunctionPlan("create")
    if not changes:
        return FunctionPlan("unchanged")
    details = "; ".join(changes)
    if on_configuration_change == "apply":
        return FunctionPlan(
            "replace",
            f"Replacing function {function} because its config changed ({details}). "
            "Statements already running with the old function may need to be restarted.",
        )
    prefix = (
        f"Configuration changes were identified and `on_configuration_change` was set to "
        f"`{on_configuration_change}` for {function} ({details})."
    )
    if on_configuration_change == "continue":
        return FunctionPlan("keep", f"{prefix} The existing function was left in place.")
    if on_configuration_change == "fail":
        return FunctionPlan(
            "fail",
            f"{prefix} Flink functions are immutable, so changing one means dropping and "
            "re-creating it, which can break statements that use it. Set "
            "`on_configuration_change: apply` to do that automatically.",
        )
    raise CompilationError(
        f"Unsupported on_configuration_change '{on_configuration_change}' "
        "(expected apply, continue or fail)"
    )
