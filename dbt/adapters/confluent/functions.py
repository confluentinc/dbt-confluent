"""Config validation and state comparison for the `function` materialization (#179).

A Confluent UDF is registered from an artifact (a Java JAR or Python ZIP) in Confluent Cloud, so a
function's config must name the artifact (an uploaded `artifact_id`, or a local `artifact_path`
that dbt uploads, see function_artifacts.py) and the class to bind.
Functions are immutable in Flink (no ALTER / CREATE OR REPLACE), so re-running the materialization
compares the live function (via `DESCRIBE FUNCTION`) to the config: identical is a no-op, anything
else is a drop and re-create.
"""

import re
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple, cast

import agate
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.connection import AdapterResponse

from .function_artifacts import (
    ARTIFACT_FORMATS,
    ArtifactConfig,
    artifact_display_name,
    file_digest,
)

# The adapter's `execute` bound to a compute pool: `execute(sql, fetch=True, statement_name=...)`.
FunctionExecutor = Callable[..., tuple[AdapterResponse, agate.Table]]

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


class FunctionConfig(NamedTuple):
    """A validated `function` config (see `validate_function_config`).

    Exactly one of `artifact_id` (an artifact already uploaded) and `artifact_config` (a local
    package dbt uploads) is set. `connections` are for comparison; `connection_names` are the
    names as configured, rendered as identifiers, for the DDL.
    """

    language: str
    class_name: str
    connections: list[QualifiedName]
    connection_names: list[str]
    artifact_id: str | None
    artifact_config: ArtifactConfig | None


def _split_outside_backticks(text: str, sep: str) -> list[str]:
    """Split on `sep`, ignoring separators inside backtick-quoted identifiers."""
    parts: list[str] = []
    current: list[str] = []
    quoted = False
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
    """Strip one identifier part's backticks (`` inside is an escaped backtick).

    A part is either wholly unquoted, with no backticks at all, or fully enclosed in backticks
    with every inner backtick doubled. Anything else is malformed: raises ValueError.
    """
    part = part.strip()
    if part.startswith("`"):
        inner = part[1:-1]
        if len(part) < 2 or not part.endswith("`") or "`" in inner.replace("``", ""):
            raise ValueError(f"{part!r} has unbalanced backtick quoting")
        return inner.replace("``", "`")
    if "`" in part:
        raise ValueError(f"{part!r} has unbalanced backtick quoting")
    return part


def parse_qualified_name(text: str, catalog: str, database: str) -> QualifiedName:
    """Parse `name`, `database.name` or `catalog.database.name` (parts optionally backtick-quoted).

    Missing leading parts default to the given `catalog` and `database`. Raises ValueError for
    empty parts, more than three parts or malformed backtick quoting.
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


def validate_function_config(
    model_config: dict[str, Any],
    catalog: str | None,
    database: str | None,
    *,
    project_root: Path | None = None,
    project: str,
    name_prefix: str,
    function: str | None,
) -> FunctionConfig:
    """Validate a `function` node's config and return the values the DDL needs.

    The config must name the `language` (java/python), the `class` to bind (a Python module path
    for python) and exactly one of `artifact_id` (`cfa-...`, already uploaded) or `artifact_path`
    (a local .jar/.zip matching the language, relative to `project_root`, that dbt uploads).
    `connections` is an optional list of connection names; a name that isn't fully qualified is
    resolved against `catalog` and `database` (the function's own). Only scalar functions are
    supported. Collects every problem into one error. `model_config` is the Jinja config object
    (anything with `.get`).

    An `artifact_path` becomes an `ArtifactConfig` naming the artifact it is (or will be) uploaded
    under: `name_prefix`, the dbt `project` and the `function` name, plus a digest of the package's
    content. `catalog`, `database` and `function` are the function relation's parts; a relation
    missing any of them is an error.
    """
    if catalog is None or database is None or function is None:
        raise CompilationError(
            "The 'function' materialization needs a fully qualified relation "
            f"(got catalog={catalog!r}, database={database!r}, function={function!r})"
        )
    language = str(model_config.get("language") or "").lower()
    artifact_id = model_config.get("artifact_id")
    artifact_path = model_config.get("artifact_path")
    class_name = model_config.get("class")
    class_name = class_name.strip() if isinstance(class_name, str) else ""
    raw_connections = model_config.get("connections")
    if raw_connections is None:
        raw_connections = []
    function_type = model_config.get("type")
    function_type = "scalar" if function_type is None else str(function_type).lower()

    problems = []
    if language not in _FUNCTION_LANGUAGES:
        problems.append(
            f"'language' must be one of {', '.join(_FUNCTION_LANGUAGES)} "
            f"(got {model_config.get('language')!r})"
        )
    resolved_path: Path | None = None
    if artifact_id is not None and artifact_path is not None:
        problems.append("'artifact_id' and 'artifact_path' are mutually exclusive")
    elif artifact_path is not None:
        resolved_path = _resolve_artifact_path(artifact_path, language, project_root, problems)
    elif not isinstance(artifact_id, str) or not _ARTIFACT_ID_RE.fullmatch(artifact_id):
        problems.append(
            "one of 'artifact_id' (an artifact ID of the form 'cfa-...') or 'artifact_path' "
            f"(a local package to upload) is required (got artifact_id={artifact_id!r})"
        )
    if not class_name:
        problems.append("'class' must be a non-empty string")
    connections: list[QualifiedName] = []
    if not isinstance(raw_connections, list) or not all(
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
    artifact_config = None
    if resolved_path:
        digest = file_digest(resolved_path)
        artifact_config = ArtifactConfig(
            path=resolved_path,
            name=artifact_display_name(name_prefix, project, digest),
            project=project,
            language=language,
        )
    return FunctionConfig(
        language=language,
        class_name=class_name,
        connections=connections,
        connection_names=[render_identifier(raw) for raw in raw_connections],
        artifact_id=artifact_id,
        artifact_config=artifact_config,
    )


def _resolve_artifact_path(
    artifact_path: Any, language: str, project_root: Path | None, problems: list[str]
) -> Path | None:
    """The absolute path of a configured `artifact_path`, or None after recording why not."""
    if not isinstance(artifact_path, str) or not artifact_path:
        problems.append(f"'artifact_path' must be a non-empty string (got {artifact_path!r})")
        return None
    path = (project_root or Path()) / artifact_path
    if not path.is_file():
        problems.append(f"'artifact_path' {str(path)!r} is not a file")
        return None
    expected = ARTIFACT_FORMATS[language][0] if language in ARTIFACT_FORMATS else None
    if expected and path.suffix.lower() != expected:
        problems.append(
            f"'artifact_path' must be a {expected} file for {language} (got {path.name!r})"
        )
        return None
    return path.resolve()


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


def desired_function_state(udf: FunctionConfig) -> FunctionState:
    """The FunctionState a validated config (see `validate_function_config`) asks for.

    A function from an `artifact_config` whose artifact doesn't exist yet has no artifact ID
    (`udf.artifact_id` is None), which never equals a live one: it always needs creating.
    """
    artifact_id = udf.artifact_id
    if not artifact_id:
        assert udf.artifact_config  # validation guarantees one or the other
        artifact_id = f"<new upload of {udf.artifact_config.name}>"
    return FunctionState(
        class_name=udf.class_name,
        language=udf.language,
        artifact_id=artifact_id,
        connections=frozenset(udf.connections),
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


def plan_function_change(existing: FunctionState | None, udf: FunctionConfig) -> list[str] | None:
    """Compare the live function (`describe_function`'s result) to the validated config `udf`.

    Returns None when the function doesn't exist, otherwise the (possibly empty) list of
    differences; empty means the function is already what the config asks for.
    """
    return None if existing is None else diff_function_state(existing, desired_function_state(udf))


def describe_function(execute: FunctionExecutor, relation: BaseRelation) -> FunctionState | None:
    """The live function at `relation` (via `DESCRIBE FUNCTION`), or None if it doesn't exist.

    `execute` is called as `execute(sql, fetch=True)`.
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
    return parse_describe_function(described.rows, database, schema)


def render_create_function(
    relation: BaseRelation,
    class_name: str,
    language: str,
    artifact_id: str,
    connection_names: list[str],
) -> str:
    """The `CREATE FUNCTION` statement. `connection_names` are already rendered identifiers."""
    escaped_class = class_name.replace("'", "''")
    sql = f"create function {relation.render()} as '{escaped_class}'"
    if language == "python":
        sql += " language python"
    sql += f" using jar 'confluent-artifact://{artifact_id}'"
    if connection_names:
        sql += f" using connections ({', '.join(connection_names)})"
    return sql


class FunctionPlan(NamedTuple):
    """What the materialization should do. `message` is the warning (replace, keep) or error
    (fail) text to surface, or None."""

    action: str  # create | unchanged | replace | keep | fail
    message: str | None = None


def plan_function_action(
    function: str,
    changes: list[str] | None,
    on_configuration_change: str,
    full_refresh: bool = False,
) -> FunctionPlan:
    """Decide what to do given `plan_function_change`'s result and dbt's
    `on_configuration_change` setting (apply | continue | fail).

    `function` is the rendered function name, for messages. When the function differs from its
    config, `apply` drops and re-creates it, `continue` leaves it in place, and `fail` errors.
    `full_refresh` always drops and re-creates an existing function, matching or not, and
    overrides `on_configuration_change`, which governs changes to an existing function, not a
    rebuild the user asked for.
    """
    if changes is None:
        return FunctionPlan("create")
    if full_refresh:
        return FunctionPlan(
            "replace",
            f"Replacing function {function} because of --full-refresh. "
            "Statements already running with the old function may need to be restarted.",
        )
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
