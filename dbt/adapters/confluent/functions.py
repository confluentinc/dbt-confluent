"""Core logic related to function (i.e. UDF) management."""

import re
from typing import Any

from dbt_common.exceptions import CompilationError

# Flink artifact IDs look like `cfa-xxxxxx`.
_ARTIFACT_ID_RE = re.compile(r"cfa-[A-Za-z0-9]+")

_FUNCTION_LANGUAGES = ("java", "python")


def validate_function_config(model_config: Any) -> dict[str, Any]:
    """Validate a `function` node's config and return the values the DDL needs.

    The config must name the `language` (java/python), the `artifact_id` (`cfa-...`) and the
    `class` to bind (a Python module path for python). `connections` is an optional list of
    connection names. Only scalar functions are supported. Collects every problem into one
    error. `model_config` is the Jinja config object (anything with `.get`).
    """
    language = str(model_config.get("language") or "").lower()
    artifact_id = model_config.get("artifact_id")
    class_name = model_config.get("class")
    connections = model_config.get("connections") or []
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
    if isinstance(connections, str) or not all(isinstance(c, str) and c for c in connections):
        problems.append("'connections' must be a list of connection names")
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
        "connections": list(connections),
    }
