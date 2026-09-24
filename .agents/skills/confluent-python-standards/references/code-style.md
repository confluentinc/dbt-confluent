# Python Code Style — Confluent dbt Adapter

_Read when reviewing or writing Python code in a Confluent dbt adapter project. Covers coding patterns, anti-patterns, and exceptions._

---

## Exception Handling

### Core principle

Prefer LBYL when a cheap, precise precondition keeps intent clearer than a `try/except`. Reach for exceptions (EAFP) when:

1. **Error boundaries** — CLI/API level, translating failures into user-visible errors
2. **The operation itself is the authoritative test** — no cheap pre-check exists. This is the common case in adapter code, where the network or DDL call *is* the check
3. **Adding context before re-raising** — wrapping a third-party exception with more detail

**Before writing a `try/except`, ask:** is this an error boundary? Is there a cheap, precise pre-check? Am I catching a *specific* exception, not bare `except:`/`except Exception:`? Am I adding meaningful context, or hiding the error? Default: let exceptions bubble up.

### Dictionary access

```python
# CORRECT: membership test
if key in mapping:
    process(mapping[key])

# ALSO CORRECT: .get() with default
value = mapping.get(key, default_value)

# WRONG: KeyError as routine control flow
try:
    value = mapping[key]
except KeyError:
    handle_missing()
```

### bare `raise` vs `raise e`

Use bare `raise` when re-raising the same exception — `raise e` truncates the traceback to the re-raise point:

```python
# WRONG: replaces the original traceback (ruff TRY201)
except SomeError as e:
    if attempt >= limit:
        raise e

# CORRECT: preserves the full traceback
except SomeError as e:
    if attempt >= limit:
        raise
```

`TRY201` catches this, but `TRY` is not in the recommended `select` — enforce it in review until it is.

### Exception chaining (ruff B904)

Chain when raising a *different* exception inside `except`. `B904` is enabled via the `B` group, so ruff blocks this at commit time:

```python
try:
    parse_config(path)
except ValueError as e:
    raise ConfigError(f"Bad config at {path}: {e}") from e
```

Use `from None` only when the original cause is genuinely noise.

### Never swallow silently

```python
# WRONG
try:
    optional_feature()
except Exception:
    pass  # impossible to diagnose

# CORRECT: let it bubble, or log at the boundary
try:
    optional_feature()
except Exception as e:
    logging.warning("Optional feature failed: %s", e)
```

### Prefer real parsers over brittle pre-checks

Don't replace parser calls with incomplete string heuristics (`str.isdigit()`, hand-rolled date checks). Extract a reusable helper instead:

```python
from typing import TypeVar, Callable

T = TypeVar("T")

def try_parse(parse: Callable[[str], T], value: str, default: T) -> T:
    """Parse value with parse, returning default on ValueError."""
    try:
        return parse(value)
    except ValueError:
        return default

port = try_parse(int, user_input, 80)
```

### No `assert` in production code

`assert` is stripped by Python's `-O` flag, so it cannot be a runtime check. Fine in tests; in adapter/connection/impl code, raise explicitly:

```python
# WRONG — silently removed under -O
assert cursor.statement is not None, "Cursor has no active statement"

# CORRECT
if cursor.statement is None:
    raise DbtDatabaseError("Cursor has no active statement")
```

Exception: `assert x is not None` used purely to narrow a type for mypy, where the value cannot be `None` at runtime. Prefer restructuring so the narrowing is unnecessary; if you keep it, a comment saying it is a type-narrowing assert stops the next reviewer from "fixing" it into a raise.

---

## Path Operations

Use `pathlib`, not `os.path`, and always pass `encoding`:

```python
# CORRECT
from pathlib import Path
config_file = Path.home() / ".config" / "app.yml"
if config_file.exists():
    content = config_file.read_text(encoding="utf-8")

# WRONG: os.path, and a platform-dependent default encoding
import os.path
config_file = os.path.join(os.path.expanduser("~"), ".config", "app.yml")
with open(config_file) as f:
    content = f.read()
```

Ruff's `PTH` group enforces this once enabled. **Known exception**: `dbt/include/<name>/__init__.py` uses `PACKAGE_PATH = os.path.dirname(__file__)` — the canonical dbt include-path idiom. Add a per-file-ignore for `PTH120` there rather than diverging from dbt.

### Existence checks

Call `.exists()` only when filesystem presence is part of the requirement, and never wrap `.resolve()` / `.is_relative_to()` in `try/except OSError` — they communicate their result directly.

```python
# CORRECT: check only when absence matters
for wt_path in worktree_paths:
    resolved = wt_path.resolve()
    if not resolved.exists():
        continue
    if current_dir.is_relative_to(resolved):
        current_worktree = resolved
        break

# CORRECT: ask resolve() to fail on missing
config_dir = config_path.resolve(strict=True)
```

---

## Import Organisation

1. **Module-level imports always** — ordering is ruff `I`; placement is your judgment
2. **Absolute imports only** — no relative imports. No enabled ruff rule catches this (`TID252` is not in the select list), so it is a review responsibility
3. Inline imports are legitimate **only** for circular dependencies, `TYPE_CHECKING`, or conditional optional features — and must carry a comment saying which

```python
# CORRECT: TYPE_CHECKING to avoid a runtime circular import
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypackage.models import Schema

# WRONG: relative import
from .config import load_config

# WRONG: inline import with no justification
def my_function() -> None:
    import json
```

---

## Performance Rules

### Properties and magic methods must be O(1)

Callers assume attribute access and `len()` are free. Anything that does I/O or iterates gets an explicit method name.

```python
# WRONG: property doing a network call
@property
def schema_count(self) -> int:
    return self._fetch_from_registry()

# CORRECT: explicit method name for the expensive path
def fetch_schema_count(self) -> int:
    return self._fetch_from_registry()

# CORRECT: O(1) property
@property
def schema_count(self) -> int:
    return self._cached_count
```

The same applies to `__len__`, `__bool__`, `__contains__` — return a stored count, don't `sum(1 for _ in self._items)`.

---

## Anti-Patterns

### No re-exports — one canonical import path

Every symbol has exactly one import path. The exception is dbt's plugin entry-point requirement — use the `as` form to make the re-export explicit:

```python
# WRONG: mypackage/__init__.py creates a second path
from mypackage.core import MyClass
__all__ = ["MyClass"]

# CORRECT: empty __init__.py; callers import from the canonical location
# from mypackage.core import MyClass

# CORRECT: explicit re-export for dbt plugin entry points (required by dbt's loader)
from dbt.adapters.confluent.impl import ConfluentAdapter as ConfluentAdapter
```

### Inline single-use values

Don't declare a local far from its use, and don't destructure object fields you reference once.

```python
# WRONG: declared 20 lines early; fields extracted for a single call
def process(ctx, items):
    result_path = compute_result_path(ctx)
    # ... many other lines ...
    result = fetch_schema(subject)
    name = result.name
    version = result.version
    register(name, version)
    save_to_path(transformed, result_path)

# CORRECT: compute at the use site; access attributes directly
def process(ctx, items):
    schema = fetch_schema(subject)
    register(schema.name, schema.version)
    save_to_path(transform(items), compute_result_path(ctx))
```

### Max 4 levels of indentation

```python
# WRONG: 5 levels deep
for topic in topics:
    if topic.enabled:
        for partition in topic.partitions:
            if partition.leader:
                for record in partition.records:
                    process(record)

# CORRECT: extract a helper
for topic in topics:
    if topic.enabled:
        process_topic_partitions(topic.partitions)
```

### Keep context managers inline

```python
# CORRECT: lifecycle is visible
with lock if thread_safe else nullcontext():
    process(data)

# WRONG: lifecycle hidden behind a name
cm = lock if thread_safe else nullcontext()
with cm:
    process(data)
```

### Keyword-only params past the first

A function taking 5+ parameters puts `*` after the first (or after `ctx`) so the rest must be named at the callsite. If the group is cohesive, a dataclass beats a long signature.

---

## Testing Patterns

### Prefer `pytest-mock` over `unittest.mock`

Use the `mocker` fixture instead of `unittest.mock.patch` context managers or decorators: it composes with pytest fixtures, unwinds automatically after each test, and avoids nesting. `mocker.MagicMock` *is* `unittest.mock.MagicMock`, re-exported — so drop the `unittest.mock` import entirely.

Requires `pytest-mock` in the test dependency group; add it before applying this rule to a repo that lacks it.

```python
# CORRECT: flat, automatic cleanup, no unittest.mock import
def test_retries_on_pool_exhausted(mocker, cursor):
    mocker.patch("dbt.adapters.confluent.connections.time.sleep")
    mock_event = mocker.patch("dbt.adapters.confluent.connections.fire_event")
    cursor.execute.side_effect = [ComputePoolExhaustedError(...), None]
    _run(cursor)
    assert cursor.execute.call_count == 2
    mock_event.assert_called()

# CORRECT: inline return_value when there's nothing further to assert on the mock
def test_statement_name(mocker, adapter):
    mocker.patch.object(adapter.connections, "get_thread_handle", return_value=mock_handle())
    assert adapter.get_statement_name("my_model", "my_project").startswith("dbt-")

# WRONG: unittest context managers — verbose, tempts nesting
def test_retries(cursor):
    with patch("dbt.adapters.confluent.connections.time.sleep"):
        with patch("dbt.adapters.confluent.connections.fire_event") as mock_event:
            _run(cursor)
    mock_event.assert_called()
```

---

## Backwards Compatibility Philosophy

**Default: break the API and migrate callsites immediately.** Only preserve compatibility when:

- The symbol is public API with external consumers (e.g. a dbt `@available` method called from Jinja macros)
- The user explicitly requests it
- Migration cost is prohibitively high — document why in `CONTRIBUTING.md`

```python
# WRONG: keeping the old API behind a flag nobody asked for
def get_statement_name(self, model_name: str, project_name: str, legacy: bool = False) -> str:
    if legacy:
        return _old_name(model_name)
    return sanitize_statement_name(f"{project_name}-{model_name}")

# CORRECT
def get_statement_name(self, model_name: str, project_name: str) -> str:
    return sanitize_statement_name(f"{project_name}-{model_name}")
```

**SemVer consequence**: breaking a public `@available` adapter method requires a major version bump — document the deprecation window in `CONTRIBUTING.md`.
