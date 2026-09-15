# Python Code Style — Confluent Python Projects

_Read when reviewing or writing Python code in any Confluent Python project (adapters, SDKs, CLIs, pipelines). Covers coding patterns, anti-patterns, and exceptions._

## Table of Contents

- [Default stance: prefer explicit preconditions (LBYL)](#default-stance-prefer-explicit-preconditions-lbyl)
- [Exception handling](#exception-handling)
- [Path operations](#path-operations)
- [Import organisation](#import-organisation)
- [Performance rules](#performance-rules)
- [Anti-patterns](#anti-patterns)
- [Backwards compatibility philosophy](#backwards-compatibility-philosophy)

---

## Default Stance: Prefer Explicit Preconditions (LBYL)

This skill leans LBYL when a cheap, precise precondition keeps intent clearer than a `try/except`.

```python
# CORRECT: Check first
if key in mapping:
    value = mapping[key]
    process(value)

# WRONG: Exception as control flow
try:
    value = mapping[key]
    process(value)
except KeyError:
    pass
```

---

## Exception Handling

### Core principle

Prefer LBYL for routine branching when the precondition is cheap and precise.

Use exceptions (EAFP) when:
1. **Error boundaries** — CLI/API level, translate failures to user-visible errors
2. **The operation itself is the authoritative test** — no cheap pre-check exists
3. **Adding context before re-raising** — wrap third-party exceptions with more detail

### Dictionary access

```python
# CORRECT: membership test
if key in mapping:
    value = mapping[key]
    process(value)

# ALSO CORRECT: .get() with default
value = mapping.get(key, default_value)

# WRONG: KeyError as control flow
try:
    value = mapping[key]
except KeyError:
    handle_missing()
```

### Error boundaries (CLI / API level)

```python
@app.command()
def run(topic: str) -> None:
    try:
        produce_to(topic)
    except Exception as e:
        typer.echo(json.dumps({"success": False, "error": str(e)}))
        raise SystemExit(1) from e
```

### Exception chaining (ruff B904)

Always chain exceptions when raising inside `except`. Never lose the original traceback:

```python
# CORRECT: preserve context
try:
    parse_config(path)
except ValueError as e:
    raise ConfigError(f"Bad config at {path}: {e}") from e

# CORRECT: intentionally suppress (CLI JSON output)
try:
    result = operation()
except RuntimeError as e:
    typer.echo(json.dumps({"error": str(e)}))
    raise SystemExit(1) from None  # traceback irrelevant to CLI user

# WRONG: missing chain (B904 violation)
try:
    parse_config(path)
except ValueError:
    raise SystemExit(1)  # lint error
```

### Never swallow silently

```python
# WRONG
try:
    optional_feature()
except Exception:
    pass  # impossible to diagnose

# CORRECT: let bubble, or log at boundary
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

# Usage
port = try_parse(int, user_input, 80)
```

---

## Path Operations

### Use pathlib — never `os.path`

```python
# CORRECT
from pathlib import Path
config_file = Path.home() / ".config" / "app.yml"
if config_file.exists():
    content = config_file.read_text(encoding="utf-8")

# WRONG
import os.path
config_file = os.path.join(os.path.expanduser("~"), ".config", "app.yml")
```

### Always specify encoding

```python
# CORRECT
content = path.read_text(encoding="utf-8")
path.write_text(data, encoding="utf-8")

# WRONG (platform-dependent)
content = path.read_text()
```

### Existence checks

Only call `.exists()` when filesystem presence is part of your requirement:

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

# WRONG: broad exception around APIs that communicate result directly
for wt_path in worktree_paths:
    try:
        resolved = wt_path.resolve()
        if current_dir.is_relative_to(resolved):
            current_worktree = resolved
            break
    except OSError:
        continue
```

---

## Import Organisation

1. **Module-level imports always** — no inline imports without documented justification
2. **Absolute imports only** — no relative imports
3. Inline imports are legitimate only for: circular dependencies, `TYPE_CHECKING`, conditional optional features

```python
# CORRECT: module-level, absolute
import json
from pathlib import Path
from mypackage.config import load_config

# CORRECT: TYPE_CHECKING to avoid runtime circular import
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypackage.models import Schema

# WRONG: relative import
from .config import load_config

# WRONG: inline import without justification
def my_function() -> None:
    import json  # never
```

---

## Performance Rules

### Properties must be O(1)

```python
# WRONG: property doing I/O or computation
@property
def schema_count(self) -> int:
    return self._fetch_from_registry()  # network call!

# CORRECT: explicit method name
def fetch_schema_count(self) -> int:
    return self._fetch_from_registry()

# CORRECT: O(1) property
@property
def schema_count(self) -> int:
    return self._cached_count
```

### Magic methods must be O(1)

```python
# WRONG: __len__ iterating
def __len__(self) -> int:
    return sum(1 for _ in self._items)

# CORRECT
def __len__(self) -> int:
    return self._count
```

---

## Anti-Patterns

### No re-exports — one canonical import path

Every symbol has exactly one import path. Never re-export through `__init__.py`:

```python
# WRONG: mypackage/__init__.py creates a second path
from mypackage.core import MyClass
__all__ = ["MyClass"]

# CORRECT: empty __init__.py; callers import from canonical location
# from mypackage.core import MyClass

# CORRECT: explicit re-export for required plugin entry points
from mypackage.core import my_function as my_function
```

### Declare variables close to use

```python
# WRONG: declared 20 lines before use
def process(ctx, items):
    result_path = compute_result_path(ctx)
    # ... many other lines ...
    save_to_path(transformed, result_path)

# CORRECT: inline at use site
def process(ctx, items):
    transformed = transform(items)
    save_to_path(transformed, compute_result_path(ctx))
```

### Don't destructure into single-use locals

```python
# WRONG: unnecessary extraction
result = fetch_schema(subject)
name = result.name
version = result.version
register(name, version)

# CORRECT: access attributes directly
schema = fetch_schema(subject)
register(schema.name, schema.version)
```

### Max 4 levels of indentation

```python
# WRONG: 5 levels deep
for topic in topics:
    if topic.enabled:
        for partition in topic.partitions:
            if partition.leader:
                for record in partition.records:
                    process(record)  # 5 levels!

# CORRECT: extract helper
for topic in topics:
    if topic.enabled:
        process_topic_partitions(topic.partitions)
```

### Keep context managers inline

```python
# CORRECT: lifecycle is visible
with lock if thread_safe else nullcontext():
    process(data)

# WRONG: lifecycle hidden
cm = lock if thread_safe else nullcontext()
with cm:
    process(data)
```

---

## Backwards Compatibility Philosophy

**Default: no backwards compatibility preservation.**

Only preserve when:
- Code is part of a public API with external consumers
- User explicitly requests it
- Migration cost is prohibitively high (document why)

```python
# WRONG: keeping old API unnecessarily
def produce(data: dict, legacy_format: bool = False) -> None:
    if legacy_format:
        return _old_produce(data)
    return _new_produce(data)

# CORRECT: break and migrate callsites immediately
def produce(data: dict) -> None:
    return _new_produce(data)
```

**SemVer consequence**: breaking a public API requires a major version bump — document your deprecation window (`CONTRIBUTING.md`) and apply it consistently rather than per-release.
