# Type Annotation Syntax — Python 3.13

_Incremental improvements; 3.12 syntax is current best practice._

## New in 3.13

- **`TypeIs`** (PEP 742) — narrowing type guard that returns a narrowed type (unlike `TypeGuard`)
- Improved error messages and `locals()` snapshot semantics
- `warnings.deprecated()` for deprecation annotations

## All earlier syntax continues to work

See [`versions/python-3.12.md`](python-3.12.md) for PEP 695 and `type` statement.

## TypeIs narrowing

```python
from typing import TypeIs

def is_string_list(val: list[object]) -> TypeIs[list[str]]:
    return all(isinstance(x, str) for x in val)

def process(items: list[object]) -> None:
    if is_string_list(items):
        # items is narrowed to list[str] here
        for s in items:
            print(s.upper())
```

`TypeIs` differs from `TypeGuard`: the narrowed type must be a subtype of the input type.

## Deprecation annotations

```python
import warnings

@warnings.deprecated("Use new_producer() instead")
def old_producer(bootstrap: str) -> None: ...
```

## Practical advice

For Confluent Python projects targeting 3.13, the main additions to your normal 3.12 practice are:
- Use `TypeIs` when you write narrowing guard functions (prefer over `TypeGuard` for subtypes)
- Annotate deprecated public APIs with `@warnings.deprecated` — this gives type checkers and IDEs a signal without removing the function prematurely
