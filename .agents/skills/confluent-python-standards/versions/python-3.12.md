# Type Annotation Syntax — Python 3.12

_Adds PEP 695 type parameter syntax and `type` statement aliases._

## New in 3.12

- **PEP 695 type parameter syntax** — `def func[T](x: T) -> T`, `class C[T]:`
- **`type` statement** — explicit type aliases: `type UserId = str`

## All 3.10 and 3.11 syntax continues to work

See [`versions/python-3.11.md`](python-3.11.md) for `Self` and other 3.11 additions.

## Generic functions — PEP 695

```python
# 3.12+: no TypeVar import needed for simple generics
def first[T](items: list[T]) -> T | None:
    return items[0] if items else None

def zip_dicts[K, V](keys: list[K], values: list[V]) -> dict[K, V]:
    return dict(zip(keys, values))

# Still use TypeVar for constraints and covariance
from typing import TypeVar
Numeric = TypeVar("Numeric", int, float)
def add(a: Numeric, b: Numeric) -> Numeric: ...
```

## Generic classes — PEP 695

```python
from typing import Self

class Stack[T]:
    def __init__(self) -> None:
        self._items: list[T] = []

    def push(self, item: T) -> Self:
        self._items.append(item)
        return self

    def pop(self) -> T | None:
        return self._items.pop() if self._items else None
```

## Type aliases

```python
# 3.12+: explicit type statement (preferred for generics)
type UserId = str
type JsonValue = dict[str, JsonValue] | list[JsonValue] | str | int | float | bool | None
type Result[T] = tuple[T, str | None]

# Simple assignment still valid for non-generic aliases
Config = dict[str, str | int | bool]
```

## When to use PEP 695 vs TypeVar

| Use PEP 695 | Still use TypeVar |
|-------------|-------------------|
| Simple generics (no constraints) | Constrained: `TypeVar("T", str, bytes)` |
| New code | Covariant/contravariant type vars |
| Most generic functions/classes | Reusing one TypeVar across multiple functions |
