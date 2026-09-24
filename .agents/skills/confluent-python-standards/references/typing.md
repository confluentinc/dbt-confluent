# Type Annotation Syntax — Python 3.10+

_Read when reviewing or writing annotations. Apply the 3.10 baseline always, then only the sections at or below the project's detected floor._

## 3.10 Baseline (always applies)

- **`X | Y` union types** (PEP 604) — replaces `Union[X, Y]` and `Optional[X]`
- **Built-in generic types** (PEP 585) — `list[T]`, `dict[K, V]`, `set[T]`, `tuple[X, Y]`

Keep from `typing`: `TypeVar`, `Generic`, `Protocol` (rare), `TYPE_CHECKING`, `Any`.
Remove: `List`, `Dict`, `Set`, `Tuple`, `Union`, `Optional`.

```
List[X]       → list[X]          Union[X, Y]   → X | Y
Dict[K, V]    → dict[K, V]       Optional[X]   → X | None
Set[X]        → set[X]           Tuple[X, Y]   → tuple[X, Y]
```

```python
def find(id: str) -> User | None: ...

names: list[str] = []
config: dict[str, int] = {}
point: tuple[int, int] = (0, 0)

# Callables — use collections.abc, not typing
from collections.abc import Callable
handler: Callable[[str, int], bool]

# Forward references / circular imports
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypackage.models import Schema
```

### Typing rules

- **MUST type**: all public function params (except `self`/`cls`), all return values, all class attributes, module-level constants
- **SHOULD type**: internal function signatures, complex locals
- **MAY skip**: obvious local variables (`count = 0`), short lambda params, loop vars in short comprehensions

### Interfaces

Prefer **ABC** for internal code you own — you control every implementation, you may need `isinstance()`, and ABCs can share method bodies. Use **Protocol** only for structural typing against an external library you don't control, or for a minimal 1–2 method surface.

```python
from abc import ABC, abstractmethod

class SchemaRepository(ABC):
    @abstractmethod
    def get(self, subject: str) -> dict[str, str] | None: ...

    @abstractmethod
    def register(self, subject: str, schema: str) -> int: ...
```

### Generics on 3.10

`TypeVar` is still required — PEP 695 syntax is 3.12+.

```python
from typing import TypeVar, Generic
T = TypeVar("T")

def first(items: list[T]) -> T | None:
    return items[0] if items else None

class Relation(Generic[T]): ...
```

---

## 3.11+ — `Self`

`Self` (PEP 673) types methods that return their own instance; it replaces a bound `TypeVar`.

```python
from typing import Self

class RelationBuilder:
    def schema(self, schema: str) -> Self:
        self._schema = schema
        return self

    def build(self) -> Relation: ...

class Config:
    @classmethod
    def from_env(cls) -> Self:
        return cls(...)
```

```python
# 3.10: bound TypeVar          # 3.11+: cleaner
T = TypeVar("T", bound="Builder")
def set_name(self: T, n: str) -> T: ...   →   def set_name(self, n: str) -> Self: ...
```

---

## 3.12+ — PEP 695 type parameters and `type` aliases

No `TypeVar` import needed for simple generics.

```python
def first[T](items: list[T]) -> T | None:
    return items[0] if items else None

class Stack[T]:
    def __init__(self) -> None:
        self._items: list[T] = []

# Explicit type aliases
type UserId = str
type Result[T] = tuple[T, str | None]
```

Still use `TypeVar` for **constrained** type vars (`TypeVar("T", str, bytes)`), covariance/contravariance, or one type var reused across several functions.

---

## 3.13+ — `TypeIs` and deprecation annotations

```python
from typing import TypeIs

def is_string_list(val: list[object]) -> TypeIs[list[str]]:
    return all(isinstance(x, str) for x in val)
```

`TypeIs` narrows in both branches, unlike `TypeGuard`; the narrowed type must be a subtype of the input. Prefer it over `TypeGuard` for subtype narrowing.

```python
import warnings

@warnings.deprecated("Use new_relation() instead")
def old_relation(name: str) -> None: ...
```

Annotate deprecated public APIs with `@warnings.deprecated` — type checkers and IDEs pick it up without removing the function prematurely.
