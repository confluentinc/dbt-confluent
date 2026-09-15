# Type Annotation Syntax — Python 3.10

_Minimum version for `dbt-confluent` and most Confluent Python projects._

## What's available from 3.10

- **`X | Y` union types** (PEP 604) — replaces `Union[X, Y]` and `Optional[X]`
- **Built-in generic types** (PEP 585) — `list[T]`, `dict[K, V]`, `set[T]`, `tuple[X, Y]`

## What to use from `typing`

Keep only: `TypeVar`, `Generic`, `Protocol` (rare), `TYPE_CHECKING`, `Any`

Remove: `List`, `Dict`, `Set`, `Tuple`, `Union`, `Optional`

## Quick reference

```python
# Unions and Optional
def find(id: str) -> User | None: ...
def process(value: str | int | float) -> str: ...

# Collections — use built-ins
names: list[str] = []
config: dict[str, int] = {}
ids: set[str] = set()
point: tuple[int, int] = (0, 0)

# Callables — use collections.abc
from collections.abc import Callable
handler: Callable[[str, int], bool]

# Generics — TypeVar still required in 3.10
from typing import TypeVar, Generic
T = TypeVar("T")

def first(items: list[T]) -> T | None:
    return items[0] if items else None

class Stack(Generic[T]):
    def __init__(self) -> None:
        self._items: list[T] = []

# Forward references / circular imports
from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from mypackage.models import Schema
```

## Typing rules

- **MUST type**: all public function params (except `self`/`cls`), all return values, all class attributes, module-level constants
- **SHOULD type**: internal function signatures, complex locals
- **MAY skip**: obvious local variables (`count = 0`), short lambda params, loop vars in short comprehensions

## Interfaces

Prefer **ABC** for internal code you own. Use **Protocol** only for structural/duck-typing against external libraries.

```python
from abc import ABC, abstractmethod

class SchemaRepository(ABC):
    @abstractmethod
    def get(self, subject: str) -> dict[str, str] | None: ...

    @abstractmethod
    def register(self, subject: str, schema: str) -> int: ...
```

## Migration from Python 3.9

```
List[X]       → list[X]
Dict[K, V]    → dict[K, V]
Set[X]        → set[X]
Tuple[X, Y]   → tuple[X, Y]
Union[X, Y]   → X | Y
Optional[X]   → X | None
```
