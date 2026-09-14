# Type Annotation Syntax — Python 3.11

_Adds `Self` type for method chaining and builder patterns._

## New in 3.11

- **`Self` type** (PEP 673) — for methods that return the instance; replaces bound `TypeVar`

## All 3.10 syntax continues to work

See [`versions/python-3.10.md`](python-3.10.md) for the full 3.10 baseline.

## Self type

```python
from typing import Self

class KafkaProducerBuilder:
    def bootstrap_servers(self, servers: str) -> Self:
        self._servers = servers
        return self

    def topic(self, topic: str) -> Self:
        self._topic = topic
        return self

    def build(self) -> KafkaProducer:
        return KafkaProducer(self._servers, self._topic)

# Fluent interface
producer = KafkaProducerBuilder().bootstrap_servers("localhost:9092").topic("events").build()

# Factory classmethod
class Config:
    @classmethod
    def from_env(cls) -> Self:
        return cls(...)
```

## Replace bound TypeVar with Self

```python
# OLD (3.10): bound TypeVar for self-returning methods
T = TypeVar("T", bound="Builder")
def set_name(self: T, name: str) -> T: ...

# NEW (3.11): cleaner
from typing import Self
def set_name(self, name: str) -> Self: ...
```
