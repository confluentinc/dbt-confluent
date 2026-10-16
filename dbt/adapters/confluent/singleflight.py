"""Compute a value once, however many threads ask for it at the same time.

dbt runs models on several worker threads. When they all need the same slow result (a file hash, a
listing, an upload), the first thread to ask computes it and the rest wait for that one answer
instead of repeating the work. A failure is shared too: it is never retried.

`SingleFlightCell` holds one value; `SingleFlightDict` holds one cell per key.
"""

import threading
import time
from collections.abc import Callable, Hashable
from typing import Any, Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class SharedComputeError(Exception):
    """Raised to a waiter when the caller that computed its value failed.

    `__cause__` is the original error.
    """


class ComputeTimeout(TimeoutError):
    """Raised to a waiter that gave up waiting for the value to be computed."""


class SingleFlightCell(Generic[V]):
    """One value, filled in once by whichever caller asks first.

    `get(compute)`: the first caller (the leader) runs `compute()` on its own thread; later callers
    block until it finishes and receive the same value. The outcome is remembered for the life of
    the object, a failure included, so nothing is retried. The leader sees the original error; each
    waiter gets its own `SharedComputeError` chained to it, so every caller has its own traceback.
    A waiter stops waiting `compute_timeout` seconds after computation began and raises
    `ComputeTimeout` (the leader keeps running, and the value is still recorded if it later
    finishes). `compute` must not call `get` on the same cell.

    `compute` is supplied per call, so only the leader's runs; the waiters' functions are discarded,
    along with anything they close over (a connection, a config). Callers sharing a cell must
    therefore be interchangeable: any one of their `compute` functions has to give the answer all
    of them want. If two callers could want different results, they belong in different cells.
    """

    def __init__(self, compute_timeout: float) -> None:
        self._compute_timeout = compute_timeout
        # Held only to pick the leader, never while `compute` runs.
        self._lock = threading.Lock()
        self._started = False
        self._started_at = 0.0
        self._done = threading.Event()
        self._value: V | None = None
        self._error: BaseException | None = None

    def get(self, compute: Callable[[], V]) -> V:
        with self._lock:
            is_leader = not self._started
            if is_leader:
                self._started = True
                self._started_at = time.monotonic()

        if is_leader:
            return self._lead(compute)
        return self._wait()

    def _lead(self, compute: Callable[[], V]) -> V:
        try:
            value = compute()
            self._value = value
            return value
        except BaseException as error:
            self._error = error
            raise
        finally:
            # Even a KeyboardInterrupt on the leader's thread must wake the waiters.
            self._done.set()

    def _wait(self) -> V:
        remaining = self._started_at + self._compute_timeout - time.monotonic()
        if not self._done.wait(max(remaining, 0)):
            raise ComputeTimeout(
                f"Gave up waiting after {self._compute_timeout:g}s for another thread to finish "
                "computing this value"
            )
        if self._error is not None:
            raise SharedComputeError(
                f"Another thread failed to compute this value: "
                f"{type(self._error).__name__}: {self._error}"
            ) from self._error
        return self._value  # type: ignore[return-value]


class SingleFlightDict(Generic[K, V]):
    """A dict of `SingleFlightCell`s: `get(key, compute)` is `cell_for(key).get(compute)`.

    Different keys are computed in parallel. `compute` is supplied per call, not per dict, because
    what it needs (a connection, a config) comes from the caller, not from the key.
    """

    def __init__(self, compute_timeout: float) -> None:
        self._compute_timeout = compute_timeout
        # Held only to look up or insert a cell, never while computing.
        self._lock = threading.Lock()
        self._cells: dict[K, SingleFlightCell[Any]] = {}

    def get(self, key: K, compute: Callable[[], V]) -> V:
        with self._lock:
            cell = self._cells.get(key)
            if cell is None:
                cell = self._cells[key] = SingleFlightCell(self._compute_timeout)
        return cell.get(compute)
