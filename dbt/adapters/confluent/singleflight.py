"""Compute a value once, however many threads ask for it at the same time.

dbt runs models on several worker threads. When they all need the same slow result (a file hash, a
listing, an upload), the first thread to ask computes it and the rest wait for that one answer
instead of repeating the work. A failure is shared too, and by default never retried; a cell can
opt to forget it so that a later caller tries again.

`SingleFlightCell` holds one value; `SingleFlightDict` holds one cell per key.
"""

import threading
import time
from collections.abc import Callable, Hashable
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class _Flight(Generic[V]):
    """One attempt at computing a cell's value: who started it, when, and how it ended."""

    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self.done = threading.Event()
        self.value: V | None = None
        self.error: BaseException | None = None


class SingleFlightCell(Generic[V]):
    """One value, filled in once by whichever caller asks first.

    `get(compute)`: the first caller (the leader) runs `compute()` on its own thread; later callers
    block until it finishes and receive the same value. The outcome is remembered for the life of
    the object, so nothing is computed twice. By default a failure is remembered too: every caller,
    leader or waiter, gets the original error and nothing is retried. With `cache_errors=False`
    a failure is shared only with the callers already waiting on that attempt; the next caller
    after it starts a fresh attempt. That suits an idempotent read, where one transient failure
    (or one thread's broken connection) should not fail the whole build. A waiter stops waiting
    `compute_timeout` seconds after computation began and raises `TimeoutError` (the leader keeps
    running, and the value is still recorded if it later finishes). A leader that hangs is never
    replaced, whatever `cache_errors` says. `compute` must not call `get` on the same cell.

    `compute` is supplied per call, so only the leader's runs; the waiters' functions are discarded,
    along with anything they close over (a connection, a config). Callers sharing a cell must
    therefore be interchangeable: any one of their `compute` functions has to give the answer all
    of them want. If two callers could want different results, they belong in different cells.

    A failure is shared as one exception instance, re-raised on every caller's thread, so its
    `__traceback__` collects the frames of all of them. `compute` must therefore not raise
    anything dbt mutates per node: dbt's task runner calls `DbtRuntimeError.add_node` only on an
    error with no node yet, so the first thread to handle the shared error stamps its node on it
    and every other function's failure is then reported against that one. Raise a plain or driver
    exception here and let each caller translate it into a dbt error of its own.
    """

    def __init__(self, compute_timeout: float, *, cache_errors: bool = True) -> None:
        self._compute_timeout = compute_timeout
        self._cache_errors = cache_errors
        # Held only to pick the leader or retire a failed flight, never while `compute` runs.
        self._lock = threading.Lock()
        self._flight: _Flight[V] | None = None

    def get(self, compute: Callable[[], V]) -> V:
        with self._lock:
            flight = self._flight
            is_leader = flight is None
            if flight is None:
                flight = self._flight = _Flight()

        if is_leader:
            return self._lead(flight, compute)
        return self._wait(flight)

    def _lead(self, flight: _Flight[V], compute: Callable[[], V]) -> V:
        try:
            value = compute()
            flight.value = value
            return value
        except BaseException as error:
            flight.error = error
            if not self._cache_errors:
                # Retire this flight so the next caller leads a new one. Callers already waiting
                # hold `flight` itself and still get `error`.
                with self._lock:
                    if self._flight is flight:
                        self._flight = None
            raise
        finally:
            # Even a KeyboardInterrupt on the leader's thread must wake the waiters.
            flight.done.set()

    def _wait(self, flight: _Flight[V]) -> V:
        remaining = flight.started_at + self._compute_timeout - time.monotonic()
        if not flight.done.wait(max(remaining, 0)):
            raise TimeoutError(
                f"Gave up waiting after {self._compute_timeout:g}s for another thread to finish "
                "computing this value"
            )
        if flight.error is not None:
            raise flight.error
        return flight.value  # type: ignore[return-value]


class SingleFlightDict(Generic[K, V]):
    """A dict of `SingleFlightCell`s: `get(key, compute)` runs `compute` in that key's cell.

    Different keys are computed in parallel. `compute` is supplied per call, not per dict, because
    what it needs (a connection, a config) comes from the caller, not from the key.
    """

    def __init__(self, compute_timeout: float) -> None:
        self._compute_timeout = compute_timeout
        # Held only to look up or insert a cell, never while computing.
        self._lock = threading.Lock()
        self._cells: dict[K, SingleFlightCell[V]] = {}

    def get(self, key: K, compute: Callable[[], V]) -> V:
        with self._lock:
            cell = self._cells.get(key)
            if cell is None:
                cell = self._cells[key] = SingleFlightCell(self._compute_timeout)
        return cell.get(compute)
