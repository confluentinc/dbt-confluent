"""Unit tests for computing a value once across threads.

The callers are real threads released together by a barrier, so a broken leader election shows up
as more than one `compute` call rather than being masked by sequential execution.
"""

import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from dbt.adapters.confluent.singleflight import SingleFlightCell, SingleFlightDict

CALLERS = 8
GENERAL_TIMEOUT = 10.0
TINY_TIMEOUT = 0.05
# Bounds how long a test waits on a thread that should already have finished.
JOIN_TIMEOUT = 5.0
# Time for a thread that has just announced it is about to call `get` to reach the blocking wait.
SETTLE_TIME = 0.1


class CountingCompute:
    """A `compute` function that counts its calls and can fail or wait on demand."""

    def __init__(self, result: Any = "value", error: BaseException | None = None) -> None:
        self.calls = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.result = result
        self.error = error
        self._block = False
        self._lock = threading.Lock()

    def blocking(self) -> "CountingCompute":
        self._block = True
        return self

    def __call__(self) -> Any:
        with self._lock:
            self.calls += 1
        self.entered.set()
        if self._block:
            assert self.release.wait(JOIN_TIMEOUT), "test never released the leader"
        if self.error is not None:
            raise self.error
        return self.result


def run_concurrently(call: Callable[[], Any], callers: int = CALLERS) -> list[Any]:
    """Run `call` on `callers` threads released together; return each outcome (value or error)."""
    barrier = threading.Barrier(callers)

    def one() -> Any:
        barrier.wait(JOIN_TIMEOUT)
        try:
            return call()
        except BaseException as error:
            return error

    with ThreadPoolExecutor(max_workers=callers) as pool:
        futures = [pool.submit(one) for _ in range(callers)]
        return [f.result(JOIN_TIMEOUT) for f in futures]


class WaitObservingCell(SingleFlightCell[str]):
    """A cell that reports each caller that has joined a flight as a waiter.

    `_wait` is only reached once a caller has taken its reference to the current flight under the
    cell's lock, so seeing a caller there proves it will get that flight's outcome, however late
    the leader finishes. Sleeping and hoping instead would let a slow thread start a new attempt.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._joined = threading.Semaphore(0)

    def _wait(self, flight: Any) -> str:
        self._joined.release()
        return super()._wait(flight)

    def wait_for_waiters(self, count: int) -> None:
        for _ in range(count):
            assert self._joined.acquire(timeout=JOIN_TIMEOUT), "a waiter never joined the flight"


def test_compute_runs_once_for_simultaneous_callers():
    cell = SingleFlightCell[str](GENERAL_TIMEOUT)
    compute = CountingCompute(result="the-value")

    outcomes = run_concurrently(lambda: cell.get(compute))

    assert compute.calls == 1
    assert outcomes == ["the-value"] * CALLERS


def test_later_call_returns_the_value_without_recomputing():
    cell = SingleFlightCell[str](GENERAL_TIMEOUT)
    compute = CountingCompute()
    cell.get(compute)

    assert cell.get(compute) == compute.result
    assert compute.calls == 1


def test_different_keys_do_not_block_each_other():
    cells = SingleFlightDict[str, str](GENERAL_TIMEOUT)
    slow = CountingCompute(result="slow-value").blocking()
    leader_results: list[str] = []
    leader = threading.Thread(target=lambda: leader_results.append(cells.get("slow", slow)))
    leader.start()
    try:
        assert slow.entered.wait(JOIN_TIMEOUT)

        # "slow" is still computing; "fast" must finish anyway.
        assert cells.get("fast", CountingCompute(result="fast-value")) == "fast-value"
        assert leader.is_alive(), "'slow' finished early, so 'fast' was not tested against it"
    finally:
        slow.release.set()
        leader.join(JOIN_TIMEOUT)

    assert not leader.is_alive()
    assert leader_results == [slow.result]


def test_same_key_computes_once_across_threads():
    cells = SingleFlightDict[str, str](GENERAL_TIMEOUT)
    compute = CountingCompute()

    outcomes = run_concurrently(lambda: cells.get("key", compute))

    assert compute.calls == 1
    assert outcomes == [compute.result] * CALLERS


def test_failure_is_shared_and_not_retried():
    original = RuntimeError("upload failed")
    cell = SingleFlightCell[str](GENERAL_TIMEOUT)
    compute = CountingCompute(error=original)

    outcomes = run_concurrently(lambda: cell.get(compute))

    assert compute.calls == 1
    assert outcomes == [original] * CALLERS, "every caller sees the original error"

    with pytest.raises(RuntimeError, match="upload failed"):
        cell.get(compute)
    assert compute.calls == 1


def test_uncached_failure_is_shared_with_waiters_then_the_next_call_retries():
    original = RuntimeError("list failed")
    cell = WaitObservingCell(GENERAL_TIMEOUT, cache_errors=False)
    failing = CountingCompute(error=original).blocking()
    outcomes: list[BaseException] = []
    threads = [
        threading.Thread(target=lambda: outcomes.append(_capture(lambda: cell.get(failing))))
        for _ in range(CALLERS)
    ]
    threads[0].start()
    assert failing.entered.wait(JOIN_TIMEOUT)
    for thread in threads[1:]:
        thread.start()
    cell.wait_for_waiters(CALLERS - 1)  # all are on the failing attempt before it fails
    failing.release.set()
    for thread in threads:
        thread.join(JOIN_TIMEOUT)

    assert failing.calls == 1, "waiters share the attempt they were waiting on"
    assert outcomes == [original] * CALLERS

    recovered = CountingCompute(result="recovered")
    assert cell.get(recovered) == "recovered"
    assert cell.get(CountingCompute(result="ignored")) == "recovered"
    assert recovered.calls == 1


def test_after_an_uncached_failure_a_new_flight_is_shared_like_any_other():
    cell = WaitObservingCell(GENERAL_TIMEOUT, cache_errors=False)
    with pytest.raises(RuntimeError):
        cell.get(CountingCompute(error=RuntimeError("first")))
    second = CountingCompute(result="second").blocking()
    leader = threading.Thread(target=lambda: cell.get(second))
    leader.start()
    assert second.entered.wait(JOIN_TIMEOUT)

    # A fresh flight is under way; a caller now waits on it rather than starting a third.
    late = CountingCompute(result="late")
    waiter_results: list[str] = []
    waiter = threading.Thread(target=lambda: waiter_results.append(cell.get(late)))
    waiter.start()
    cell.wait_for_waiters(1)
    second.release.set()
    leader.join(JOIN_TIMEOUT)
    waiter.join(JOIN_TIMEOUT)

    assert waiter_results == ["second"]
    assert late.calls == 0


def test_waiters_are_released_when_the_leader_is_interrupted():
    cell = SingleFlightCell[str](GENERAL_TIMEOUT)
    compute = CountingCompute(error=KeyboardInterrupt()).blocking()
    leader_outcome: list[BaseException] = []
    leader = threading.Thread(
        target=lambda: leader_outcome.append(_capture(lambda: cell.get(compute)))
    )
    leader.start()
    assert compute.entered.wait(JOIN_TIMEOUT)

    waiter_outcome: list[BaseException] = []
    waiter_calling = threading.Event()

    def wait_for_value() -> None:
        waiter_calling.set()
        waiter_outcome.append(_capture(lambda: cell.get(compute)))

    waiter = threading.Thread(target=wait_for_value)
    waiter.start()
    assert waiter_calling.wait(JOIN_TIMEOUT)
    time.sleep(SETTLE_TIME)  # let the waiter block before the leader is released
    assert waiter.is_alive() and not waiter_outcome
    compute.release.set()
    leader.join(JOIN_TIMEOUT)
    waiter.join(JOIN_TIMEOUT)

    assert not waiter.is_alive(), "waiter hung after the leader was interrupted"
    assert isinstance(leader_outcome[0], KeyboardInterrupt)
    assert waiter_outcome[0] is leader_outcome[0]


def test_waiter_times_out_on_a_hung_compute_and_the_cell_is_unchanged():
    cell = SingleFlightCell[str](TINY_TIMEOUT)
    compute = CountingCompute(result="late-value").blocking()
    leader = threading.Thread(target=lambda: cell.get(compute))
    leader.start()
    assert compute.entered.wait(JOIN_TIMEOUT)

    with pytest.raises(TimeoutError):
        cell.get(compute)

    compute.release.set()
    leader.join(JOIN_TIMEOUT)
    assert cell.get(compute) == "late-value"
    assert compute.calls == 1


def _capture(call: Callable[[], Any]) -> BaseException:
    try:
        call()
    except BaseException as error:
        return error
    raise AssertionError("expected the call to raise")
