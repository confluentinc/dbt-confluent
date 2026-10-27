"""Querying INFORMATION_SCHEMA for the catalog of a given set of relations.

The backend can't handle a single query over many relations (nor `OR` predicates), and gives up
on a statement after ~35s with a 500. So the relations are queried in chunks, one equality-only
UNION ALL query per chunk (the `get_catalog_relations` macro), and a failing chunk is retried
smaller after a backoff. See `query_catalog_rows`.
"""

import random
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.impl import InformationSchema
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Confluent")

Item = TypeVar("Item")
Result = TypeVar("Result")

# How many relations each INFORMATION_SCHEMA catalog query covers at first (see
# `query_catalog_rows`). Each failed query halves it for the rest of the catalog,
# because the backend gives up on a statement after ~35s and a query takes roughly 1s per
# relation it covers.
CATALOG_RELATIONS_CHUNK_SIZE = 10
# Consecutive failures before the catalog query gives up. Enough to halve the chunk size from
# `CATALOG_RELATIONS_CHUNK_SIZE` down to one relation (10 -> 5 -> 2 -> 1) and then fail
# three times there.
CATALOG_MAX_CONSECUTIVE_ERRORS = 6
# Pause before retrying a failed catalog query: exponential in the number of consecutive
# failures, capped, with +/-25% jitter.
CATALOG_RETRY_BASE_DELAY_SECS = 1.0
CATALOG_RETRY_MAX_DELAY_SECS = 30.0
CATALOG_RETRY_JITTER_BAND = (0.75, 1.25)


def query_catalog_rows(
    execute_macro: Callable[..., Any],
    information_schema: InformationSchema,
    relations: list[BaseRelation],
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[dict[str, Any]]:
    """Table and column rows from INFORMATION_SCHEMA for exactly `relations`, as a generator.

    Nothing is queried until the result is iterated, and errors surface then.

    Queries a few relations at a time (`ThrottledChunkReader`). A chunk that fails (typically a
    backend timeout, sadly reported as a 500, another HTTP 5xx, or a 429) is retried after a backoff with half the chunk
    size, and the remaining chunks keep that size rather than climbing back up. Once
    `CATALOG_MAX_CONSECUTIVE_ERRORS` queries have failed in a row, the
    error is raised. `execute_macro` is the adapter's, used to run `get_catalog_relations`;
    `sleep` waits out the backoff.
    """

    def log_retry(error: Exception, chunk: list[BaseRelation], delay: float) -> None:
        logger.warning(
            f"Catalog query for {len(chunk)} relation(s) failed ({error}); retrying in "
            f"{delay:.1f}s"
        )

    def is_retryable(error: Exception) -> bool:
        return isinstance(error, DbtDatabaseError) and is_retryable_backend_error(error)

    def query_chunk(chunk: list[BaseRelation]) -> list[dict[str, Any]]:
        logger.info(f"Fetching schema for chunk of {len(chunk)} relation(s)")

        return cast(
            list[dict[str, Any]],
            execute_macro(
                "get_catalog_relations",
                kwargs={"information_schema": information_schema, "relations": chunk},
            ),
        )

    reader = ThrottledChunkReader[BaseRelation](
        sorted(relations, key=str),
        initial_chunk_size=CATALOG_RELATIONS_CHUNK_SIZE,
        backoff=Backoff(
            base_delay_secs=CATALOG_RETRY_BASE_DELAY_SECS,
            max_delay_secs=CATALOG_RETRY_MAX_DELAY_SECS,
            jitter_band=CATALOG_RETRY_JITTER_BAND,
        ),
        is_retryable=is_retryable,
        on_retryable_error=log_retry,
        max_consecutive_errors=CATALOG_MAX_CONSECUTIVE_ERRORS,
        sleep=sleep,
    )

    return reader.run(query_chunk)


@dataclass(frozen=True)
class Backoff:
    """Pause before retrying: `base_delay_secs`, doubling with each consecutive failure up to
    `max_delay_secs`, then multiplied by a random factor from `jitter_band`."""

    base_delay_secs: float
    max_delay_secs: float
    jitter_band: tuple[float, float]


class ThrottledChunkReader(Generic[Item]):
    """Reads through `items` a chunk at a time, backing off and shrinking the chunk on failures.

    `run(query)` calls `query` with each chunk, which returns an iterable of results, and yields
    those results one by one (a generator `flat_map`), once their chunk has succeeded. A query
    that raises an error for which `is_retryable(error)` is true is reported to
    `on_retryable_error(error, chunk, delay)` and, after sleeping for `delay`, run again on the
    same items at half the size; later chunks keep the smaller size rather than climbing back
    up. Once `max_consecutive_errors` queries have failed in a row the error is raised. Any other
    error propagates untouched.
    """

    def __init__(
        self,
        items: list[Item],
        initial_chunk_size: int,
        backoff: Backoff,
        is_retryable: Callable[[Exception], bool],
        max_consecutive_errors: int,
        on_retryable_error: Callable[[Exception, list[Item], float], None],
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._items = items
        self._backoff = backoff
        self._is_retryable = is_retryable
        self._max_consecutive_errors = max_consecutive_errors
        self._on_retryable_error = on_retryable_error
        self._sleep = sleep
        self._start = 0
        self._size = initial_chunk_size
        self._consecutive_failures = 0

    def run(self, query: Callable[[list[Item]], Iterable[Result]]) -> Iterator[Result]:
        """Runs `query` over every item, a chunk at a time, yielding the results it returns."""
        while self._start < len(self._items):
            chunk = self._items[self._start : self._start + self._size]
            try:
                results = query(chunk)
            except Exception as e:
                if not self._is_retryable(e):
                    raise
                self._consecutive_failures += 1
                if self._consecutive_failures >= self._max_consecutive_errors:
                    raise
                self._size = max(1, self._size // 2)
                delay = random.uniform(*self._backoff.jitter_band) * min(
                    self._backoff.base_delay_secs * 2 ** (self._consecutive_failures - 1),
                    self._backoff.max_delay_secs,
                )
                self._on_retryable_error(e, chunk, delay)
                self._sleep(delay)
                continue
            self._start += len(chunk)
            self._consecutive_failures = 0
            yield from results


def is_retryable_backend_error(error: DbtDatabaseError) -> bool:
    """Whether `error` is the backend answering a catalog query with an HTTP 429 (rate limited) or
    5xx: a 500 is how it reports giving up on a slow statement, the rest are brief outages.

    The adapter wraps the driver's error, which carries the HTTP status, so it is on `__cause__`.
    The driver retries these statuses itself for GETs, but never for the statement-submission POST
    that this query goes through.
    """
    status = getattr(error.__cause__, "http_status_code", None)
    return status is not None and (status == 429 or 500 <= status < 600)
