import logging
import time
import uuid
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import confluent_sql
from confluent_sql import HIDDEN_LABEL, Cursor
from confluent_sql.exceptions import (
    ComputePoolExhaustedError,
    OperationalError,
    StatementNotFoundError,
)
from confluent_sql.execution_mode import ExecutionMode
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import (
    DbtDatabaseError,
    DbtRuntimeError,
)
from dbt_common.utils import cast_to_str

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.types import (
    AdapterEventDebug,
    AdapterEventWarning,
    ConnectionUsed,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.sql import SQLConnectionManager

from .__version__ import version
from .utils import fetch_from_cursor

if TYPE_CHECKING:
    import agate

logger = logging.getLogger(__name__)


@dataclass
class ConfluentCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Add credentials members here, like:
    organization_id: str
    # API credentials:
    # - flink_api_key / flink_api_secret: Flink-region key, used for all Flink SQL
    #   statement operations. Required — since `open` no longer forwards the global
    #   pair to confluent_sql's own `global_api_key`, a profile supplying only the
    #   global pair fails at connect() with confluent_sql's "Either ... must be
    #   provided". (Before that rewiring a global-only profile did connect.)
    # - global_api_key / global_api_secret: a Tableflow-capable key, forwarded as
    #   tableflow_api_key/tableflow_api_secret and therefore used only for Tableflow
    #   control-plane routes. Deliberately never takes over Flink SQL auth even when
    #   it's the only pair given. Scope matters here — each route is served by a
    #   backend that accepts only its own key type (verified against a live org):
    #
    #       key type         Flink SQL   /tableflow/v1   /cmk/v2
    #       flink (region)      200          401           401
    #       tableflow           404          200           401
    #       cloud               404          401           200
    #       global              200          200           200
    #
    #   So use `resource_type=global`: Tableflow also needs the Kafka cluster id,
    #   which the driver resolves from `schema` (the cluster's display name) via
    #   /cmk/v2 — a route a `tableflow` key can't reach. A `resource_type=cloud`
    #   key does NOT work for Tableflow — it 401s, which is the one failure that
    #   looks like a credential problem but is really a scope problem.
    global_api_key: str | None = None
    global_api_secret: str | None = None
    flink_api_key: str | None = None
    flink_api_secret: str | None = None
    # Optional ("poolless"): when omitted, Confluent Cloud Flink runs statements
    # in the environment+region default compute pool (provisioning if necessary).
    compute_pool_id: str | None = None
    cloud_provider: str | None = None
    cloud_region: str | None = None
    endpoint: str | None = None
    execution_mode: ExecutionMode = ExecutionMode.STREAMING_QUERY
    statement_name_prefix: str = "dbt-"
    statement_label: str = "dbt-confluent"

    _ALIASES = {"environment_id": "database", "dbname": "schema"}

    @property
    def type(self):
        """Return name of adapter."""
        return "confluent"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.endpoint or f"{self.cloud_provider}-{self.cloud_region}-{self.organization_id}"

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        keys = ("organization_id", "database", "schema", "compute_pool_id")
        if self.endpoint:
            return (*keys, "endpoint")
        else:
            return (*keys, "cloud_provider", "cloud_region")


def _execute_query_with_retry(
    cursor: "confluent_sql.Cursor",
    sql: str,
    bindings: Any | None,
    retryable_exceptions: tuple[type[Exception], ...],
    retry_limit: int,
    attempt: int,
    statement_name: str | None = None,
    statement_labels: list[str] | None = None,
    compute_pool_id: str | None = None,
    statement_properties: dict[str, str | int | bool] | None = None,
) -> None:
    """Execute the cursor and retry on transient failures.

    Iterates instead of recursing so that the call stack stays flat and
    all mutable state (``attempt``, ``limit``) is carried as loop variables.
    The extended 12-retry budget for slow transient conditions is therefore
    correctly accumulated across iterations rather than being re-applied
    per-frame.

    Lives at module scope (not as a closure inside add_query) so it can
    be unit-tested in isolation.

    compute_pool_id: if provided, overrides the connection-default compute pool
    for this statement (per-model ``compute_pool_id`` config). If None, the
    connection's default compute pool is used.

    statement_properties: if provided (per-model ``statement_properties`` config),
    passed through to the driver as Flink SET-style statement properties.
    """
    limit = retry_limit
    while True:
        try:
            cursor.execute(
                sql,
                bindings,
                statement_name=statement_name,
                statement_labels=statement_labels,
                compute_pool_id=compute_pool_id,
                properties=statement_properties,
            )
            return  # success — exit the loop
        except retryable_exceptions as e:
            # Cease retries and fail when limit is hit.
            if attempt >= limit:
                raise

            backoff = min(attempt * 3, 15)
            retries_left = limit - attempt

            if isinstance(e, ComputePoolExhaustedError):
                fire_event(
                    AdapterEventWarning(
                        base_msg=f"Compute pool exhausted. {retries_left} retries left. "
                        f"Retrying in {backoff} seconds."
                    )
                )
            else:
                fire_event(
                    AdapterEventDebug(
                        base_msg=f"Got a retryable error {type(e)}. {retries_left} retries left. "
                        f"Retrying in {backoff} seconds.\nError:\n{e}"
                    )
                )
            time.sleep(backoff)
            # Reuse the same statement name on retry. ComputePoolExhaustedError
            # cleans up the failed statement, so the name is available for reuse.
            attempt += 1

        except OperationalError as e:
            # Three transient conditions we wait out by retrying:
            #  - "being modified": a materialized table's prior CREATE OR ALTER is
            #    still establishing/evolving; it always settles on its own.
            #  - "kafka topic does not exist": a recreate found the dying catalog
            #    entry of a recently dropped relation; it clears within tens of
            #    seconds.
            #  - 409: a prior statement with the same name is still tearing down
            #    asynchronously after a DELETE.
            # "table already exists" is deliberately NOT retried: it never clears
            # by waiting, and retrying would delay every genuine name conflict by
            # the whole budget.
            # Message matches are checked before the status: Confluent may surface
            # them with a 409 too, and keying off the status would misreport (and
            # mis-budget) them as a name-reuse race.
            msg = str(e).lower()
            if "table already exists" in msg:
                raise
            if "schema registry subject" in msg and (
                "doesn't match" in msg or "does not match" in msg
            ):
                # Not retried: a dropped relation's Schema Registry subjects are
                # not deleted with it, so this never clears by waiting. Typical
                # cause: recreating a dropped relation under the same name with a
                # differently-shaped schema — e.g. replacing a materialized table
                # (keyed schema) with a `table` model's snapshot CTAS (keyless).
                # The raw server message is cryptic, so append recovery guidance.
                raise OperationalError(
                    f"{e}\nA Schema Registry subject registered by a previously "
                    f"dropped relation with this name still exists and is "
                    f"incompatible with the schema this statement would register "
                    f"(subjects are not deleted when a relation is dropped). "
                    f"Either delete the lingering subject(s) in Schema Registry "
                    f"and re-run, or give the model a different relation name "
                    f"(e.g. via an alias)."
                ) from e
            is_being_modified = "being modified" in msg
            is_topic_gone = "kafka topic does not exist" in msg
            is_409 = getattr(e, "http_status_code", None) == 409
            if not (is_being_modified or is_topic_gone or is_409):
                raise

            # The message-matched conditions are slow and variable to clear, so
            # they get a generous dedicated budget accumulated across iterations;
            # the 409 name-reuse race keeps the smaller default one.
            if is_being_modified:
                limit = max(limit, 12)
                backoff = 10
                reason = (
                    "Materialized table is still being modified by a prior statement "
                    "(still establishing/evolving)"
                )
            elif is_topic_gone:
                limit = max(limit, 12)
                backoff = 10
                reason = (
                    "A recently dropped relation with this name has not finished "
                    "tearing down (its Kafka topic is already gone)"
                )
            else:
                backoff = min(attempt * 3, 15)
                reason = (
                    f"Statement name '{statement_name}' is already in use "
                    f"(prior statement may still be tearing down)"
                )
            if attempt >= limit:
                raise

            # A rejection that reached the FAILED phase leaves the statement in
            # place, still occupying statement_name — without this delete the
            # retry would bounce off 409 name conflicts instead of seeing the
            # condition clear. Best-effort: after an HTTP-level rejection no
            # statement exists, and on budget exhaustion (raise above) the FAILED
            # statement is left in place for debugging.
            try:
                cursor.delete_statement()
            except Exception as cleanup_error:  # noqa: BLE001
                fire_event(
                    AdapterEventDebug(
                        base_msg=f"Could not delete failed statement "
                        f"'{statement_name}' before retrying: {cleanup_error}"
                    )
                )

            retries_left = limit - attempt
            fire_event(
                AdapterEventDebug(
                    base_msg=f"{reason}. {retries_left} retries left. Retrying in {backoff} seconds."
                )
            )
            time.sleep(backoff)
            attempt += 1


class ConfluentConnectionManager(SQLConnectionManager):
    TYPE = "confluent"

    def get_thread_handle(self) -> confluent_sql.Connection:
        """Typed accessor for the current thread's live `confluent_sql.Connection`.

        `Connection.handle` (dbt-adapters) is untyped (`Any`), since it's generic across
        every adapter's own driver -- this is the one place that gets narrowed to
        `confluent_sql`'s actual `Connection`, so callers elsewhere don't each need their
        own annotation on the same untyped attribute access.
        """
        return self.get_thread_connection().handle

    @classmethod
    def get_result_from_cursor(cls, cursor: Cursor, limit: int | None) -> "agate.Table":
        from dbt_common.clients.agate_helper import table_from_data_flat

        data: Iterable[Any] = []
        column_names: list[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            rows = fetch_from_cursor(cursor, limit)
            data = cls.process_results(column_names, rows)

        return table_from_data_flat(data, column_names)

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
        execution_mode: str | None = None,
        hidden: bool = False,
        statement_name: str | None = None,
        compute_pool_id: str | None = None,
        statement_properties: dict[str, str | int | bool] | None = None,
    ) -> tuple[AdapterResponse, "agate.Table"]:
        """This is customized so we can pass execution_mode, hidden, statement_name,
        compute_pool_id and statement_properties down the chain."""
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(
            sql,
            auto_begin,
            execution_mode=execution_mode,
            statement_name=statement_name,
            hidden=hidden,
            compute_pool_id=compute_pool_id,
            statement_properties=statement_properties,
        )
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            cursor.close()
            table = empty_table()
        return response, table

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Any | None = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: tuple[type[Exception], ...] = (ComputePoolExhaustedError,),
        retry_limit: int = 5,
        execution_mode: str | None = None,
        hidden: bool = False,
        statement_name: str | None = None,
        compute_pool_id: str | None = None,
        statement_properties: dict[str, str | int | bool] | None = None,
    ) -> tuple[Connection, Any]:
        """
        Copied from upstream (in SqlConnectionManager) with handling of cursor's
        execution_mode, hidden label and statement_name. ExecutionMode can be specified at
        the project level in credentials, or as a node info in config blocks.

        statement_name: if provided, used as the Flink statement name (deterministic).
        If None, a UUID-based name is generated (for metadata/schema queries).

        compute_pool_id: if provided (per-model `compute_pool_id` config), overrides the
        connection-default compute pool for this statement. If None, the connection's
        default compute pool (from credentials) is used.

        statement_properties: if provided (per-model `statement_properties` config),
        passed through to the driver as Flink SET-style statement properties.
        """
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(
            ConnectionUsed(
                conn_type=self.TYPE,
                conn_name=cast_to_str(connection.name),
                node_info=get_node_info(),
            )
        )

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = f"{sql[:512]}..."
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name),
                    sql=log_sql,
                    node_info=get_node_info(),
                )
            )

            pre = time.perf_counter()

            if execution_mode:
                resolved_mode = ExecutionMode(execution_mode)
            else:
                resolved_mode = ExecutionMode(connection.credentials.execution_mode)

            labels = [connection.credentials.statement_label]
            if hidden:
                labels.append(HIDDEN_LABEL)

            # Use deterministic name if provided, otherwise fall back to UUID
            if statement_name is None:
                prefix = connection.credentials.statement_name_prefix
                statement_name = f"{prefix}{uuid.uuid4()}"

            cursor = connection.handle.cursor(mode=resolved_mode)
            _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
                attempt=1,
                statement_name=statement_name,
                statement_labels=labels,
                compute_pool_id=compute_pool_id,
                statement_properties=statement_properties,
            )

            result = self.get_response(cursor)

            fire_event(
                SQLQueryStatus(
                    status=str(result),
                    elapsed=time.perf_counter() - pre,
                    node_info=get_node_info(),
                    query_id=result.query_id,
                )
            )

            return connection, cursor

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield
        except StatementNotFoundError as e:
            msg = f"Statement '{e.statement_name}' not found for '{sql}': {e}"
            logger.debug(msg)
            raise DbtDatabaseError(msg) from e
        except confluent_sql.Error as e:
            # confluent_sql.Error is the public base for all driver errors; we
            # intentionally catch the full hierarchy here so that any new
            # driver-level subclass is reported as a DbtDatabaseError (a dbt
            # operational failure) rather than a generic DbtRuntimeError.
            # fire_event is deliberately not used: these errors are query-level
            # failures best surfaced through the standard dbt exception path.
            msg = f"confluent_sql error for '{sql}': {e}"
            logger.debug(msg)
            raise DbtDatabaseError(msg) from e
        except Exception as e:
            # Catch-all for unexpected non-driver errors (e.g. network stack,
            # serialisation). Kept broad on purpose: we cannot enumerate every
            # possible infrastructure exception, and dbt expects all query
            # failures to be wrapped in a DbtRuntimeError so its error-handling
            # machinery can present them consistently.
            msg = f"Error running SQL '{sql}': {e}"
            logger.debug(msg)
            raise DbtRuntimeError(msg) from e

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state is ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            # This is hardcoded here as we don't want this to be customizable
            user_agent = f"Confluent-dbt/v{version}"

            handle = confluent_sql.connect(
                global_api_key=credentials.global_api_key,
                global_api_secret=credentials.global_api_secret,
                flink_api_key=credentials.flink_api_key,
                flink_api_secret=credentials.flink_api_secret,
                environment_id=credentials.database,
                compute_pool_id=credentials.compute_pool_id,
                organization_id=credentials.organization_id,
                cloud_provider=credentials.cloud_provider,
                cloud_region=credentials.cloud_region,
                endpoint=credentials.endpoint,
                database=credentials.schema,
                http_user_agent=user_agent,
                # INFORMATION_SCHEMA queries (especially the unified UNION ALL drift
                # catalog) routinely take longer than the default 5s timeout on cold
                # metadata lookups, surfacing as a "read operation timed out".
                http_timeout_secs=60,
            )
            connection.state = ConnectionState.OPEN
            connection.handle = handle
            return connection
        except confluent_sql.Error:
            # confluent_sql.connect() raises confluent_sql.Error (or a subclass)
            # for all connection-level failures (bad credentials, unreachable
            # endpoint, invalid configuration). We mark the connection as failed
            # and re-raise so dbt can present the original driver message.
            connection.state = ConnectionState.FAIL
            connection.handle = None
            raise
        except Exception:
            # Unexpected non-driver failure (e.g. import error, misconfigured
            # proxy). Mark failed and re-raise with original diagnostics intact.
            connection.state = ConnectionState.FAIL
            connection.handle = None
            raise

    @classmethod
    def get_response(cls, cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse object
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        assert cursor.statement is not None, "Cursor has no active statement"
        return AdapterResponse(f"{cursor._statement.phase}")

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.handle.close()

    def commit(self):
        # Confluent Cloud SQL does not support transactions. Silently no-op so
        # dbt's generic transaction machinery (which calls begin/commit around
        # every query) does not break. Raising here would break every query.
        pass

    def begin(self):
        # Confluent Cloud SQL does not support transactions. Silently no-op so
        # dbt's generic transaction machinery (which calls begin/commit around
        # every query) does not break. Raising here would break every query.
        pass

    @classmethod
    def data_type_code_to_name(cls, type_code: int | str) -> str:
        """
        Get the string representation of the data type from the type code.

        Flink SQL returns type names like:
        - ARRAY<STRING> → ARRAY
        - MAP<INT, STRING> → MAP
        - DECIMAL(10, 2) → DECIMAL
        - ROW<field1 INT, field2 STRING> → ROW

        This method extracts the base type name by removing type parameters.
        """
        if isinstance(type_code, int):
            # Confluent SQL library returns string type names, not numeric codes
            # If we somehow get a numeric code, convert it to string
            type_code = str(type_code)

        # Remove generic type parameters (e.g., ARRAY<STRING> → ARRAY)
        # and precision/scale parameters (e.g., DECIMAL(10,2) → DECIMAL)
        base_type = type_code.split("(")[0].split("<")[0].strip().upper()

        return base_type
