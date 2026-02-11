import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import confluent_sql
from confluent_sql.execution_mode import ExecutionMode
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import (
    ConnectionError,
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
    ConnectionUsed,
    SQLQuery,
    SQLQueryStatus,
)
from dbt.adapters.sql import SQLConnectionManager

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
    host: str
    cloud_provider: str
    cloud_region: str
    compute_pool_id: str
    organization_id: str
    flink_api_key: str
    flink_api_secret: str
    execution_mode: ExecutionMode = ExecutionMode.STREAMING_QUERY

    ALIASES = {"environment_id": "database", "dbname": "schema"}

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
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "database")


class ConfluentConnectionManager(SQLConnectionManager):
    TYPE = "confluent"

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
        execution_mode: str | None = None,
    ) -> tuple[AdapterResponse, "agate.Table"]:
        """This is customized so we can pass execution_mode down the chain."""
        from dbt_common.clients.agate_helper import empty_table

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin, execution_mode=execution_mode)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor, limit)
        else:
            table = empty_table()
        return response, table

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Any | None = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: tuple[type[Exception], ...] = (),
        retry_limit: int = 1,
        execution_mode: str | None = None,
    ) -> tuple[Connection, Any]:
        """
        Copied from upstream (in SqlConnectionManager) with handling of cursor's
        execution_mode. ExecutionMode can be specified at the project level in credentials,
        or as a node info in config blocks.
        """

        def _execute_query_with_retry(
            cursor: Any,
            sql: str,
            bindings: Any | None,
            retryable_exceptions: tuple[type[Exception], ...],
            retry_limit: int,
            attempt: int,
        ):
            """
            A success sees the try exit cleanly and avoid any recursive
            retries. Failure begins a sleep and retry routine.
            """
            try:
                cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                # Cease retries and fail when limit is hit.
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        base_msg=f"Got a retryable error {type(e)}. {retry_limit - attempt} retries left. "
                        f"Retrying in 1 second.\nError:\n{e}"
                    )
                )
                time.sleep(1)

                return _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    retry_limit=retry_limit,
                    attempt=attempt + 1,
                )

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

            cursor = connection.handle.cursor(mode=resolved_mode)
            _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
                attempt=1,
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
        except confluent_sql.Error as e:
            # TODO: Use logger, or fire a dbt event? Or both?
            msg = f"confluent_sql error for '{sql}': {e}"
            logger.debug(msg)
            raise DbtDatabaseError(msg) from e
        except Exception as e:
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
            # TODO: Use logger, or fire a dbt event? Or both?
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        # logging.basicConfig(level=logging.INFO, force=True)
        # logging.getLogger().handlers[0].addFilter(logging.Filter("confluent_sql"))

        try:
            handle = confluent_sql.connect(
                flink_api_key=credentials.flink_api_key,
                flink_api_secret=credentials.flink_api_secret,
                environment=credentials.database,
                compute_pool_id=credentials.compute_pool_id,
                organization_id=credentials.organization_id,
                cloud_provider=credentials.cloud_provider,
                cloud_region=credentials.cloud_region,
                dbname=credentials.schema,
            )
            connection.state = "open"
            connection.handle = handle
            return connection
        except Exception as e:
            connection.state = "fail"
            connection.handle = None
            raise ConnectionError("confluent_sql connection error") from e

    @classmethod
    def get_response(cls, cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse object
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        assert cursor._statement is not None, "Cursor has no active statement"
        return AdapterResponse(f"{cursor._statement.phase}")

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.handle.close()

    def commit(self):
        # Confluent cloud SQL does not support transactions, so commit is a noop here.
        # TODO: Should we raise an exception if a non supported feature is used instead?
        pass

    def begin(self):
        # Confluent cloud SQL does not support transactions, so begin is a noop here.
        # TODO: Should we raise an exception if a non supported feature is used instead?
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
