import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Any

import confluent_sql
import dbt_common
from dbt.adapters.contracts.connection import Credentials, Connection, AdapterResponse
from dbt.adapters.events.types import ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.utils import cast_to_str

import dbt


logger = logging.getLogger(__name__)


@dataclass
class ConfluentCloudCredentials(Credentials):
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

    ALIASES = {"environment_id": "database", "dbname": "schema"}

    @property
    def type(self):
        """Return name of adapter."""
        return "confluentcloud"

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


class ConfluentCloudConnectionManager(SQLConnectionManager):
    TYPE = "confluentcloud"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        try:
            yield
        except confluent_sql.Error as e:
            logger.debug(f"confluent_sql error: {e}")
            raise dbt_common.exceptions.DbtDatabaseError(str(e))
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            raise dbt_common.exceptions.DbtRuntimeError(str(e))

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

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
            raise dbt_common.exceptions.ConnectionError(f"{e}")

    @classmethod
    def get_response(cls, cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        assert cursor._statement is not None, "Cursor has no active statement"
        return AdapterResponse(f"{cursor._statement.phase}")

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        connection.close()

    def commit(self):
        # Confluent cloud SQL does not support transactions, so commit is a noop here.
        pass

    def begin(self):
        # Confluent cloud SQL does not support transactions, so begin is a noop here.
        pass

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> tuple[Connection, Any]:
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
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(
                SQLQuery(
                    conn_name=cast_to_str(connection.name), sql=log_sql, node_info=get_node_info()
                )
            )
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round((time.time() - pre)),
                    node_info=get_node_info(),
                )
            )

            return connection, cursor
