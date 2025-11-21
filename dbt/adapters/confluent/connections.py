import logging
from contextlib import contextmanager
from dataclasses import dataclass

import confluent_sql
from dbt_common.exceptions import ConnectionError, DbtDatabaseError, DbtRuntimeError

from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.sql import SQLConnectionManager

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
            msg = "confluent_sql error"
            logger.debug(f"{msg}: {e}")
            raise DbtDatabaseError(msg) from e
        except Exception as e:
            msg = "Error running SQL"
            logger.debug(f"{msg}: {sql}")
            raise DbtRuntimeError(msg) from e

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        if connection.state == "open":
            # TODO: Use logger, or fire a dbt event? Or both?
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
        connection.close()

    def commit(self):
        # Confluent cloud SQL does not support transactions, so commit is a noop here.
        # TODO: Should we raise an exception if a non supported feature is used instead?
        pass

    def begin(self):
        # Confluent cloud SQL does not support transactions, so begin is a noop here.
        # TODO: Should we raise an exception if a non supported feature is used instead?
        pass
