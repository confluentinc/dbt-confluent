from dbt.adapters.base import AdapterPlugin
from dbt.adapters.confluent.column import ConfluentColumn
from dbt.adapters.confluent.connections import (
    ConfluentConnectionManager,
    ConfluentCredentials,
)
from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.include import confluent

__all__ = ["Plugin", "ConfluentConnectionManager", "ConfluentColumn"]

Plugin = AdapterPlugin(
    # dbt-core's `AdapterProtocol` is a Generic Protocol (see its own "TODO CT-211"),
    # and no concrete adapter satisfies it under strict structural checking.
    adapter=ConfluentAdapter,  # type: ignore[arg-type]
    credentials=ConfluentCredentials,
    include_path=confluent.PACKAGE_PATH,
)
