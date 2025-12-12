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
    adapter=ConfluentAdapter,
    credentials=ConfluentCredentials,
    include_path=confluent.PACKAGE_PATH,
)
