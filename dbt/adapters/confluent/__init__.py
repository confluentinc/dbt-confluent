from dbt.adapters.base import AdapterPlugin
from dbt.adapters.confluent.connections import (
    ConfluentConnectionManager,
    ConfluentCredentials,
)
from dbt.adapters.confluent.impl import ConfluentAdapter
from dbt.include import confluent

__all__ = ["Plugin", "ConfluentConnectionManager"]

Plugin = AdapterPlugin(
    adapter=ConfluentAdapter,
    credentials=ConfluentCredentials,
    include_path=confluent.PACKAGE_PATH,
)
