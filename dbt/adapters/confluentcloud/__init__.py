from dbt.adapters.base import AdapterPlugin
from dbt.adapters.confluentcloud.connections import (
    ConfluentCloudConnectionManager,
    ConfluentCloudCredentials,
)
from dbt.adapters.confluentcloud.impl import ConfluentCloudAdapter
from dbt.include import confluentcloud


__all__ = ["Plugin", "ConfluentCloudConnectionManager"]

Plugin = AdapterPlugin(
    adapter=ConfluentCloudAdapter,
    credentials=ConfluentCloudCredentials,
    include_path=confluentcloud.PACKAGE_PATH,
)
