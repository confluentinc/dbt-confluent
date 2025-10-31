from dbt.adapters.confluentcloud.connections import ConfluentCloudConnectionManager # noqa
from dbt.adapters.confluentcloud.connections import ConfluentCloudCredentials
from dbt.adapters.confluentcloud.impl import ConfluentCloudAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import confluentcloud


Plugin = AdapterPlugin(
    adapter=ConfluentCloudAdapter,
    credentials=ConfluentCloudCredentials,
    include_path=confluentcloud.PACKAGE_PATH
    )
