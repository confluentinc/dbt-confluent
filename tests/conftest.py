import pytest

import os
# import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    flink_api_key = os.getenv("CONFLUENT_FLINK_API_KEY")
    flink_api_secret = os.getenv("CONFLUENT_FLINK_API_SECRET")
    environment = os.getenv("CONFLUENT_ENV_ID")
    organization_id = os.getenv("CONFLUENT_ORG_ID")
    compute_pool_id = os.getenv("CONFLUENT_COMPUTE_POOL_ID")
    cloud_provider = os.getenv("CONFLUENT_CLOUD_PROVIDER")
    cloud_region = os.getenv("CONFLUENT_CLOUD_REGION")
    dbname = os.getenv("CONFLUENT_TEST_DBNAME")
    host = f"https://flink.{cloud_region}.{cloud_provider}.confluent.cloud"
    target = {
        "type": "confluentcloud",
        "threads": 1,
        "host": host,
        "cloud_provider": cloud_provider,
        "cloud_region": cloud_region,
        "compute_pool_id": compute_pool_id,
        "organization_id": organization_id,
        "flink_api_key": flink_api_key,
        "flink_api_secret": flink_api_secret,
        "database": environment,
        "schema": dbname,
        "test_schema": dbname,
    }
    print(target)
    return target
