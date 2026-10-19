"""Unit tests for ConfluentCredentials profile parsing."""

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.confluent.connections import ConfluentCredentials

BASE_PROFILE = {"database": "env-123", "schema": "lkc-123", "organization_id": "org-123"}


class TestResourceNamePrefix:
    def test_defaults_to_dbt(self):
        credentials = ConfluentCredentials.from_dict(BASE_PROFILE)
        assert credentials.resource_name_prefix == "dbt-"

    def test_custom_value(self):
        credentials = ConfluentCredentials.from_dict(
            {**BASE_PROFILE, "resource_name_prefix": "eng-a-"}
        )
        assert credentials.resource_name_prefix == "eng-a-"

    @pytest.mark.parametrize(
        "legacy_keys",
        [
            {"statement_name_prefix": "x-"},
            {"statement_name_prefix": "x-", "resource_name_prefix": "eng-a-"},
        ],
    )
    def test_statement_name_prefix_rejected(self, legacy_keys):
        with pytest.raises(DbtRuntimeError, match="use `resource_name_prefix` instead"):
            ConfluentCredentials.from_dict({**BASE_PROFILE, **legacy_keys})
