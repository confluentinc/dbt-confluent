"""Unit tests for ConfluentCredentials profile parsing."""

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.confluent.connections import ConfluentCredentials

BASE_PROFILE = {"database": "env-123", "schema": "lkc-123", "organization_id": "org-123"}


class TestResourceNamePrefix:
    def test_defaults_to_empty(self):
        credentials = ConfluentCredentials.from_dict(BASE_PROFILE)
        assert credentials.resource_name_prefix == ""

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

    @pytest.mark.parametrize("prefix", ["a", "eng-a-", "dbt-adapter-test-", "0x", "a" * 64])
    def test_valid_prefix_accepted(self, prefix):
        credentials = ConfluentCredentials.from_dict(
            {**BASE_PROFILE, "resource_name_prefix": prefix}
        )
        assert credentials.resource_name_prefix == prefix

    @pytest.mark.parametrize(
        "prefix",
        [
            "eng_a_",  # '_' is legal in topics but not in statement names
            "Alice-",  # uppercase is legal in topics but not in statement names
            "eng.a.",  # '.' is legal in topics but not in statement names
            "-eng",  # statement names must start alphanumeric
            "eng a",
            "eng/a",
        ],
    )
    def test_invalid_characters_rejected(self, prefix):
        with pytest.raises(DbtRuntimeError, match="Invalid resource_name_prefix"):
            ConfluentCredentials.from_dict({**BASE_PROFILE, "resource_name_prefix": prefix})

    def test_too_long_rejected(self):
        with pytest.raises(DbtRuntimeError, match="Invalid resource_name_prefix"):
            ConfluentCredentials.from_dict({**BASE_PROFILE, "resource_name_prefix": "a" * 65})
