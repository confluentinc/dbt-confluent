"""dbt-core validates the adapter version as strict SemVer at registration
time (`dbt.adapters.factory._validate_version` ->
`dbt_common.semver.VersionSpecifier.from_version_string`), rejecting anything
that isn't, e.g. PEP 440 dev-release syntax like `0.4.0.dev0`. That check
lives entirely in dbt-core/dbt-common, so nothing in this repo would catch a
bad version string short of actually running `dbt` -- this test exercises
the same validation directly against our own version constant.
"""

from dbt_common.semver import VersionSpecifier

from dbt.adapters.confluent.__version__ import version


def test_version_is_valid_semver():
    VersionSpecifier.from_version_string(version)
