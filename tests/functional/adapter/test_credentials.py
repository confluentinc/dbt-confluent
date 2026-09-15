"""Preflight checks for the API keys in test.env.

Every other functional test needs working credentials, and a wrong one fails deep
inside a dbt run with an error that rarely names the real problem -- a `401` from
Tableflow reads as a bad secret when it actually means the key has the wrong
*scope*. These tests probe each key pair directly, one cheap authenticated GET
each, so a credential problem is reported as a credential problem.

Scopes are not interchangeable; each backend accepts only its own key type
(verified live). This is what the probes below encode:

    confluent api-key create --resource   Flink SQL   /tableflow/v1   /cmk/v2
    flink (with --cloud/--region)            200          401           401
    tableflow                                404          200           401
    cloud                                    404          401           200
    global                                   200          200           200

Each probe skips when its key pair (or the ids its URL needs) isn't set, so the
optional keys `bootstrap.sh` writes cost nothing when absent.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass

import httpx
import pytest

_TIMEOUT = 30.0


@dataclass(frozen=True)
class _Probe:
    """One key pair, the cheapest authenticated GET that proves it works, and the
    scope mistake a failure most likely means."""

    id: str
    key_var: str
    secret_var: str
    # Built lazily: reading the ids at module import time would bake in whatever
    # was set before pytest-dotenv loaded test.env.
    url: Callable[[], str]
    # Env vars the URL needs beyond the key pair itself.
    needs: tuple[str, ...]
    hint: str


def _env(name: str) -> str:
    return os.environ[name]


_PROBES = (
    _Probe(
        id="flink-sql",
        key_var="CONFLUENT_FLINK_API_KEY",
        secret_var="CONFLUENT_FLINK_API_SECRET",
        needs=(
            "CONFLUENT_ORG_ID",
            "CONFLUENT_ENV_ID",
            "CONFLUENT_CLOUD_PROVIDER",
            "CONFLUENT_CLOUD_REGION",
        ),
        url=lambda: (
            f"https://flink.{_env('CONFLUENT_CLOUD_REGION')}.{_env('CONFLUENT_CLOUD_PROVIDER')}"
            f".confluent.cloud/sql/v1/organizations/{_env('CONFLUENT_ORG_ID')}"
            f"/environments/{_env('CONFLUENT_ENV_ID')}/statements?page_size=1"
        ),
        hint=(
            "must be a Flink *region* key: "
            "`confluent api-key create --resource flink --cloud <p> --region <r>`. "
            "A cloud- or tableflow-scoped key answers 404 here, not 401, so the "
            "failure doesn't even look like an auth problem"
        ),
    ),
    _Probe(
        id="tableflow",
        key_var="CONFLUENT_GLOBAL_API_KEY",
        secret_var="CONFLUENT_GLOBAL_API_SECRET",
        # `spec.kafka_cluster` isn't optional on this route -- omitting it is a
        # 400, which would read as a credential failure when it isn't one.
        needs=("CONFLUENT_ENV_ID", "CONFLUENT_KAFKA_CLUSTER_ID"),
        url=lambda: (
            "https://api.confluent.cloud/tableflow/v1/tableflow-topics"
            f"?environment={_env('CONFLUENT_ENV_ID')}"
            f"&spec.kafka_cluster={_env('CONFLUENT_KAFKA_CLUSTER_ID')}"
        ),
        hint=(
            "must be `--resource global` (or `--resource tableflow`). "
            "A `--resource cloud` key is rejected with a bare 401 even though it "
            "is a perfectly valid key elsewhere"
        ),
    ),
    _Probe(
        # Same key as above, second route: Tableflow also has to resolve the
        # `lkc-…` id from the cluster display name, and that lookup is CMK's.
        # This is the probe that separates a `global` key from a `tableflow` one.
        id="cmk-cluster-lookup",
        key_var="CONFLUENT_GLOBAL_API_KEY",
        secret_var="CONFLUENT_GLOBAL_API_SECRET",
        needs=("CONFLUENT_ENV_ID",),
        url=lambda: (
            f"https://api.confluent.cloud/cmk/v2/clusters?environment={_env('CONFLUENT_ENV_ID')}"
        ),
        hint=(
            "must be `--resource global`: a `--resource tableflow` key reaches "
            "Tableflow but gets 401 here, so the cluster-id lookup can't be made"
        ),
    ),
    _Probe(
        id="kafka-cluster",
        key_var="CONFLUENT_KAFKA_API_KEY",
        secret_var="CONFLUENT_KAFKA_API_SECRET",
        needs=("CONFLUENT_BOOTSTRAP_SERVERS", "CONFLUENT_KAFKA_CLUSTER_ID"),
        # Confluent Cloud serves the Kafka REST API on the bootstrap host over 443.
        url=lambda: (
            f"https://{_env('CONFLUENT_BOOTSTRAP_SERVERS').split(':')[0]}"
            f"/kafka/v3/clusters/{_env('CONFLUENT_KAFKA_CLUSTER_ID')}/topics"
        ),
        hint="must be a key scoped to this `lkc-…` cluster",
    ),
    _Probe(
        id="schema-registry",
        key_var="CONFLUENT_SR_API_KEY",
        secret_var="CONFLUENT_SR_API_SECRET",
        needs=("CONFLUENT_SR_URL",),
        url=lambda: f"{_env('CONFLUENT_SR_URL').rstrip('/')}/subjects",
        hint="must be a key scoped to this `lsrc-…` Schema Registry cluster",
    ),
)


@pytest.mark.parametrize("probe", _PROBES, ids=lambda p: p.id)
def test_api_key_is_accepted(probe: _Probe):
    """The key pair authenticates against the one route the suite needs it for."""
    key = os.getenv(probe.key_var)
    secret = os.getenv(probe.secret_var)
    if not (key and secret):
        pytest.skip(f"{probe.key_var}/{probe.secret_var} not set")
    missing = [name for name in probe.needs if not os.getenv(name)]
    if missing:
        pytest.skip(f"{', '.join(missing)} not set")

    url = probe.url()
    response = httpx.get(url, auth=(key, secret), timeout=_TIMEOUT)

    assert response.status_code == 200, (
        f"{probe.key_var} ({key}) got HTTP {response.status_code} from {url.split('?')[0]} "
        f"-- {probe.hint}. See README.md#configuration for the full scope matrix."
    )
