"""Create, delete and sweep the scoped API keys the credential tests mint at runtime.

Uses Confluent's IAM API, authenticated with the suite's Global API key. Keys are owned by
whoever owns that Global key, and carry a reserved display name (`dbt-adapter-test-<resource>-
<hex epoch>`) so the sweep can tell them from anyone else's keys.

Underscore-prefixed so pytest does not treat it as a test module.
"""

import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import httpx

from tests.functional.adapter._helpers import SWEEP_MIN_AGE_SECONDS

BASE_URL = "https://api.confluent.cloud"
KEY_NAME_PREFIX = "dbt-adapter-test-"

Resource = Literal["cloud", "tableflow"]
RESOURCES: tuple[Resource, ...] = ("cloud", "tableflow")

# The only keys the sweep may delete: exactly the shape `create_api_key` names its keys.
_KEY_NAME_RE = re.compile(rf"^{KEY_NAME_PREFIX}(?:cloud|tableflow)-(?P<tag>[0-9a-f]{{8}})$")


@dataclass(frozen=True)
class ApiKey:
    id: str
    secret: str


def iam_client(global_key: str, global_secret: str) -> httpx.Client:
    return httpx.Client(base_url=BASE_URL, auth=(global_key, global_secret), timeout=30)


def _owner_id(client: httpx.Client, key_id: str) -> str:
    """The user (or service account) that owns `key_id`: the owner of every key we create."""
    response = client.get(f"/iam/v2/api-keys/{key_id}")
    response.raise_for_status()
    return response.json()["spec"]["owner"]["id"]


def create_api_key(
    client: httpx.Client, owner_key_id: str, resource: Resource, tag: str
) -> ApiKey:
    """A new key for `resource`, owned by the owner of `owner_key_id`, named for the sweep."""
    response = client.post(
        "/iam/v2/api-keys",
        json={
            "spec": {
                "display_name": f"{KEY_NAME_PREFIX}{resource}-{tag}",
                "description": "Temporary key made by dbt-confluent's functional tests",
                "owner": {"id": _owner_id(client, owner_key_id)},
                "resource": {"id": resource},
            }
        },
    )
    response.raise_for_status()
    body = response.json()
    return ApiKey(id=body["id"], secret=body["spec"]["secret"])


def delete_api_key(client: httpx.Client, key_id: str) -> None:
    """Delete a key; one that is already gone is fine."""
    response = client.delete(f"/iam/v2/api-keys/{key_id}")
    if response.status_code != httpx.codes.NOT_FOUND:
        response.raise_for_status()


def sweep_stale_api_keys(
    client: httpx.Client, owner_key_id: str, min_age: float = SWEEP_MIN_AGE_SECONDS
) -> None:
    """Delete test keys leaked by earlier sessions (a crashed run never reaches teardown).

    Only touches keys that are the suite's own: owned by the same user as `owner_key_id`, for
    the cloud or tableflow resource, and named exactly as `create_api_key` names them. Keys
    younger than `min_age` may belong to a session still running, so they are left alone.
    """
    owner = _owner_id(client, owner_key_id)
    now = time.time()
    for resource in RESOURCES:
        url: str | None = "/iam/v2/api-keys"
        params: dict[str, str | int] | None = {
            "spec.owner": owner,
            "spec.resource": resource,
            "page_size": 100,
        }
        while url:
            response = client.get(url, params=params)
            response.raise_for_status()
            page = response.json()
            for key in page["data"]:
                if not _KEY_NAME_RE.match(key["spec"].get("display_name") or ""):
                    continue
                created_at = datetime.fromisoformat(key["metadata"]["created_at"]).timestamp()
                if now - created_at >= min_age:
                    delete_api_key(client, key["id"])
            # `next` is an absolute URL that already carries the query string.
            url, params = page["metadata"].get("next"), None


def resolve_kafka_cluster_id(client: httpx.Client, environment_id: str, name: str) -> str:
    """The `lkc-...` id of the Kafka cluster named `name` in the environment."""
    response = client.get(
        "/cmk/v2/clusters", params={"environment": environment_id, "page_size": 100}
    )
    response.raise_for_status()
    ids = [c["id"] for c in response.json()["data"] if c["spec"]["display_name"] == name]
    if len(ids) != 1:
        raise LookupError(f"expected one Kafka cluster named {name!r}, found {ids}")
    return ids[0]
