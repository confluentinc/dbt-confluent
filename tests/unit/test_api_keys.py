"""The functional tests' IAM/CMK helpers, against a mocked Confluent Cloud API."""

import httpx
import pytest

from tests.functional.adapter._api_keys import resolve_kafka_cluster_id

BASE_URL = "https://api.confluent.cloud"
ENVIRONMENT = "env-abc123"
CLUSTER_NAME = "my_cluster"
SECOND_PAGE_TOKEN = "page-2"


def cluster(cluster_id: str, name: str) -> dict:
    return {"id": cluster_id, "spec": {"display_name": name}}


def clients_listing(first_page: list[dict], second_page: list[dict]) -> httpx.Client:
    """A client for an API that lists clusters over two pages, linked by `metadata.next`."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page_token") == SECOND_PAGE_TOKEN:
            return httpx.Response(200, json={"data": second_page, "metadata": {}})
        next_url = f"{BASE_URL}/cmk/v2/clusters?page_token={SECOND_PAGE_TOKEN}"
        return httpx.Response(200, json={"data": first_page, "metadata": {"next": next_url}})

    return httpx.Client(base_url=BASE_URL, transport=httpx.MockTransport(handler))


class TestResolveKafkaClusterId:
    def test_finds_a_cluster_on_a_later_page(self):
        client = clients_listing(
            first_page=[cluster("lkc-first", "other")],
            second_page=[cluster("lkc-wanted", CLUSTER_NAME)],
        )

        assert resolve_kafka_cluster_id(client, ENVIRONMENT, CLUSTER_NAME) == "lkc-wanted"

    def test_a_duplicate_name_on_a_later_page_is_an_error(self):
        client = clients_listing(
            first_page=[cluster("lkc-one", CLUSTER_NAME)],
            second_page=[cluster("lkc-two", CLUSTER_NAME)],
        )

        with pytest.raises(LookupError, match="lkc-one.*lkc-two"):
            resolve_kafka_cluster_id(client, ENVIRONMENT, CLUSTER_NAME)

    def test_no_such_cluster_is_an_error(self):
        client = clients_listing(first_page=[], second_page=[cluster("lkc-x", "other")])

        with pytest.raises(LookupError, match="expected one"):
            resolve_kafka_cluster_id(client, ENVIRONMENT, CLUSTER_NAME)
