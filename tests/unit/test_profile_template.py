from pathlib import Path
from unittest import mock

import click
import pytest
import yaml

from dbt.adapters.confluent.connections import ConfluentCredentials
from dbt.task.init import InitTask

PROFILE_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2] / "dbt" / "include" / "confluent" / "profile_template.yml"
)


def load_profile_template():
    with open(PROFILE_TEMPLATE_PATH) as f:
        return yaml.safe_load(f)


def _run_generate_target(prompts_section, prompt_responses):
    """Helper to run generate_target_from_input with mocked click.prompt."""
    task = InitTask.__new__(InitTask)
    with mock.patch("click.prompt", side_effect=prompt_responses):
        # Pass a fresh dict to avoid dbt-core's mutable default argument bug
        return task.generate_target_from_input(prompts_section, target={})


ENDPOINT_URL = "https://flink.us-east-2.aws.private.confluent.cloud"

# Menu positions in the template, for building prompt answers.
GLOBAL_KEY, FLINK_KEY, OAUTH = 1, 2, 3
INFER, PROVIDER_AND_REGION, CUSTOM_ENDPOINT = 1, 2, 3
FLINK_PROVIDER_AND_REGION, FLINK_CUSTOM_ENDPOINT = 1, 2  # a Flink key's menu has no INFER
SPECIFIC_POOL, DEFAULT_POOL = 1, 2


def prompt_answers(auth_answers, pool_answers=(DEFAULT_POOL,), name_prefix=""):
    """Every answer for a full `dbt init` run, in the template's prompt order. `auth_answers` is
    everything from the choice of credentials through the cloud settings."""
    return [
        *pool_answers,  # _choose_compute_pool
        "env-xyz",  # environment_id
        "my_db",  # dbname
        *auth_answers,  # _choose_api_credentials, its keys, organization_id, _choose_backend_url
        "streaming_query",  # execution_mode
        "dbt-confluent-",  # statement_name_prefix
        name_prefix,  # name_prefix
        "dbt-confluent",  # statement_label
        1,  # threads
    ]


def generate(auth_answers, **kwargs):
    template = load_profile_template()
    return _run_generate_target(template["prompts"], prompt_answers(auth_answers, **kwargs))


# (id, auth answers, expected profile entries, keys that must be absent)
SCENARIOS = [
    pytest.param(
        [
            FLINK_KEY,
            "my-key",
            "my-secret",
            "org-123",
            FLINK_PROVIDER_AND_REGION,
            "aws",
            "us-west-1",
        ],
        {
            "flink_api_key": "my-key",
            "organization_id": "org-123",
            "cloud_provider": "aws",
            "cloud_region": "us-west-1",
        },
        ["endpoint", "global_api_key", "auth"],
        id="flink-key-provider-and-region",
    ),
    pytest.param(
        [FLINK_KEY, "my-key", "my-secret", "org-123", FLINK_CUSTOM_ENDPOINT, ENDPOINT_URL],
        {"flink_api_key": "my-key", "organization_id": "org-123", "endpoint": ENDPOINT_URL},
        ["cloud_provider", "cloud_region"],
        id="flink-key-endpoint",
    ),
    pytest.param(
        [GLOBAL_KEY, "global-key", "global-secret", "", INFER],
        {"global_api_key": "global-key", "global_api_secret": "global-secret"},
        ["flink_api_key", "cloud_provider", "cloud_region", "endpoint"],
        id="global-key-everything-inferred",
    ),
    pytest.param(
        [GLOBAL_KEY, "global-key", "global-secret", "org-123", PROVIDER_AND_REGION, "gcp", "x"],
        {"organization_id": "org-123", "cloud_provider": "gcp", "cloud_region": "x"},
        ["endpoint"],
        id="global-key-explicit",
    ),
    pytest.param(
        [OAUTH, "", INFER],
        {"auth": "oauth"},
        ["global_api_key", "flink_api_key", "cloud_provider", "cloud_region", "endpoint"],
        id="oauth-everything-inferred",
    ),
    pytest.param(
        [OAUTH, "org-123", CUSTOM_ENDPOINT, ENDPOINT_URL],
        {"auth": "oauth", "organization_id": "org-123", "endpoint": ENDPOINT_URL},
        ["cloud_provider", "cloud_region"],
        id="oauth-endpoint",
    ),
]


class TestProfileTemplate:
    def test_template_is_valid_yaml(self):
        template = load_profile_template()
        assert "fixed" in template
        assert template["fixed"]["type"] == "confluent"
        assert "prompts" in template

    @pytest.mark.parametrize(("auth_answers", "expected", "absent"), SCENARIOS)
    def test_choices_produce_the_expected_profile(self, auth_answers, expected, absent):
        target = generate(auth_answers)

        for key, value in expected.items():
            assert target[key] == value
        for key in absent:
            assert key not in target

    @pytest.mark.parametrize(("auth_answers", "expected", "absent"), SCENARIOS)
    def test_every_generated_profile_is_valid(self, auth_answers, expected, absent):
        """Whatever `dbt init` writes must load: an omitted organization or cloud setting is
        only acceptable where the profile can infer it."""
        target = generate(auth_answers)

        ConfluentCredentials.from_dict({k: v for k, v in target.items() if k != "threads"})

    def test_choose_specific_compute_pool(self):
        target = generate(
            [GLOBAL_KEY, "key", "secret", "", INFER], pool_answers=(SPECIFIC_POOL, "lfcp-abc")
        )

        assert target["compute_pool_id"] == "lfcp-abc"

    def test_choose_poolless(self):
        """Choosing the default compute pool omits compute_pool_id entirely."""
        target = generate([GLOBAL_KEY, "key", "secret", "", INFER])

        assert "compute_pool_id" not in target

    def test_name_prefix(self):
        target = generate([GLOBAL_KEY, "key", "secret", "", INFER], name_prefix="alice-")

        assert target["name_prefix"] == "alice-"

    def test_global_key_does_not_prompt_for_the_flink_pair(self):
        target = generate([GLOBAL_KEY, "key", "secret", "", INFER])

        assert "flink_api_key" not in target
        assert "flink_api_secret" not in target

    def test_blank_organization_id_is_written_as_blank(self):
        """The blank default is what `dbt init` writes; the credentials read it as omitted."""
        target = generate([OAUTH, "", INFER])

        assert target["organization_id"] == ""
        credentials = ConfluentCredentials.from_dict(
            {k: v for k, v in target.items() if k != "threads"}
        )
        assert not credentials.organization_id

    def backend_choice_prompt(self, auth_choice):
        """The text of the `_choose_backend_url` prompt shown after choosing `auth_choice`."""
        template = load_profile_template()
        section = list(template["prompts"]["_choose_api_credentials"].values())[auth_choice - 1]
        with mock.patch("click.prompt") as mock_prompt:
            mock_prompt.side_effect = [1, StopIteration]
            task = InitTask.__new__(InitTask)
            answers = {"_choose_backend_url": section["_choose_backend_url"]}
            try:
                task.generate_target_from_input(answers)
            except StopIteration:
                pass
            call = mock_prompt.call_args_list[0]
        assert call[1]["type"] is click.INT
        return call[0][0]

    @pytest.mark.parametrize("auth_choice", [GLOBAL_KEY, OAUTH])
    def test_inferable_credentials_offer_inference(self, auth_choice):
        prompt = self.backend_choice_prompt(auth_choice)

        assert "Infer cloud_provider and cloud_region" in prompt
        assert "Use cloud_provider and cloud_region" in prompt
        assert "Use a custom endpoint" in prompt

    def test_flink_key_does_not_offer_inference(self):
        """A Flink-region key can't infer anything, so inference isn't on its menu."""
        prompt = self.backend_choice_prompt(FLINK_KEY)

        assert "Infer" not in prompt
        assert "Use cloud_provider and cloud_region" in prompt
        assert "Use a custom endpoint" in prompt

    def test_organization_id_prompt_may_be_blank_only_where_inferable(self):
        choices = load_profile_template()["prompts"]["_choose_api_credentials"]
        blankable = [
            label
            for label, options in choices.items()
            if options["organization_id"].get("default") == ""
        ]

        assert blankable == [label for label in choices if "Flink" not in label]
