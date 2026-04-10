from pathlib import Path
from unittest import mock

import click
import yaml
from dbt.task.init import InitTask

PROFILE_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2]
    / "dbt"
    / "include"
    / "confluent"
    / "profile_template.yml"
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


class TestProfileTemplate:
    def test_template_is_valid_yaml(self):
        template = load_profile_template()
        assert "fixed" in template
        assert template["fixed"]["type"] == "confluent"
        assert "prompts" in template

    def test_choose_cloud_provider_region(self):
        """Choosing 'Use cloud_provider and cloud_region' prompts for provider and region."""
        template = load_profile_template()
        prompt_responses = [
            1,                  # _choose_backend_url: "Use cloud_provider and cloud_region"
            "aws",              # cloud_provider
            "us-west-1",        # cloud_region
            "org-123",          # organization_id
            "lfcp-abc",         # compute_pool_id
            "env-xyz",          # environment_id
            "my_db",            # dbname
            "my-key",           # flink_api_key
            "my-secret",        # flink_api_secret
            "streaming_query",  # execution_mode
            "dbt-confluent-",   # statement_name_prefix
            "dbt-confluent",    # statement_label
            1,                  # threads
        ]

        target = _run_generate_target(template["prompts"], prompt_responses)

        assert target["cloud_provider"] == "aws"
        assert target["cloud_region"] == "us-west-1"
        assert "endpoint" not in target

    def test_choose_custom_endpoint(self):
        """Choosing 'Use a custom endpoint' prompts for endpoint only."""
        template = load_profile_template()
        endpoint_url = "https://flink.us-east-2.aws.private.confluent.cloud"
        prompt_responses = [
            2,                  # _choose_backend_url: "Use a custom endpoint"
            endpoint_url,       # endpoint
            "org-123",          # organization_id
            "lfcp-abc",         # compute_pool_id
            "env-xyz",          # environment_id
            "my_db",            # dbname
            "my-key",           # flink_api_key
            "my-secret",        # flink_api_secret
            "streaming_query",  # execution_mode
            "dbt-confluent-",   # statement_name_prefix
            "dbt-confluent",    # statement_label
            1,                  # threads
        ]

        target = _run_generate_target(template["prompts"], prompt_responses)

        assert target["endpoint"] == endpoint_url
        assert "cloud_provider" not in target
        assert "cloud_region" not in target

    def test_choose_prompt_message_contains_option_labels(self):
        """Verify the _choose_ prompt displays the human-readable option labels."""
        template = load_profile_template()

        with mock.patch("click.prompt") as mock_prompt:
            # We only care about the first call (the _choose_ prompt),
            # so raise StopIteration after it to abort early.
            mock_prompt.side_effect = [1, StopIteration]
            task = InitTask.__new__(InitTask)

            # Just run the _choose_ part
            choose_section = {"_choose_backend_url": template["prompts"]["_choose_backend_url"]}
            try:
                task.generate_target_from_input(choose_section)
            except StopIteration:
                pass

            first_call_args = mock_prompt.call_args_list[0]
            prompt_msg = first_call_args[0][0]
            assert "Use cloud_provider and cloud_region" in prompt_msg
            assert "Use a custom endpoint" in prompt_msg
            assert first_call_args[1]["type"] is click.INT
