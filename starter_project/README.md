# Materialized Table Bug Bash — Starter Project

Get from a fresh checkout to your first `dbt run` in ~5 minutes.

## 1. Checkout the branch and navigate to the starter project

Clone the repo first, if needed:

```bash
git clone git@github.com:confluentinc/dbt-confluent.git
cd dbt-confluent
```

Then switch to the bug bash branch and starter project folder:

```bash
git checkout 2026-08-bug-bash-dbt-mt
cd starter_project
```

## 2. Setup your environment

Export this static configuration info to your shell's environment.

_This will point your dbt project to the test environment in the Flink is Fine org, where we have a dedicated cluster and compute pool for this bug bash (each named 2026-08-bug-bash-dbt-mt)._

```bash
# `cc-tools` can inject `uv` config that overrides package index
# resolution and breaks a plain `uv sync`. Sidestep it for this session:
export UV_NO_CONFIG=1

export CONFLUENT_ENV_ID=env-d0v2k7
export CONFLUENT_ORG_ID=7c210ed4-6e1e-4355-abf9-b25e25a8b25a
export CONFLUENT_COMPUTE_POOL_ID=lfcp-j58nndq
export CONFLUENT_CLOUD_PROVIDER=aws
export CONFLUENT_CLOUD_REGION=us-east-2
export CONFLUENT_TEST_DBNAME=2026-08-bug-bash-dbt-mt
```

Then make sure you have a Flink API key or Global API Key for the "Flink is Fine" organization exported:

```bash
export CONFLUENT_FLINK_API_KEY=<your-api-key>
export CONFLUENT_FLINK_API_SECRET=<your-api-secret>
```

_You can make a new key at https://confluent.cloud/settings/api-keys if needed (ensuring you're in the Flink is Fine org)._

## 2. Install dependencies

This command will switch to the repo root temporarily & install required python dependencies:

```bash
( cd "$(git rev-parse --show-toplevel)" && uv sync --dev )
```

This installs dbt-core and this branch's `dbt-confluent` adapter (with `materialized_table` support) into a local venv that `uv run` picks up automatically, including from inside `starter_project/`.

## 4. Verify the connection

```bash
uv run dbt debug
```

## 5. Run it

```bash
uv run dbt run
```

If that works, you should be able to find your new materialized table [here](https://confluent.cloud/environments/env-d0v2k7/flink/materialized-tables?mtable_account_type_env-d0v2k7=&mtable_active_compute_pools_only_env-d0v2k7=true&mtable_compute_pool_id_env-d0v2k7=lfcp-j58nndq&mtable_filter_status_env-d0v2k7=&mtable_principal_env-d0v2k7=&tab=cloud).

Note that all models & their associated tables are namespaced by the current user's `$USER` name. So, this builds `${USER}_source_values` (a regular table), then `$USER_total_val` — a `materialized_table` model (`models/total_val.sql`) that's the actual subject of this bug bash. From here, try the scenarios in the bug bash doc against `$USER_total_val` (or new models of your own).

*WARNING: If your $USER name is not unique (e.g. on a shared machine) or not set, you may hit conflicts with others. You can set `BUGBASH_USER` explicitly to override this value.*
