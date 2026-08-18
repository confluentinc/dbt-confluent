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

We have dedicated testing infra setup in the Flink is Fine organization for this bug bash (though squatting in the existing TableAPI env).

You can access this infra in the web UI with the links below, which may be useful during the bug bash:

- **Organization:** Flink is Fine / 7c210ed4-6e1e-4355-abf9-b25e25a8b25a (Ask the bug-bash helpers if you don't have access)
- **Environment:** [TableAPI / env-d0v2k7](https://confluent.cloud/environments/env-d0v2k7/overview?tab=cloud)
- **Cluster:** [2026-08-bug-bash-dbt-mt / lkc-6koqqv6](https://confluent.cloud/environments/env-d0v2k7/clusters/lkc-6koqqv6/overview)
- **Compute Pool:** [2026-08-bug-bash-dbt-mt / lfcp-j58nndq](https://confluent.cloud/environments/env-d0v2k7/flink/pools/lfcp-j58nndq/overview)

Export these environemnt variables to point DBT at this test infra:

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

## 3. Install dependencies

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

## A few dbt basics

If you got this far, congrats! You're ready to make and run some DBT models of your own. Here's a few basics:

| Command | What it does |
| --- | --- |
| `dbt run` | Build all models in the project. |
| `dbt run -s <model_name>` | Select just one model to run instead of the whole project |
| `dbt run -s +<model_name>` | Also build anything that model `ref()`s first — use this the first time you touch a model whose dependency doesn't exist yet |
| `dbt run -s <model_name>+` | Also build anything downstream of the selected model — useful for e.g. reprocessing workflows |
| `dbt run --full-refresh ...` | Drops and recreates instead of evolving in place. Useful you need to make a breaking schema change that can't be made in-place. |
