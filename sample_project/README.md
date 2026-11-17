# Sample Project: UDFs

This project registers two scalar UDFs with the `function` materialization and calls each from a
`streaming_table` model:

| Function (`functions/`) | Language | Package | Model (`models/`) |
|---|---|---|---|
| `is_smaller` | Python | `udfs/build/tshirt_sizing.zip` (from `udfs/tshirt_sizing/`) | `tshirt_sizing_example` |
| `shout` | Java | `udfs/build/shout.jar` (from `udfs/shout/`) | `shout_example` |

You can use this as a starting point for exercising the UDF support in DBT.

# Setup

Get from a fresh checkout to running your first Flink UDF through dbt in ~5 minutes.

See [`MATERIALIZATIONS.md`](../MATERIALIZATIONS.md#function) for how the `function` materialization works.

## 1. Checkout the branch and navigate to the sample project

Clone the repo first, if needed:

```bash
git clone git@github.com:confluentinc/dbt-confluent.git
cd dbt-confluent
```

Then switch to the UDF bug bash branch and the sample project folder:

```bash
git checkout bug-bash-udfs
cd sample_project
```

## 2. Setup your environment

Point dbt at your Confluent Cloud environment with these variables:

```bash
# `cc-tools` can inject `uv` config that overrides package index
# resolution and breaks a plain `uv sync`. Sidestep it for this session:
export UV_NO_CONFIG=1

export CONFLUENT_ENV_ID=env-d0v2k7
export CONFLUENT_ORG_ID=7c210ed4-6e1e-4355-abf9-b25e25a8b25a
export CONFLUENT_COMPUTE_POOL_ID=lfcp-j58nndq
export CONFLUENT_CLOUD_PROVIDER=aws
export CONFLUENT_CLOUD_REGION=us-east-2
export CONFLUENT_TEST_DBNAME=bug-bash-dbt-udfs-2026-11
```

### Credentials

Every `function` uploads its package as a Flink artifact, so the profile needs credentials that can
use the artifact API: `auth: oauth`, or a Global API key, are the easiest ways to get started.

#### OAuth

Edit `profiles.yml`: uncomment `auth: oauth` and comment out the `global_api_key` /
`global_api_secret` lines. The first `dbt debug` or `dbt run` bounces you to the browser to log in;
later runs are a near no-op while your Confluent Cloud browser session stays active.

#### API key (the default in `profiles.yml`)

To use a global API key for auth, export them in your shell environment and the profiles.yml file will pick them up from there:

```bash
export CONFLUENT_GLOBAL_API_KEY=<your-api-key>
export CONFLUENT_GLOBAL_API_SECRET=<your-api-secret>
```

_You can make a new key at https://confluent.cloud/settings/api-keys if needed._

## 3. Install dependencies

```bash
uv sync --dev
```

This installs dbt-core and this branch's `dbt-confluent` adapter into a local venv that `uv run`
picks up automatically, including from inside `sample_project/`.

## 4. Build the UDF packages

The Python package needs [`uv`](https://docs.astral.sh/uv/) (installed above). The Java package
needs [Maven](https://maven.apache.org/) and a JDK (11 or later). Then, from `udfs/`:

```bash
# You may need to run `dev-login` first.
cd udfs
make
cd ..
```

This produces `udfs/build/tshirt_sizing.zip` and `udfs/build/shout.jar`. If `java` on your `PATH` is the macOS
stub with no JDK behind it, set `JAVA_HOME` first (e.g. with Homebrew:
`export JAVA_HOME=/opt/homebrew/opt/openjdk@21 PATH=/opt/homebrew/opt/openjdk@21/bin:$PATH`).

## 5. Verify the connection

```bash
uv run dbt debug
```

## 6. Run it

```bash
uv run dbt build
```

_Note: You may be used to using `dbt run`, but `dbt build` is required to build the functions themselves - `run` only builds the "models"._

dbt uploads each package as an artifact, creates the functions in Flink, then runs the two
`streaming_table` models that call them. Expected results:

| Model | Column | Value |
|---|---|---|
| `tshirt_sizing_example` | `s_smaller_than_m` | `true` |
| `tshirt_sizing_example` | `xl_smaller_than_m` | `false` |
| `tshirt_sizing_example` | `unknown_size` | `false` |
| `shout_example` | `shouted` | `HELLO!` |
| `shout_example` | `shouted_null` | `NULL` |

Things to try:

- Edit a UDF's source, re-run `make`, then `dbt run -s <function>`. A new package hash uploads a new
  artifact and re-creates the function.
- Change a function's config (e.g. `path`) and re-run to see `on_configuration_change` at work.
- List and delete the artifacts no function uses any more with the `list_function_artifacts` and
  `sweep_function_artifacts` run-operations.
- Add a UDF of your own: a new `functions/<name>.sql` and a model that calls it with
  `{{ function('<name>') }}`. Streaming table models need their columns declared in
  `models/schema.yml`.

## A few dbt basics

- **`uv run dbt build`:** Build all functions and models in the project.
- **`uv run dbt build -s <name>`:** Select just one function or model instead of the whole project.
- **`uv run dbt run -s +<model_name>`:** Also build everything the model depends on first, such as the
  functions it calls.
- **`uv run dbt run -s <name>+`:** Also build anything downstream of the selected node (useful to replace a function and all statements that depend on it).
- **`uv run dbt run --full-refresh ...`:** Drops and recreates instead of evolving in place.

## A few UDF-specific operations

- **`uv run dbt run-operation list_function_artifacts`:** List information about artifacts managed by this dbt project.
- **`uv run dbt run-operation sweep_function_artifacts`:** Sweep unused artifacts managed by this dbt project.
  - This is a dry run by default. The output will provide guidance to perform the actual deletion.
