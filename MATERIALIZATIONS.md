# Materializations

## Table of Contents

- [Introduction](#introduction)
- [Supported Materializations](#supported-materializations)
- [Unsupported Materializations](#unsupported-materializations)
- [Materializations Reference](#materializations-reference)
  - [Materialized Table](#materialized-table)
  - [Streaming Table](#streaming-table)
  - [Streaming Source](#streaming-source)
  - [Table](#table)
  - [View](#view)
  - [Ephemeral](#ephemeral)
- [Model Configuration](#model-configuration)
  - [Validation](#validation)
  - [Tableflow](#tableflow)
  - [Distributed By](#distributed-by)
  - [Statement Properties](#statement-properties)
  - [Compute Pool](#compute-pool)
- [Common Mechanics](#common-mechanics)
  - [Schema Drift Detection](#schema-drift-detection)
  - [Deterministic Statement Names](#deterministic-statement-names)

## Introduction

The materializations in dbt-confluent cover both batch and streaming use cases.

Some materializations behave like their counterparts from dbt adapters for traditional data warehouses, e.g.:

- [`table`](#table)
- [`view`](#view)
- [`ephemeral`](#ephemeral)

The **streaming materializations** use potentially long-running Flink statements or connector processes to produce continuous results:

- [`materialized_table`](#materialized-table)
- [`streaming_table`](#streaming-table)
- [`streaming_source`](#streaming-source)

Materializations that store data do so as [tables](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html),
which are themselves backed by Apache Kafka topics. This storage can be augmented with features like [Tableflow](#tableflow).

An important difference between streaming materializations and other materializations you may be used to is that they can be stateful and unbounded.
Because the streaming materializations can produce continuous results or have online consumers, you will need to consider:

- **Job state.** Many materializations are backed by a Flink job with its own in-memory
  state (aggregations, joins, windows). Some changes force that state to be discarded and rebuilt.
  For example, see [`materialized_table` evolution](#materialized-table-evolution--state-impact).

- **Schema evolution.** Models may have any number of streaming consumers, so schema compatibility
  must be considered across points of evolution to avoid affecting live downstream consumers.
  Some schema changes may require recreating the backing storage to clear incompatible records.
  Because these changes can be destructive, the adapter provides explicit control through e.g.
  [Schema Drift Detection](#schema-drift-detection).

- **Topic identity.** "Recreating" a table doesn't just replace a schema, it is a multi-step operation
  that includes several side-effects that can affect active consumers. E.g. the previous topic is deleted
  and created again with a new identity (even if the name is unchanged), and the schema is soft-deleted and
  recreated in the schema registry. If live consumers are depending on these resources when the recreation
  is performed, they may break.

## Supported Materializations

The table below summarizes all materializations supported by the dbt-confluent adapter:

| Materialization | Description | Execution Mode |
|---|---|---|
| [`materialized_table`](#materialized-table) | Declarative `CREATE OR ALTER MATERIALIZED TABLE`. The standard materialization for continuous stream processing | Streaming |
| [`streaming_table`](#streaming-table) | DDL plus a long-running `INSERT INTO ... SELECT`. The precursor to [`materialized_table`](#materialized-table). | Streaming |
| [`streaming_source`](#streaming-source) | A connector-backed source table, e.g. `faker`. | Streaming |
| [`table`](#table) | One-shot `CREATE TABLE ... AS SELECT` (CTAS). | Snapshot |
| [`view`](#view) | A named query inlined into consumers, not a persisted result. | Inherited |
| [`ephemeral`](#ephemeral) | Standard dbt CTE fragment. | Inherited |

_Note: [`view`](#view) & [`ephemeral`](#ephemeral) inherit their execution mode from any job that queries them, since they run inline in those jobs and not as independent Flink statements._

## Unsupported Materializations

Some standard dbt materializations are not supported by this adapter:

| Materialization | Reason |
|---|---|
| `materialized_view` | dbt's built-in `materialized_view` materialization is not implemented. For a Flink materialized table use [`materialized_table`](#materialized-table). |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use [`materialized_table`](#materialized-table) instead. |
| `snapshot` | dbt snapshots require MERGE/UPDATE operations that Flink does not support. |

## Materializations Reference

### Materialized Table

```sql
-- materialized_table_example.sql

{{ config(
    materialized='materialized_table',
    distributed_by={'columns': ['customer_id'], 'buckets': 4},
    start_mode='RESUME_OR_FROM_BEGINNING',
) }}

select
    customer_id,
    count(*) as order_count,
    sum(price) as lifetime_value
from {{ ref('orders') }}
group by customer_id
```

`materialized_table` is declarative.
Every run reasserts the desired definition, and Flink reconciles the table's actual state to match
it. An unchanged definition is a server-side no-op, rather than dbt-confluent choosing between a
drop/recreate and a schema-drift-based skip the way
`table`, `streaming_table`, and `streaming_source` do.

See Confluent's [materialized tables](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
concept page for the underlying feature.

#### Materialized Table: Config Options

| Config | Description |
|---|---|
| `distributed_by` | See [Distributed By](#distributed-by). |
| `with` | Table options, e.g. `{'key.format': 'avro-registry'}`. |
| `start_mode` | Where the query starts (or, on an in-place evolution, restarts) reading; see [`start_mode`](#materialized-table-start-mode) below. |
| `statement_properties` | See [Statement Properties](#statement-properties). |
| `tableflow` | See [Tableflow](#tableflow). Checked on every run: create or in-place evolution alike. |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). Details below. |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |

Each run already uses a unique per-run statement name regardless of `statement_name` (see [Statements Emitted](#materialized-table-statements-emitted) below); a configured `statement_name` becomes the base that per-run suffix is derived from.

`freshness_interval`, `refresh_mode`, and `partition_by` exist in open-source Flink but not in Confluent's dialect; they raise a compile error. Any other dbt-confluent config key this materialization doesn't read (e.g. `connector`, `on_schema_drift`) is also rejected; see [Validation](#validation).

#### Materialized Table: Start Mode

Controls where the query starts (or, on an in-place evolution, restarts) reading. Default: `RESUME_OR_FROM_BEGINNING`. Accepted values are:

| Value | Starts from |
|---|---|
| `FROM_BEGINNING` | Beginning of the topic |
| `FROM_NOW` | Current offset |
| `FROM_TIMESTAMP('<timestamp>')` | The given timestamp |
| `FROM_NOW(INTERVAL '<n>' <unit>)` | `<n> <unit>` before now |
| `RESUME_OR_FROM_BEGINNING` | Saved offsets, if present; else the beginning of the topic |
| `RESUME_OR_FROM_NOW` | Saved offsets, if present; else the current offset |
| `RESUME_OR_FROM_TIMESTAMP('<timestamp>')` | Saved offsets, if present; else the given timestamp |
| `RESUME_OR_FROM_NOW(INTERVAL '<n>' <unit>)` | Saved offsets, if present; else `<n> <unit>` before now |

`start_mode` also governs what happens to a *stateful* query's results when the table [evolves](#materialized-table-evolution--state-impact) in place.

#### Materialized Table: Contracts and Primary Keys

With `config(contract={'enforced': true})` and an explicit `columns:`/`constraints:` block in the model's schema.yml, `materialized_table` renders an explicit column-definition list ahead of `DISTRIBUTED BY`/`WITH`/`START_MODE`:

```sql
CREATE OR ALTER MATERIALIZED TABLE <relation> (<cols>, PRIMARY KEY (...) NOT ENFORCED)
  ... AS SELECT ...
```

- The `PRIMARY KEY (...) NOT ENFORCED` clause comes from a model-level `primary_key` constraint, matching Confluent's materialized-table grammar.
- This is what makes the resulting table usable in **snapshot queries** against its key.

Without an enforced contract, the materialization renders a plain `AS SELECT` with no explicit column list.

#### Materialized Table: Evolution / State Impact

During an evolution the Flink job clears its internal state, resetting any aggregations, window, or join state.

It then begins (re-)processing data according to the configured [`start_mode`](#materialized-table-start-mode):

- Under a `RESUME_*` start mode (the default, `RESUME_OR_FROM_BEGINNING`, is one of these), stateless queries (projections, filters) evolve seamlessly: no reprocessing, no duplicates.
- For *stateful* queries, evolution recalculates results from a clean slate rather than adjusting the old ones. Depending on `start_mode`, that recalculation may not cover the same source data as before, so joins, aggregations, and other stateful results can shift (e.g. an aggregation resuming from an offset instead of replaying history will look "undercounted").

See [Confluent's materialized tables concepts page](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html#controlling-reprocessing-with-start-mode).

**Known limitations:**

- Not every change can evolve in place.
  + Dropping columns is rejected at submission. The fix is `--full-refresh`.

#### Materialized Table: Statements Emitted

Every run submits the same statement, whether the table does not exist yet, exists unchanged, or
exists with a different definition. An unchanged definition is a server-side no-op:

```sql
CREATE OR ALTER MATERIALIZED TABLE <relation> [(<cols>, PRIMARY KEY (...) NOT ENFORCED)]
  [DISTRIBUTED BY (...)] [WITH (...)] [START_MODE = ...] AS <model SELECT>
```

See the [`CREATE OR ALTER MATERIALIZED TABLE` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html).

#### Materialized Table: Switching Materializations

An existing regular table or view cannot be converted to a materialized table, and a materialized table cannot be adopted by the other materializations.

Making such a change requires using `--full-refresh` to completely delete the existing table first, which does imply data loss.

---

### Streaming Table

```sql
-- streaming_table_example.sql

{{ config(
    materialized='streaming_table',
    distributed_by={'columns': ['customer_id'], 'buckets': 4},
    with={'changelog.mode': 'append'},
) }}

select customer_id, order_id, price
from {{ ref('orders') }}
where price > 0
```

`streaming_table` creates a table, then runs a separate, continuous `INSERT INTO ... SELECT` statement to populate it.
This two-statement approach was the preferred way to build streaming pipelines before the introduction of [`materialized_table`](#materialized-table).

See Confluent's [dynamic tables and continuous queries](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html)
concept page for the underlying execution model.

#### Streaming Table: Config Options

| Config | Description |
|---|---|
| `with` | Table options baked into the DDL statement; this is the DDL-side equivalent of `statement_properties` below. |
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). The DDL gets a `-ddl` suffix appended to this name. |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `statement_properties` | See [Statement Properties](#statement-properties). Applies only to the INSERT statement, not the CREATE TABLE DDL; for the DDL side, use `with` instead. |
| `tableflow` | See [Tableflow](#tableflow). |

#### Streaming Table: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `streaming_table` runs [schema
drift detection](#schema-drift-detection).

Separately, on every re-run, the adapter checks the long-running INSERT statement itself.
If it's missing (e.g. the process crashed between DDL and DML, or the statement was deleted
externally) or in a terminal phase (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), `dbt run` resubmits
**only the INSERT** statement under the same deterministic name. The table and its topic state are preserved,
and no `--full-refresh` is required.

A `RUNNING` statement, an in-flight transition (`PENDING`, `STOPPING`, `DELETING`), or `DEGRADED` is
treated as healthy: the adapter does not interrupt it.

#### Streaming Table: Statements Emitted

`streaming_table` submits two statements:

```sql
CREATE TABLE <relation> [DISTRIBUTED BY (...)] [WITH (...)]     -- DDL, bounded, name suffixed "-ddl"
INSERT INTO <relation> <model SELECT>                           -- DML, long-running
```

The DDL completes immediately; the INSERT is a genuinely long-running, continuous statement.
See the [`CREATE TABLE`](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
and [`INSERT INTO ... FROM SELECT`](https://docs.confluent.io/cloud/current/flink/reference/queries/insert-into-from-select.html)
references.

#### Streaming Table: Adopting Existing Resources

If you already have a Flink pipeline running, deployed by hand, by a previous tool, or by another
team, you can bring it under dbt management without recreating it, using `streaming_table`.
A pipeline is two things: a **table** (the relation) and a **statement** (the long-running query
that populates it). Map your model to each:

- **Table** — set dbt's standard [`alias`](https://docs.getdbt.com/reference/resource-configs/alias) config to the existing table name (omit it if the table already matches the model name).
- **Statement** — set `statement_name` to the existing statement name.

```sql
{{ config(
    materialized='streaming_table',
    alias='orders_enriched',          -- existing table
    statement_name='orders-enriched-insert',  -- existing INSERT statement
    with={'changelog.mode': 'append'},
) }}
select order_id, price from {{ ref('orders') }}
```

On the next `dbt run` (no `--full-refresh`), the adapter looks up both by name and takes over their lifecycle:

- If the statement is **healthy** (`RUNNING`, an in-flight transition such as `PENDING`, `STOPPING`, or `DELETING`, or `DEGRADED`, i.e. any non-terminal phase), it is adopted as-is: the run skips creation and leaves the statement untouched.
- If the statement is **missing or terminal** (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), it is re-submitted under the same name.
- The existing **table is never dropped** (only `--full-refresh` drops and recreates).

Adoption is purely name-based: the adapter does not track which tool created a resource, only its name, so there is no separate "import" step.

**Preconditions**:

- **Schema should match.** Under the default `on_schema_drift='fail'`, the adapter runs [schema drift detection](#schema-drift-detection) comparing the existing table to the model's SELECT before adopting; any mismatch fails the run. With `config(on_schema_drift='ignore')`, enforcement depends on the adoption path:
    - **Healthy statement → skip:** the running statement is left untouched and nothing is re-submitted, so **no drift is enforced at all, including columns.** A mismatch is silently tolerated; you must align the model to the existing schema yourself.
    - **Dead/terminal statement → restart:** the INSERT is re-submitted, so a columns-only check still runs (a column mismatch would also be rejected by Flink); benign options/distribution drift is relaxed.
- **Names are sanitized.** The `statement_name` you configure is normalized to Flink's constraints (see [Flink Naming Constraints](#flink-naming-constraints)) before lookup, so it must match the existing statement's actual name. Statements already named within those constraints (lowercase alphanumeric + hyphens) match verbatim.

---

### Streaming Source

```sql
-- streaming_source_example.sql

{{ config(
    materialized='streaming_source',
    connector='faker',
    with={'faker.rows.per.second': 1},
) }}

customer_id STRING,
order_total DOUBLE,
order_ts TIMESTAMP(3)
```

Note the model's body is a column-definition list, not a `SELECT` — `streaming_source` has no query to compile.

`streaming_source` creates a connector-backed source table.
It requires `config(connector='...')`; the model SQL defines only the column definitions, with no `SELECT` query.

With this materialization you can, for example, configure a `faker` connector to generate mock data for development
and testing.

See Confluent's [faker sample-data how-to guide](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html)
for the underlying feature.

#### Streaming Source: Config Options

| Config | Description |
|---|---|
| `connector` (required) | The connector to attach; see the [faker sample-data how-to guide](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html). |
| `with` | Additional connector options. |
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `tableflow` | See [Tableflow](#tableflow). |

#### Streaming Source: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `streaming_source` runs [schema
drift detection](#schema-drift-detection) against the model's column definitions (there's no SELECT
query to infer a schema from).
Recovery works differently from `streaming_table`: automatic recovery is **not** supported here,
because the CREATE statement also attaches the connector, and Flink doesn't allow re-attaching a
connector to an existing table.
If the connector statement is dead, a plain run follows the normal existing-relation skip path and
logs only the generic relation-already-exists information. You must run with `--full-refresh` (which
drops and recreates the table, permanently deleting its data) to fix it.

#### Streaming Source: Statements Emitted

`streaming_source` submits a single statement:

```sql
CREATE TABLE <relation> (<column definitions>) [DISTRIBUTED BY (...)] WITH (connector = '...', ...)
```

`connector` is merged directly into the `WITH` options. See the
[`CREATE TABLE` connector clause reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#connector)
and the
[faker sample-data how-to guide](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html).
Unlike `CREATE TABLE` statements used in other materializations, this statement doesn't just create a table: it *is* the
connector's ongoing running process, and there's no separate long-running statement behind it.

---

### Table

```sql
-- table_example.sql

{{ config(
    materialized='table',
) }}

select customer_id, count(*) as order_count, sum(price) as lifetime_value
from {{ ref('orders') }}
group by customer_id
```

`table` creates a table via a one-shot `CREATE TABLE ... AS SELECT` (CTAS).
It's the closest analog to a traditional batch-warehouse table: the query runs once, produces a
result, and completes.
If you're new to dbt-confluent, this is the easiest materialization to start with before moving on
to `streaming_table` or `materialized_table`.

See Confluent's [snapshot queries](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html)
concept page for the underlying execution model.

#### Table: Config Options

| Config | Description |
|---|---|
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `tableflow` | See [Tableflow](#tableflow). |
| `ignore_unsupported_config` | See [Validation](#validation). |

#### Table: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `table` skips creation after
running [schema drift detection](#schema-drift-detection) (columns and `distributed_by`).
Use `--full-refresh` to drop and recreate the table.
This permanently deletes the backing Kafka topic and all of its data.

#### Table: Statements Emitted

`table` submits a single statement:

```sql
CREATE TABLE <relation> [DISTRIBUTED BY (...)] AS (<model SELECT>)
```

The statement runs in Confluent Cloud Flink's
[snapshot execution mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html):
it reads a point-in-time view of its sources via Flink's batch execution mode, and "runs, returns
results, and then exits," rather than running continuously.

See the [`CREATE TABLE` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
for the full DDL grammar.

---

### View

```sql
-- view_example.sql

{{ config(materialized='view') }}

select customer_id, order_id, price
from {{ ref('orders') }}
where price > 0
```

A view is a named query definition: Flink inlines its SQL into whatever job(s) actually query it, at
those jobs' own execution time, rather than running any compute of its own.
Unlike Kafka-backed materializations, `view` doesn't create a topic that stores data.
Creating a view does reserve a special Kafka topic name, but that topic is metadata-only and never
holds records. This differs from `ephemeral`, which creates no topic at all.

See Confluent's [`CREATE VIEW` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html)
for the underlying statement.

#### View: Config Options

Only three dbt-confluent config keys apply to `view`:

| Config | Description |
|---|---|
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `ignore_unsupported_config` | See [Validation](#validation). |

Most other cross-materialization config keys don't apply here: `with`, `distributed_by`,
and `tableflow`, for example, all require a real Kafka-backed table, and this list isn't exhaustive.
Setting any of dbt-confluent's other config keys on a `view` model fails at compile time; see
[Validation](#validation).

#### View: Statements Emitted

On a rerun, `view` submits two statements. On a first run, there is no existing view to drop, so
only `CREATE VIEW` is submitted. Unlike every other materialization on this page, there is no
diffing and no skip-if-unchanged path here:

```sql
DROP VIEW IF EXISTS <relation>
CREATE VIEW <relation> AS (<model SELECT>)
```

Both are short-lived, completing statements, not long-running jobs.
See the [`CREATE VIEW`](https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html)
and [`DROP VIEW`](https://docs.confluent.io/cloud/current/flink/reference/statements/drop-view.html)
references.
`--full-refresh` behaves identically to a plain run, since a plain run already drops and recreates.

---

### Ephemeral

```sql
-- ephemeral_example.sql

{{ config(materialized='ephemeral') }}

select customer_id, order_id, price
from {{ ref('orders') }}
where price > 0
```

`ephemeral` is a standard dbt CTE-based query fragment.  No adapter-specific code exists for it at
all, so it behaves exactly like dbt-core's built-in `ephemeral` materialization on any other
adapter.  No Kafka topic is created and no Flink statement is submitted; the model's compiled SQL is
inlined as a CTE into every downstream model that `ref()`s it.

Because there's no dbt-confluent
macro in the loop, none of dbt-confluent's config validation (see [Validation](#validation))
applies: setting `with`, `distributed_by`, `connector`, or any other dbt-confluent config key on an
`ephemeral` model is silently ignored rather than rejected.  If multiple downstream models `ref()`
the same `ephemeral` model over a Kafka-backed source, each one inlines and re-plans that source
independently. Flink scans the source once per consumer, not once shared across them.

---

## Model Configuration

This section contains documentation for configuration options that apply to multiple materializations.
Each per-materialization section links back to the specific subsections below that it supports.

### Validation

Setting a dbt-confluent config key on a materialization that doesn't use it fails the run immediately with a clear error, rather than silently doing nothing. For example, `config(materialized='table', statement_properties={...})` fails at compile time (`statement_properties` is only read by `streaming_table` and `materialized_table`), instead of the value being silently ignored.

This only ever checks dbt-confluent's own config keys (`with`, `distributed_by`, `connector`, `on_schema_drift`, `statement_name`, `compute_pool_id`, `statement_properties`, `start_mode`, `tableflow`, `ignore_unsupported_config`) against the materialization you're using. Any other config key, including your own custom keys read by your own hooks or macros, is never inspected and never affected by this check.

If a key name genuinely collides with one of dbt-confluent's own (an unlikely but possible coincidence), opt it out per model with `ignore_unsupported_config`:

```sql
{{ config(
    materialized='table',
    statement_properties={'my_custom_key': 'value'},  -- not really ours; used by a custom macro
    ignore_unsupported_config=['statement_properties'],
) }}
```

`ignore_unsupported_config` takes a list of specific key names, not a blanket on/off switch. Opting out of one false positive doesn't also suppress a real mistake on a different key in the same model.

### Tableflow

```sql
-- tableflow_example.sql

{{ config(
    materialized='table',
    tableflow={
        'table_formats': ['ICEBERG'],
        'storage': {'kind': 'Managed'},
        'config': {
            'retention_ms': 604800000,
            'error_handling': {
                'mode': 'LOG',
                'target': 'error_log',
            },
        },
    }
) }}

select order_id, customer_id, price from {{ ref('orders') }}
```

[Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) materializes the Kafka topic backing a Flink table as an Apache Iceberg and/or Delta Lake table in object storage.

That table can be [queried by external engines](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/overview.html) like Snowflake and Trino, and by [Confluent Cloud Flink itself](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-flink.html) via a [snapshot query](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html) (the same mechanism [`table`](#table) uses).

A Tableflow configuration can be added to any materialization that owns a real Kafka-backed table ([`table`](#table), [`streaming_table`](#streaming-table), [`streaming_source`](#streaming-source), [`materialized_table`](#materialized-table)) and supports the following fields:

Tableflow is reconciled on every run. The adapter creates it when it is absent, patches changes in
place when possible, and disables and re-enables it when a storage change requires recreation.
Removing the `tableflow` config does not disable existing Tableflow; use `--full-refresh` or the
adapter's lifecycle operation to disable it. If Tableflow is in `FAILED`, the run warns about that
state even when there is no configuration patch to send.

<table>
<tr><th>Field</th><th>Description</th></tr>
<tr>
  <td><code>table_formats</code> (required)</td>
  <td><code>'ICEBERG'</code>, <code>'DELTA'</code>, or a list containing either or both (<code>['DELTA', 'ICEBERG']</code>).</td>
</tr>
<tr>
  <td><code>storage</code> (required)</td>
  <td>

A variant distinguished by the `kind` key (see the [storage configuration guide](https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-storage.html)):

- Confluent-managed storage:
  ```python
  { 'kind': 'Managed' }
  ```
- Bring-your-own S3 bucket:
  ```python
  { 'kind': 'ByobAws',
    'bucket_name': '...',
    'provider_integration_id': '...' }
  ```
- Customer-owned Azure Data Lake Storage Gen2.
  ```python
  { 'kind': 'AzureDataLakeStorageGen2',
    'storage_account_name': '...',
    'container_name': '...',
    'provider_integration_id': '...' }
  ```
- Bring-your-own Google Cloud Storage bucket:
  ```python
  { 'kind': 'GoogleCloudStorage',
    'bucket_name': '...',
    'provider_integration_id': '...' }
  ```

    </td>
</tr>
<tr>
  <td><code>config</code> (optional)</td>
  <td>

A mapping with various tableflow topic-level configuration settings:

- `retention_ms` - (optional) non-negative integer or digit-only numeric string controlling snapshot retention.
- `data_retention_ms` - (optional) non-negative integer or digit-only numeric string controlling data retention.
- `error_handling` - (optional) A variant type field that specifies how errors are handled:
  |Variant|Function|
  |---|---|
  |`{ 'mode': 'SUSPEND' }`|Suspends materialization (the default).|
  |`{ 'mode': 'SKIP' }`|Skip and continue.|
  |`{ 'mode': 'LOG', 'target': '...' }`|Logs the error and continues. `target` defaults to 'error_log'.|

  </td>
</tr>
</table>

**Known Limitations:**

- Confluent Cloud enforces a grace period of up to 1 hour when switching from external to managed storage.
  - Attempting this change will succeed in disabling the external storage, but will fail (with a clear error message) trying to re-enable it before the grace period expires.
  - Re-run the model after waiting for the grace period to pass (instructions will be provided in the dbt run output),
    or use `--full-refresh` to drop the table entirely (including the data resident on the Kafka topic) and rebuild from scratch immediately.
- Tableflow's control-plane routes require a Global API key (`global_api_key` / `global_api_secret`).
  - If you previously ran your models with only the flink API key configured, you may need to generate a new key for Tableflow.
  - Note that a global API works for the Flink APIs as well, so you do not need to provide both.

### Distributed By

Confluent Flink lets you control how a table's rows are distributed across Kafka partitions with a `DISTRIBUTED BY HASH(...) INTO N BUCKETS` clause in the `CREATE TABLE` DDL.
The adapter exposes this through a `distributed_by` config on [`table`](#table), [`streaming_table`](#streaming-table), [`streaming_source`](#streaming-source), and [`materialized_table`](#materialized-table) models:

```sql
{{ config(
    materialized='streaming_table',
    distributed_by={'columns': ['order_id'], 'buckets': 4}
) }}
select order_id, customer_id, price from {{ ref('orders') }}
```

This renders as:

```sql
CREATE TABLE `orders_by_id` (...)
DISTRIBUTED BY HASH(`order_id`) INTO 4 BUCKETS
WITH (...)
```

**Known Limitations:**

- The `distributed_by` configuration cannot be altered without recreating the topic.
  - The underlying [ALTER TABLE](https://docs.confluent.io/cloud/current/flink/reference/statements/alter-table.html) statement itself does not allow updating this setting.
  - This is important because changing the distribution settings can invalidate all data already written to the topic.


#### Fields

|Field|Description|
|---|---|
|`columns`| (required) - non-empty list of column names used to compute the hash|
|`buckets`| (optional) - positive integer; omit to let Confluent Cloud choose|

#### Validation

The adapter validates the config at the start of each materialization run and raises a clear compile error if any of the following hold:

- `distributed_by` is not a mapping
- `columns` is missing, empty, a string, or contains non-string / empty entries
- A column name contains a backtick (Flink identifiers can't escape backticks)
- `buckets` is set but isn't a positive integer (rejects `0`, negatives, floats, strings, booleans)
- The mapping has any key other than `columns` or `buckets` (catches typos like `'strategy': 'range'`)

**Important:** Flink requires that the distribution columns appear at the **beginning** of the table's column schema, and in the **same order** as listed in `columns`. The adapter does not validate this (it would require parsing the model SQL). Flink will reject the `CREATE TABLE` at submission with `Key columns must appear at the beginning of the table schema. Also, DISTRIBUTED BY key names must be in the same order as the key schema columns.`

Practical implication for each materialization:
- `table` and `streaming_table`: list the distribution columns first in the model's `SELECT`.
- `streaming_source`: declare the distribution columns first in the column-definition list.

```sql
-- ❌ Rejected by Flink — `customer_id` is the distribution key but appears second
{{ config(distributed_by={'columns': ['customer_id']}) }}
select order_id, customer_id, price from {{ ref('orders') }}

-- ✅ Accepted — `customer_id` is first
{{ config(distributed_by={'columns': ['customer_id']}) }}
select customer_id, order_id, price from {{ ref('orders') }}
```

Flink only supports the `HASH` distribution strategy today, so the adapter always emits `HASH(...)`. See the [Flink CREATE TABLE documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html#distributed-by-clause) for details.

### Statement Properties

Set Flink SET-style statement properties, such as `sql.tables.scan.idle-timeout`, with the `statement_properties` config, available on the [`streaming_table`](#streaming-table) and [`materialized_table`](#materialized-table) materializations:

```sql
{{ config(
    materialized='streaming_table',
    statement_properties={'sql.tables.scan.idle-timeout': '30 s'},
) }}
```

See the [SET Statement](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html) documentation for all [available options](https://docs.confluent.io/cloud/current/flink/reference/statements/set.html#available-set-options).

This is different from `with`: `with` sets table-level WITH-clause options baked into the `CREATE TABLE` DDL, while `statement_properties` sets properties on the statement that runs the model's query: `streaming_table`'s long-running `INSERT INTO ... SELECT`, or `materialized_table`'s `CREATE OR ALTER MATERIALIZED TABLE ... AS SELECT`. The value is a dict of `string -> string|int|bool`.

Three keys are reserved for use by the driver - `sql.current-catalog`, `sql.current-database`, and `sql.snapshot.mode` (derived from the statement's execution mode). Setting any reserved properties yourself fails the run with a "reserved system property" error. Confluent Cloud Flink performs the validation of all the provided values at statement planning time.

Changing `statement_properties` on an existing, healthy `streaming_table` takes effect **only** on the next `--full-refresh` or statement restart. A running statement keeps its original properties, since (like `compute_pool_id`) they're a property of the statement, not the table, and aren't part of drift detection. `materialized_table` has no such lag: every run resubmits a fresh `CREATE OR ALTER` statement under a new per-run name (see [Deterministic Statement Names](#deterministic-statement-names)), so a changed value takes effect on the very next run.

### Compute Pool

By default, every statement runs on the compute pool configured in your profile (`compute_pool_id`). You can override the pool per model, for example to isolate a heavy model or to manage resources, with the `compute_pool_id` config, available on every materialization that submits a statement: [`table`](#table), [`view`](#view), [`materialized_table`](#materialized-table), [`streaming_table`](#streaming-table), and [`streaming_source`](#streaming-source).

```sql
{{ config(materialized='streaming_table', compute_pool_id='lfcp-abc123') }}
```

The override applies to all statements a model submits (DDL, the long-running INSERT, and metadata/drift-check queries). The pool must exist in the same environment and region as the profile, and the API key used must have access to it; Confluent Cloud validates this at submission time. When `compute_pool_id` is omitted, the profile default is used. If the profile sets no default pool either, Confluent Cloud Flink runs the statement on the environment+region default pool.

Pool changes take effect per materialization: `view` and `materialized_table` submit fresh
statements on every run and use a changed pool immediately; `table` uses the new pool on its next
CTAS, which requires `--full-refresh`; running `streaming_table` and `streaming_source` statements
are not migrated. A `streaming_table` requires a restart or `--full-refresh`; a `streaming_source`
requires `--full-refresh` because its connector statement cannot be reattached to the existing table.

For recoverable `streaming_table` models, statement recovery and cleanup (see [Statement Lifecycle](#statement-lifecycle)) operate by statement name and are pool-agnostic: the statement is found, inspected, and, when dead, resubmitted on the model's configured pool regardless of the profile default.

Changing `compute_pool_id` on an existing, healthy running statement does not migrate that statement,
since the pool is a property of the statement (not the table) and isn't part of drift detection.

#### Per-environment pools in CI/CD

The same model is often deployed to different compute pools across environments (dev / staging / prod) or regions. Rather than hard-coding a pool, inject it at deploy time with an environment variable:

```sql
{{ config(materialized='streaming_table', compute_pool_id=env_var('FLINK_COMPUTE_POOL')) }}
```

Your CI/CD pipeline sets `FLINK_COMPUTE_POOL` (and typically `statement_name`) per target, keeping a single Git source of truth.

## Common Mechanics

Background on adapter behavior that spans multiple materializations.

### Schema Drift Detection

**Scope:** this only applies to [`table`](#table),
[`streaming_table`](#streaming-table), and
[`streaming_source`](#streaming-source), the materializations that still use a
drop-and-recreate-or-skip lifecycle. [`materialized_table`](#materialized-table)
doesn't use this at all; Flink reconciles the table definition natively instead (see [Evolution /
State Impact](#materialized-table-evolution--state-impact)), which is the direction this adapter is
moving toward. `view` and `ephemeral` have no persistent schema to check.

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation.
The check compares **columns** and **`distributed_by`** for all applicable materializations, plus
**WITH options** for `streaming_table` and `streaming_source`, in a single pass and raises one
error listing every violation, so you don't have to fix them one at a time.
To rebuild the model to reflect the local configuration, use `--full-refresh` to recreate the model from scratch.

Drift detection also detects when the existing relation is a **materialized table** (a reverse materialization switch) and fails with dedicated guidance instead of a drift list; see [Switching materializations](#materialized-table-switching-materializations). (`materialized_table` models themselves do not use drift detection; Flink reconciles the re-asserted definition instead. See [Materialized Table](#materialized-table).)

To determine the expected schema of a `table` or `streaming_table`, the adapter submits the model's SELECT as a Flink `sql.dry-run` statement. Flink validates and plans the query and answers with its result schema without running it or storing a statement, so there is nothing to clean up. The adapter then issues a single `UNION ALL` query against `INFORMATION_SCHEMA.COLUMNS`, `TABLES`, and `TABLE_OPTIONS` to fetch every piece of metadata at once, and compares the existing table against the dry-run's columns.

The dry-run evaluates the SELECT in the mode the model is built in, whatever the profile's or the model's `execution_mode`: snapshot for `table` (its `CREATE TABLE ... AS SELECT` runs in `snapshot_ddl`), and `streaming_query` for `streaming_table` (its `INSERT` runs as a streaming query). So a `table` whose SELECT only works as a streaming query fails the drift check, just as its `--full-refresh` fails. A dry-run that fails (for example, invalid SQL) fails the run.

The adapter falls back to a short-lived temporary table (named `__dbt_tmp_schema_check_<model>`) where the dry-run can't stand in for it: for `streaming_source`, whose column definitions aren't a SELECT; when the dry-run reports no result schema; when the SELECT produces duplicate column names; and when a column type isn't one whose `INFORMATION_SCHEMA` spelling the adapter has verified (for example a type added to Flink after this adapter release), rather than risk reporting false drift. For `table` and `streaming_table`, the temp table is created from the model's SELECT query; for `streaming_source`, from the model's column definitions (without the connector). The temp table is included in the same metadata query and dropped in the adapter's post-model hook, which dbt invokes even when the materialization fails (e.g. when drift is detected), so a run that raises after creating the temp table doesn't leak it. As a backstop for runs that die hard (killed process, lost connectivity) before the hook runs, the temp table name is deterministic per model and the next drift check reclaims any leftover: the fallback drops it before creating a new one, and the dry-run path drops it too.

#### Configuration

Control drift detection behavior with the `on_schema_drift` config:

```sql
{{ config(
    materialized='table',
    on_schema_drift='fail'  -- 'fail' (default) or 'ignore'
) }}
```

**Options**:
- `fail` (default) - Raise an error if schema drift is detected
- `ignore` - Skip drift detection entirely; always skip if the table exists

**Example**:
```sql
-- Disable drift detection for a specific model
{{ config(
    materialized='streaming_table',
    on_schema_drift='ignore'
) }}
select * from {{ ref('source') }}
```

#### Column Drift
- **table, streaming_table**: Compares existing column names and data types with expected columns from the SELECT query, resolved by a dry-run (or the temp-table fallback). Raises an error if columns are added, removed, renamed, or if data types change. Column reordering is allowed (order doesn't matter for Kafka-backed tables).
- **streaming_source**: Compares existing column names and data types with the column definitions in the model SQL. Raises an error if columns are added, removed, renamed, or if data types change. Uses a temporary table to infer schema from SQL column definitions.

#### Distribution Drift
Compares the user-specified `config(distributed_by={...})` against the existing distribution from `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`. Raises an error if the column list or column order differ, or if the bucket count differs when it was explicitly specified.

**Important limitation**: As with WITH options, the adapter only verifies what the user explicitly requested. If `distributed_by` is unset, drift detection is skipped entirely, because Confluent assigns a default distribution (typically derived from the primary key) to every Kafka-backed table, and INFORMATION_SCHEMA does not distinguish user-specified from auto-assigned distribution. Note that you cannot truly *remove* a distribution: every Kafka-backed table has one. To stop the adapter from comparing against a previously-set `distributed_by`, drop the config and use `--full-refresh` to recreate the table. Confluent will then assign its default distribution.

#### WITH Options Drift
Compares existing `WITH` options against the model's `config(with={...})`. Raises an error if any configured option value has changed. For `streaming_source`, the mandatory `config(connector='...')` is included in this comparison (it is rendered as the `connector` WITH option), so changing the connector is detected as drift.

**Important limitation**: The adapter only verifies that user-specified options exist with the correct values. It does **not** detect when options are removed from the config, because connectors may add default options automatically (e.g., `fields.*.expression` from the faker connector), and we cannot distinguish between user-specified and auto-generated options.

**Example of undetected drift**:
```sql
-- Initial config
config(with={'changelog.mode': 'upsert'})

-- Changed to (option removed)
config(with={})

-- Result: The table still has changelog.mode=upsert, but dbt will skip (no error)
```

If you need to change or remove WITH options, use `--full-refresh` to drop and recreate the table.

#### Query Logic Changes

Schema drift detection only inspects **column names, data types, WITH options, and `distributed_by`**; it does not detect changes to the query logic itself. If you modify how a column is computed without changing its name or type, the adapter will see no drift and skip the model.

**Example of undetected change**:
```sql
-- Initial model
select order_id, round(price, 2) as price from {{ ref('source') }}

-- Changed to (different rounding)
select order_id, round(price, 4) as price from {{ ref('source') }}

-- Result: Column name and type are unchanged, so dbt will skip (no error)
```

This is an inherent limitation: `INFORMATION_SCHEMA` only stores schema metadata, not the query that produced the table. If you change query logic, use `--full-refresh` to recreate the table.

#### When Drift is Detected
If drift is detected, the run will fail with a compilation error. Use `--full-refresh` to drop and recreate the table with the new schema or options.

### Deterministic Statement Names

Each materialization that submits statements creates Flink statements with deterministic names derived from the dbt project and model names. `ephemeral` submits no statement and therefore has no statement name:

```
{prefix}{project_name}-{model_name}
```

The default prefix is `dbt-`. For `streaming_table`, which creates two statements (a DDL and an INSERT), the DDL gets a `-ddl` suffix: `dbt-{project}-{model}-ddl`.

`materialized_table` differs in two ways. Its defining `CREATE OR ALTER` statement completes immediately, since it is not a long-running maintainer (Flink maintains the table server-side), so the adapter reaps it like any other bounded statement; a failed submission is left in place for debugging (Confluent purges terminal statements after ~30 days). And each run submits under a unique per-run name (`dbt-{project}-{model}-{invocation_id}`), so a re-assert can never collide (409) with a statement lingering from a previous run.

#### Flink Naming Constraints

Flink statement names must contain only lowercase alphanumeric characters and hyphens, start with an alphanumeric character, and be at most 100 characters long. The adapter sanitizes names automatically:

- Illegal characters (including underscores) are replaced with hyphens
- A 6-char MD5 hash suffix is appended when characters are replaced, to avoid collisions (e.g. `my_model` and `my.model` produce different names)
- Names exceeding 100 characters are truncated with a 6-char hash suffix

#### Custom Statement Names

Override the generated name with the `statement_name` config:

```sql
{{ config(materialized='streaming_table', statement_name='my-custom-name') }}
```

#### Statement Lifecycle

On `--full-refresh`, the adapter deletes existing statements before dropping and recreating the table. When no relation exists, orphaned statements are also cleaned up.

For `streaming_table`, the adapter additionally checks the long-running INSERT statement on every re-run. If the statement is missing (e.g. the process crashed between DDL and DML, or the statement was deleted externally) or in a terminal phase (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), `dbt run` resubmits **only the INSERT** under the same deterministic name. The table and its topic state are preserved, and no `--full-refresh` is required. A `RUNNING` statement, in-flight transitions (`PENDING`, `STOPPING`, `DELETING`), and `DEGRADED` are treated as healthy: the adapter does not interrupt them.

For `streaming_source`, automatic recovery is **not** supported: the CREATE statement also attaches the connector, and Flink does not allow re-attaching a connector to an existing table. If the connector statement is dead, run with `--full-refresh` (which drops and recreates the table). Tracked as a follow-up.
