# Materializations

## Table of Contents

- [Introduction](#introduction)
- [Supported Materializations](#supported-materializations)
- [Unsupported Materializations](#unsupported-materializations)
- [Materializations Reference](#materializations-reference)
  - [`materialized_table`](#materialization-materialized_table)
  - [`streaming_table`](#materialization-streaming_table)
  - [`streaming_source`](#materialization-streaming_source)
  - [`table`](#materialization-table)
  - [`view`](#materialization-view)
  - [`ephemeral`](#materialization-ephemeral)
- [Model Configuration](#model-configuration)
  - [Validation](#validation)
  - [Common Config Options](#common-config-options)
- [Common Mechanics](#common-mechanics)
  - [Schema Drift Detection](#schema-drift-detection)
  - [Deterministic Statement Names](#deterministic-statement-names)

## Introduction

The materializations in dbt-confluent cover both batch and streaming use cases.

Some materializations behave like their counterparts from dbt adapters for traditional data warehouses, e.g.:

- [`table`](#materialization-table)
- [`view`](#materialization-view)
- [`ephemeral`](#materialization-ephemeral)

The **streaming materializations** use potentially long-running Flink statements or connector processes to produce continous results:

- [`materialized_table`](#materialization-materialized_table)
- [`streaming_table`](#materialization-streaming_table)
- [`streaming_source`](#materialization-streaming_source)

All materializations use [Tables](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html) for their storage needs,
which are backed by Apache Kafka topics. This storage can be augmented with features like [Tableflow](#tableflow).

The most important difference between streaming materializations and other materializations you may be used to is about **statefulness**.
Because the streaming materializations can produce continuous results or have online consumers, you will need to consider:

- **Job state.** Many materializations are backed by a Flink job with its own in-memory
  state (aggregations, joins, windows). Some changes force that state to be discarded and rebuilt.
  For example, see [`materialized_table` evolution](#materialization-materialized_table).

- **Schema evolution.** Models may have any number of streaming consumers, so schema compatibility
  must be considered across points of evolution to avoid affecting live downstream consumers.
  Some schema changes may require recreating the backing storage to clear incompatible records.
  Because these changes can be destructive, the adapter provides explicit control through e.g.
  [Schema Drift Detection](#schema-drift-detection).

- **Topic identity.** "Recreating" a table doesn't just replace a schema, it creates a topic under a
  new identity, so anything still reading the old topic's offsets doesn't automatically follow.

## Supported Materializations

_The table below summarizes all materializations supported by the dbt-confluent adapater._

| Materialization | Description |
|---|---|
| [`materialized_table`](#materialization-materialized_table) | Declarative `CREATE OR ALTER MATERIALIZED TABLE`. The standard materialization for continuous stream processing |
| [`streaming_table`](#materialization-streaming_table) | DDL plus a long-running `INSERT INTO ... SELECT`. The precursor to [`materialized_table`](#materialization-materialized_table). |
| [`streaming_source`](#materialization-streaming_source) | A connector-backed source table, e.g. `faker`. |
| [`table`](#materialization-table) | One-shot `CREATE TABLE ... AS SELECT` (CTAS). |
| [`view`](#materialization-view) | A named query inlined into consumers, not a persisted result. |
| [`ephemeral`](#materialization-ephemeral) | Standard dbt CTE fragment. |

## Unsupported Materializations

_Some standard dbt materializations are not supported by this adapter._

| Materialization | Reason |
|---|---|
| `materialized_view` | dbt's built-in `materialized_view` materialization is not implemented. For a Flink materialized table use [`materialized_table`](#materialization-materialized_table). |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use [`materialized_table`](#materialization-materialized_table) instead. |

## Materializations Reference

### Materialization: `materialized_table`

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
Every run submits the same kind of statement, and Flink reconciles the table's actual state to match
it, rather than dbt-confluent choosing between a drop/recreate and a schema-drift-based skip the way
`table`/`streaming_table`/`streaming_source` do.

See Confluent's [materialized tables](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html)
concept page for the underlying feature.

#### `materialized_table`: Config Options

| Config | Description |
|---|---|
| `distributed_by` | See [Distributed By](#distributed-by). Fixed at creation; changing it requires `--full-refresh`. |
| `with` | Table options, e.g. `{'key.format': 'avro-registry'}`. |
| `start_mode` | Where the query starts (or, on an in-place evolution, restarts) reading; see [`start_mode`](#materialized_table-start_mode) below. |
| `statement_properties` | See [Statement Properties](#statement-properties). |
| `tableflow` | See [Tableflow](#tableflow). Checked on every run: create or in-place evolution alike. |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). Details below. |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |

Each run already uses a unique per-run statement name regardless of `statement_name` (see [Statements Emitted](#materialized_table-statements-emitted) below); a configured `statement_name` becomes the base that per-run suffix is derived from.

`freshness_interval`, `refresh_mode`, and `partition_by` exist in open-source Flink but not in Confluent's dialect; they raise a compile error. Any other dbt-confluent config key this materialization doesn't read (e.g. `connector`, `on_schema_drift`) is also rejected; see [Validation](#validation).

#### `materialized_table`: `start_mode`

Controls where the query starts (or, on an in-place evolution, restarts) reading. Default: `RESUME_OR_FROM_BEGINNING`. Accepted values are::

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

`start_mode` also governs what happens to a *stateful* query's results when the table [evolves](#materialized_table-evolution--state-impact) in place.

#### `materialized_table`: Contracts and Primary Keys

With `config(contract={'enforced': true})` and an explicit `columns:`/`constraints:` block in the model's schema.yml, `materialized_table` renders an explicit column-definition list ahead of `DISTRIBUTED BY`/`WITH`/`START_MODE`:

```sql
CREATE OR ALTER MATERIALIZED TABLE <relation> (<cols>, PRIMARY KEY (...) NOT ENFORCED)
  ... AS SELECT ...
```

- The `PRIMARY KEY (...) NOT ENFORCED` clause comes from a model-level `primary_key` constraint, matching Confluent's materialized-table grammar.
- This is what makes the resulting table usable in **snapshot queries** against its key.
- As with `table` (see [Validation](#validation)), the contract's declared columns are checked against the model's compiled SQL, and a mismatch fails the run before any DDL is submitted.

Without an enforced contract, the materialization renders a plain `AS SELECT` with no explicit column list.

#### `materialized_table`: Evolution / State Impact

`materialized_table` behaves differently depending on what changes between runs:

| Scenario | What happens |
|---|---|
| New table | Created. |
| Existing table (changed or not) | Evolved **in place**: the table and its topic are kept, but the running query is stopped and replaced. |
| `--full-refresh` | `DROP MATERIALIZED TABLE IF EXISTS`, then recreate. |

**In-place evolution**, in more detail: the Flink job clears its internal state, resetting any aggregations, window, or join state. It then begins (re-)processing data according to the configured [`start_mode`](#materialized_table-start_mode):

- Under a `RESUME_*` start mode (the default, `RESUME_OR_FROM_BEGINNING`, is one of these), stateless queries (projections, filters) evolve seamlessly: no reprocessing, no duplicates.
- For *stateful* queries, evolution recalculates results from a clean slate rather than adjusting the old ones. Depending on `start_mode`, that recalculation may not cover the same source data as before, so joins, aggregations, and other stateful results can shift (e.g. an aggregation resuming from an offset instead of replaying history will look "undercounted"). See [Confluent's materialized tables concepts page](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html#controlling-reprocessing-with-start-mode) for evolution semantics and caveats.

**`--full-refresh`** is required to change `distributed_by` (fixed at creation), to apply changes an evolution rejects, and to rebuild correct results for a stateful model whose `start_mode` resumes from offsets.

**Warning:** dropping a materialized table (including via `--full-refresh`) permanently deletes the backing Kafka topic and all of its data.

**Known limitation: every run evolves, even when nothing changed.** Confluent's own
[`CREATE OR ALTER MATERIALIZED TABLE` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
states plainly that the statement is **not idempotent**: "running the same `CREATE OR ALTER` command
always triggers a new evolution, even if nothing has changed," and that evolution "always" discards
all existing Flink processing state. Because dbt-confluent always submits the full `... AS SELECT`
form of this statement on every run (see [Statements Emitted](#materialized_table-statements-emitted) below), a plain
`dbt run` against an unchanged `materialized_table` model resets its Flink state just like a real
definition change would. There is currently no true no-op path for this materialization. Confluent's
grammar also exposes a lighter, property-only `ALTER MATERIALIZED TABLE` form (no `AS SELECT`) that
does **not** trigger evolution, but dbt-confluent does not use it today. This is a dbt-confluent
limitation, not a Confluent Cloud Flink one, and is worth planning around for stateful models that
run frequently.

**Evolution limits**: not every change can evolve in place. Dropping columns is rejected at submission, observed either as a per-column error ("dropping a non-nullable, persisted column is not supported") or as a query/sink schema mismatch ("Column types of query result and sink ... do not match. Cause: Different number of columns."). The fix is `--full-refresh`. (Materialized tables don't use [schema drift detection](#schema-drift-detection); Flink reconciles the definition instead, and a rejected evolution is the analogous failure mode.)

#### `materialized_table`: Statements Emitted

Every run, whether the table doesn't exist yet, exists unchanged, or exists with a different
definition, submits the same statement:

```sql
CREATE OR ALTER MATERIALIZED TABLE <relation> [(<cols>, PRIMARY KEY (...) NOT ENFORCED)]
  [DISTRIBUTED BY (...)] [WITH (...)] [START_MODE = ...] AS <model SELECT>
```

This is never a bare `ALTER MATERIALIZED TABLE`, even when only a `with` option changes and nothing
about the query, columns, or distribution does.
Each run submits under a unique per-run statement name (see
[Deterministic Statement Names](#deterministic-statement-names)), so a re-assert can never collide
with a statement left over from a previous run.
See the [`CREATE OR ALTER MATERIALIZED TABLE` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-or-alter-materialized-table.html)
for the full DDL grammar.

#### `materialized_table`: Switching Materializations

An existing regular table or view cannot be converted to a materialized table, and a materialized table cannot be adopted by the drop-and-recreate materializations. The adapter detects both switches before submitting anything, and both resolve the same way:

- *To* `materialized_table`: a plain run fails with guidance; `--full-refresh` drops the existing relation (and its Flink statements) through the regular drop path, then creates the materialized table.
- *From* `materialized_table` (model changed to `table`/`streaming_table`/`streaming_source`): a plain run fails with guidance. This matters because a materialized table looks like a regular table to the catalog, and without the check an unchanged model would silently "succeed" while Flink kept maintaining the old defining query. `--full-refresh` drops the materialized table (via `DROP MATERIALIZED TABLE`), **but the recreate under the same name is then blocked server-side**: the drop does not remove the MT's Schema Registry subjects, and the keyed schema the MT registered doesn't match the schema the new relation would register (a `table` snapshot CTAS is keyless), so creation fails with "Schema Registry subject ... doesn't match the existing one" and the adapter's appended recovery guidance. To complete the switch, delete the lingering `<name>-key`/`<name>-value` subjects in Schema Registry and re-run (`dbt retry` works, since the MT is already gone), or give the model a different relation name via `alias`. Note the drop permanently deletes the backing topic and its data, and `on_schema_drift='ignore'` skips this detection along with the rest of the drift check.

Re-running while Flink is still establishing a freshly created or evolved table can be transiently rejected (`being modified`); the window is brief and the adapter retries automatically.

---

### Materialization: `streaming_table`

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

`streaming_table` creates a table, then runs a separate, continuous `INSERT INTO ... SELECT`
statement to populate it.
This two-statement approach is currently the preferred way to build streaming pipelines.
<!-- TODO: sync with Zander on revising the "until materialized tables reach GA" framing here -->
It supports table options via `config(with={...})`.

See Confluent's [dynamic tables and continuous queries](https://docs.confluent.io/cloud/current/flink/concepts/dynamic-tables.html)
concept page for the underlying execution model.

#### `streaming_table`: Config Options

| Config | Description |
|---|---|
| `with` | Table options baked into the DDL statement; this is the DDL-side equivalent of `statement_properties` below. |
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). The DDL gets a `-ddl` suffix appended to this name. |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `statement_properties` | See [Statement Properties](#statement-properties). Applies only to the INSERT statement, not the CREATE TABLE DDL; for the DDL side, use `with` instead. |
| `tableflow` | See [Tableflow](#tableflow). |

#### `streaming_table`: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `streaming_table` runs [schema
drift detection](#schema-drift-detection) (columns, `WITH` options, and `distributed_by`) before
deciding whether to skip or restart.
Separately, on every re-run, the adapter checks the long-running INSERT statement itself.
If it's missing (e.g. the process crashed between DDL and DML, or the statement was deleted
externally) or in a terminal phase (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), `dbt run` resubmits
**only the INSERT** under the same deterministic name. The table and its topic state are preserved,
and no `--full-refresh` is required.
A `RUNNING` statement, an in-flight transition (`PENDING`, `STOPPING`, `DELETING`), or `DEGRADED` is
treated as healthy: the adapter does not interrupt it.

#### `streaming_table`: Statements Emitted

`streaming_table` submits two statements:

```sql
CREATE TABLE <relation> [DISTRIBUTED BY (...)] [WITH (...)]     -- DDL, bounded, name suffixed "-ddl"
INSERT INTO <relation> <model SELECT>                           -- DML, long-running
```

The DDL completes immediately; the INSERT is a genuinely long-running, continuous statement. This is
the two-statement split that gives `streaming_table` its name.
See the [`CREATE TABLE`](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
and [`INSERT INTO ... FROM SELECT`](https://docs.confluent.io/cloud/current/flink/reference/queries/insert-into-from-select.html)
references.

#### `streaming_table`: Adopting Existing Resources

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

### Materialization: `streaming_source`

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

#### `streaming_source`: Config Options

| Config | Description |
|---|---|
| `connector` (required) | The connector to attach; see the [faker sample-data how-to guide](https://docs.confluent.io/cloud/current/flink/how-to-guides/custom-sample-data.html). |
| `with` | Additional connector options. |
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `tableflow` | See [Tableflow](#tableflow). |

#### `streaming_source`: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `streaming_source` runs [schema
drift detection](#schema-drift-detection) against the model's column definitions (there's no SELECT
query to infer a schema from).
Recovery works differently from `streaming_table`: automatic recovery is **not** supported here,
because the CREATE statement also attaches the connector, and Flink doesn't allow re-attaching a
connector to an existing table.
If the connector statement is dead, a plain run skips it with a logged warning rather than
restarting it. You must run with `--full-refresh` (which drops and recreates the table, permanently
deleting its data) to fix it.

#### `streaming_source`: Statements Emitted

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

### Materialization: `table`

```sql
-- table_example.sql

{{ config(
    materialized='table',
    with={'changelog.mode': 'upsert'},
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

#### `table`: Config Options

| Config | Description |
|---|---|
| `with` | Table options baked into the `CREATE TABLE` DDL, e.g. `{'changelog.mode': 'upsert'}`. |
| `distributed_by` | See [Distributed By](#distributed-by). |
| `on_schema_drift` | See [Schema Drift Detection](#schema-drift-detection). |
| `statement_name` | See [Deterministic Statement Names](#deterministic-statement-names). |
| `compute_pool_id` | See [Compute Pool](#compute-pool). |
| `tableflow` | See [Tableflow](#tableflow). |
| `ignore_unsupported_config` | See [Validation](#validation). |

#### `table`: Schema Drift / Reconciliation Behavior

If the table already exists and `--full-refresh` is not specified, `table` skips creation after
running [schema drift detection](#schema-drift-detection) (columns, `WITH` options, and
`distributed_by`).
Use `--full-refresh` to drop and recreate the table.
This permanently deletes the backing Kafka topic and all of its data.

#### `table`: Statements Emitted

`table` submits a single statement:

```sql
CREATE TABLE <relation> [DISTRIBUTED BY (...)] [WITH (...)] AS (<model SELECT>)
```

The statement runs in Confluent Cloud Flink's
[snapshot execution mode](https://docs.confluent.io/cloud/current/flink/concepts/snapshot-queries.html):
it reads a point-in-time view of its sources via Flink's batch execution mode, and "runs, returns
results, and then exits," rather than running continuously.

See the [`CREATE TABLE` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
for the full DDL grammar.

---

### Materialization: `view`

```sql
-- view_example.sql

{{ config(materialized='view') }}

select customer_id, order_id, price
from {{ ref('orders') }}
where price > 0
```

A view is a named query definition: Flink inlines its SQL into whatever job(s) actually query it, at
those jobs' own execution time, rather than running any compute of its own.
Unlike every other Kafka-backed materialization on this page, `view` doesn't create a topic that stores data.
Creating a view does reserve a special Kafka topic name, but that topic is metadata-only and never
holds records. This differs from `ephemeral`, which creates no topic at all.

See Confluent's [`CREATE VIEW` reference](https://docs.confluent.io/cloud/current/flink/reference/statements/create-view.html)
for the underlying statement.

#### `view`: Config Options

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

#### `view`: Statements Emitted

`view` submits two statements, unconditionally, on every single `dbt run`. Unlike every other
materialization on this page, there is no diffing and no skip-if-unchanged path here:

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

### Materialization: `ephemeral`

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

This sections containts documentation for configurations options that apply to multiple materialization.
Each per-materialization section links back to the specific subsections below that it supports.

### Validation

Setting a dbt-confluent config key on a materialization that doesn't use it fails the run immediately with a clear error, rather than silently doing nothing. For example, `config(materialized='table', statement_properties={...})` fails at compile time (`statement_properties` is only read by `streaming_table` and `materialized_table`), instead of the value being silently ignored.

This only ever checks dbt-confluent's own config keys (`with`, `distributed_by`, `connector`, `on_schema_drift`, `statement_name`, `compute_pool_id`, `statement_properties`, `start_mode`, `tableflow`) against the materialization you're using. Any other config key, including your own custom keys read by your own hooks or macros, is never inspected and never affected by this check.

If a key name genuinely collides with one of dbt-confluent's own (an unlikely but possible coincidence), opt it out per model with `ignore_unsupported_config`:

```sql
{{ config(
    materialized='table',
    statement_properties={'my_custom_key': 'value'},  -- not really ours; used by a custom macro
    ignore_unsupported_config=['statement_properties'],
) }}
```

`ignore_unsupported_config` takes a list of specific key names, not a blanket on/off switch. Opting out of one false positive doesn't also suppress a real mistake on a different key in the same model.

### Common Config Options
#### Distributed By

Confluent Flink lets you control how a table's rows are distributed across Kafka partitions with a `DISTRIBUTED BY HASH(...) INTO N BUCKETS` clause in the `CREATE TABLE` DDL.
The adapter exposes this through a `distributed_by` config on [`table`](#materialization-table), [`streaming_table`](#materialization-streaming_table), [`streaming_source`](#materialization-streaming_source), and [`materialized_table`](#materialization-materialized_table) models:

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

**Fields**:
- `columns` (required) - non-empty list of column names used to compute the hash
- `buckets` (optional) - positive integer; omit to let Confluent Cloud choose

**Validation**: The adapter validates the config at the start of each materialization run and raises a clear compile error if any of the following hold:
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

#### Compute Pool

By default, every statement runs on the compute pool configured in your profile (`compute_pool_id`). You can override the pool per model, for example to isolate a heavy model or to manage resources, with the `compute_pool_id` config, available on every materialization that submits a statement: [`table`](#materialization-table), [`view`](#materialization-view), [`materialized_table`](#materialization-materialized_table), [`streaming_table`](#materialization-streaming_table), and [`streaming_source`](#materialization-streaming_source).

```sql
{{ config(materialized='streaming_table', compute_pool_id='lfcp-abc123') }}
```

The override applies to all statements a model submits (DDL, the long-running INSERT, and metadata/drift-check queries). The pool must exist in the same environment and region as the profile, and the API key used must have access to it; Confluent Cloud validates this at submission time. When `compute_pool_id` is omitted, the profile default is used. If the profile sets no default pool either, Confluent Cloud Flink runs the statement on the environment+region default pool.

Statement recovery and cleanup (see [Statement Lifecycle](#statement-lifecycle)) operate by statement name and are pool-agnostic: a model's statement is found, inspected, and, when dead, resubmitted on the model's configured pool regardless of the profile default.

Changing `compute_pool_id` on an existing, healthy model takes effect **only** on the next `--full-refresh` or statement restart. A running statement is not migrated to a new pool, since the pool is a property of the statement (not the table) and isn't part of drift detection.

##### Per-environment pools in CI/CD

The same model is often deployed to different compute pools across environments (dev / staging / prod) or regions. Rather than hard-coding a pool, inject it at deploy time with an environment variable:

```sql
{{ config(materialized='streaming_table', compute_pool_id=env_var('FLINK_COMPUTE_POOL')) }}
```

Your CI/CD pipeline sets `FLINK_COMPUTE_POOL` (and typically `statement_name`) per target, keeping a single Git source of truth.

#### Statement Properties

Set Flink SET-style statement properties, such as `sql.tables.scan.idle-timeout`, with the `statement_properties` config, available on the [`streaming_table`](#materialization-streaming_table) and [`materialized_table`](#materialization-materialized_table) materializations:

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

#### Tableflow

[Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) materializes the Kafka topic backing a Flink table as an Apache Iceberg and/or Delta Lake table in object storage. That table can be [queried by external engines](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/overview.html) like Snowflake and Trino, and, as of this writing in Open Preview, by [Confluent Cloud Flink itself](https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-flink.html) via a [snapshot query](#materialization-table) against the same `sql.snapshot.mode` mechanism `table` uses. Enabling Tableflow is done through a dedicated [Confluent Cloud control-plane API](https://docs.confluent.io/cloud/current/ccloud/list-tableflow-v-1-tableflow-topics/) (`POST`/`GET`/`DELETE /tableflow/v1/tableflow-topics`) rather than Flink SQL DDL, so the adapter drives it directly through the `confluent-sql` driver rather than through the model's own statements.

Available on every materialization that owns a real Kafka-backed table ([`table`](#materialization-table), [`streaming_table`](#materialization-streaming_table), [`streaming_source`](#materialization-streaming_source), [`materialized_table`](#materialization-materialized_table)) via the `tableflow` config:

```sql
{{ config(
    materialized='table',
    tableflow={
        'table_formats': ['ICEBERG'],
        'storage': {'kind': 'Managed'},
    }
) }}
select order_id, customer_id, price from {{ ref('orders') }}
```

**Fields**:
- `table_formats` (required) — `'ICEBERG'`, `'DELTA'`, or a list containing either or both.
- `storage` (required) — a mapping with a `kind` key, using Tableflow's own API names verbatim (see the [storage configuration guide](https://docs.confluent.io/cloud/current/topics/tableflow/concepts/tableflow-storage.html)):
    - `{'kind': 'Managed'}` — Confluent-managed storage, no further config.
    - `{'kind': 'ByobAws', 'bucket_name': '...', 'provider_integration_id': '...'}` — bring-your-own S3 bucket.
    - `{'kind': 'AzureDataLakeStorageGen2', 'storage_account_name': '...', 'container_name': '...', 'provider_integration_id': '...'}` — customer-owned Azure Data Lake Storage Gen2.
- `config` (optional) — topic-level settings, mirroring Tableflow's own `spec.config` nesting verbatim rather than a flattened dbt-invented shape, so a `config` block copied straight from the API spec, the `confluent` CLI's own payload, or `confluent_sql` works unchanged:
    - `retention_ms` / `data_retention_ms` (optional) — non-negative integers (or numeric strings) controlling snapshot/data retention.
    - `error_handling` (optional) — how a bad record is handled: `{'mode': 'SUSPEND'}` (the server default, suspends materialization), `{'mode': 'SKIP'}` (skip and continue), or `{'mode': 'LOG', 'target': '...'}` (log to a dead-letter target, `target` defaults to `'error_log'`).

The adapter validates this shape (`CompilationError` on a malformed `tableflow` config) when it's actually applied. Unlike `distributed_by`/`start_mode`, `tableflow` is never baked into this DDL, so a bad value can't doom a `--full-refresh` recreate, and there's no need to validate it any earlier.

**Ensured on every run.** Whenever a model configures `tableflow`, every run (whether the relation was just created, already existed, or is being restarted) checks Tableflow's live state and:
- **Not enabled** — enables it with the current config.
- **Already enabled** — diffs the live configuration against what's now configured and applies an in-place update only if `table_formats`/`config` actually changed, so an unchanged config is a true no-op rather than cycling the backing materialization job every run. `storage` can't be changed in place (Tableflow's API doesn't support it), but a change there is still detected and handled automatically: the adapter disables and re-enables Tableflow with the new storage config. This only ever touches the Tableflow sink, never the underlying Kafka topic or its data, unlike `--full-refresh`, which drops and recreates the topic itself. Re-enabling backfills the full topic history from the earliest offset, so this doesn't leave a coverage gap.

    One specific storage transition, a custom bucket (`ByobAws`/`AzureDataLakeStorageGen2`) to Confluent-managed storage, can fail even after the automatic disable/re-enable above completes: Confluent Cloud enforces a grace period after disabling before it accepts the switch to managed storage, with no status to poll for when it's actually done. A run that hits this fails with a clear error rather than hanging. Re-run the model after waiting, or use `--full-refresh` to succeed immediately (it builds a new Kafka topic with no prior-storage history to check against, but drops and recreates the topic, wiping its existing data).

    A config that matches (nothing to PATCH) still checks the topic's [phase](https://docs.confluent.io/cloud/current/topics/tableflow/operate/monitor-tableflow.html): if it has **FAILED**, most commonly a poison-pill record suspending the materialization under `error_handling: {mode: 'SUSPEND'}` (the server default), the run logs a warning naming the error detail, rather than reporting success while Tableflow is actually dead with no way to notice. `PENDING` is not flagged; it's a normal state to be caught in, not a problem. This is a warning, not a failure: resuming a suspended topic (via the read-only `suspended` field) isn't something dbt patches, or should attempt automatically. Deciding whether a suspended record is safe to reprocess is a human call.

If `tableflow` is unset in the model, nothing is ever checked or touched, regardless of live state. This also means a table Tableflow was enabled on outside of dbt is never flagged just because the model doesn't mention it.

**Disabled automatically before `--full-refresh`, when the *new* config still sets `tableflow`.** Before dropping a relation for a full-refresh, the adapter checks live Tableflow state (via a live `GET`, not the config value) and disables it first if present, so the drop doesn't race an active materialization (confluent-sql's own recommendation). This is deliberately gated on the *current* model's config, not on live state alone: checking live state before every drop, including drops on models that have never used `tableflow`, would require every profile to supply a Global API key just to run `--full-refresh` at all.

The corollary: if a model's `tableflow` config is removed (rather than the table being dropped outright), an old Tableflow configuration left enabled on that relation is not disabled and is not touched on subsequent runs. If that relation is later full-refreshed, the drop is **not** preceded by a disable, since the new config no longer mentions `tableflow`. This can race Tableflow the same way an unguarded drop would. Explicitly turning Tableflow off (without dropping the table) is not yet supported; it's tracked as follow-up work.

**Credentials**: Tableflow's control-plane routes require a Global API key (`global_api_key` / `global_api_secret`); they resolve `database` to a Kafka cluster id via CMK, which only a Global key can do. A model that configures `tableflow` on a profile without one raises a clear error naming these fields, rather than the raw driver error.

## Common Mechanics

Background on adapter behavior that spans multiple materializations.

### Schema Drift Detection

**Scope:** this only applies to [`table`](#materialization-table),
[`streaming_table`](#materialization-streaming_table), and
[`streaming_source`](#materialization-streaming_source), the materializations that still use a
drop-and-recreate-or-skip lifecycle. [`materialized_table`](#materialization-materialized_table)
doesn't use this at all; Flink reconciles the table definition natively instead (see [Evolution /
State Impact](#materialized_table-evolution--state-impact)), which is the direction this adapter is
moving toward. `view` and `ephemeral` have no persistent schema to check.

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation. The check compares **columns**, **WITH options**, and **`distributed_by`** in a single pass and raises one error listing every violation, so you don't have to fix them one at a time. It also detects when the existing relation is a **materialized table** (a reverse materialization switch) and fails with dedicated guidance instead of a drift list; see [Switching materializations](#materialization-materialized_table). (`materialized_table` models themselves do not use drift detection; Flink reconciles the re-asserted definition instead. See [Materialized Table](#materialization-materialized_table).)

To determine the expected schema, the adapter creates a short-lived temporary table (named `__dbt_tmp_schema_check_<model>`) and issues a single `UNION ALL` query against `INFORMATION_SCHEMA.COLUMNS`, `TABLES`, and `TABLE_OPTIONS` to fetch every piece of metadata at once. For `table` and `streaming_table`, the temp table is created from the model's SELECT query; for `streaming_source`, from the model's column definitions (without the connector). The temp table is dropped in the adapter's post-model hook, which dbt invokes even when the materialization fails (e.g. when drift is detected), so a run that raises after creating the temp table doesn't leak it. As a backstop for runs that die hard (killed process, lost connectivity) before the hook runs, the temp table name is deterministic per model and the check drops any leftover before creating a new one, so the next drift check reclaims a leak.

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
- **table, streaming_table**: Compares existing column names and data types with expected columns from the SELECT query. Raises an error if columns are added, removed, renamed, or if data types change. Column reordering is allowed (order doesn't matter for Kafka-backed tables).
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

Schema drift detection only inspects **column names, data types, and WITH options**; it does not detect changes to the query logic itself. If you modify how a column is computed without changing its name or type, the adapter will see no drift and skip the model.

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

Each materialization creates Flink statements with deterministic names derived from the dbt project and model names:

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
