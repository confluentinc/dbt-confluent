# Materializations

## Supported

| Materialization | Description |
|---|---|
| `table` | Creates a table via `CREATE TABLE ... AS SELECT` (CTAS). Runs in snapshot mode — the query executes once and completes. If the table already exists, checks for schema drift (column names, data types, WITH options, `distributed_by`) and skips creation (use `--full-refresh` to drop and recreate). |
| `view` | Drop-and-recreate view. |
| `streaming_table` | Creates a table then runs a separate continuous `INSERT INTO ... SELECT` statement. This two-statement approach is currently the preferred way to build streaming pipelines (until Flink's materialized table feature reaches GA). Supports table options via `config(with={...})`. If the table already exists, checks for schema drift (column names, data types, WITH options, `distributed_by`) and skips creation (use `--full-refresh` to drop and recreate). |
| `materialized_table` | Creates and maintains a Flink materialized table via `CREATE OR ALTER MATERIALIZED TABLE ... AS SELECT`. Each run re-asserts the definition and Flink reconciles it: a new table is created, any change (columns, data types, `WITH` options, or query logic) is evolved **in place**, and an unchanged definition is a server-side no-op. `--full-refresh` drops and recreates (permanently deleting the backing topic and its data). Supports `config(distributed_by={...}, with={...}, start_mode='...')`. |
| `streaming_source` | Creates a connector-backed source table (e.g., Datagen). Requires `config(connector='...')`. The model SQL defines the column definitions. Supports additional connector options via `config(with={...})`. If the table already exists, checks for schema drift (column names, data types, WITH options, `distributed_by`) and skips creation (use `--full-refresh` to drop and recreate). See the [Confluent connector catalog](https://docs.confluent.io/cloud/current/connectors/index.html) and [Flink CREATE TABLE documentation](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html) for available connectors and options. |
| `ephemeral` | Standard dbt CTE-based query fragment, not materialized in Flink. |

## Distributed By

Confluent Flink lets you control how a table's rows are distributed across Kafka partitions with a `DISTRIBUTED BY HASH(...) INTO N BUCKETS` clause in the `CREATE TABLE` DDL. The adapter exposes this through a `distributed_by` config on `table`, `streaming_table`, and `streaming_source` models:

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

**Important — column ordering**: Flink requires that the distribution columns appear at the **beginning** of the table's column schema, and in the **same order** as listed in `columns`. The adapter does not validate this (it would require parsing the model SQL) — Flink will reject the `CREATE TABLE` at submission with `Key columns must appear at the beginning of the table schema. Also, DISTRIBUTED BY key names must be in the same order as the key schema columns.`

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

## Materialized Table

`materialized_table` is declarative: every run issues `CREATE OR ALTER MATERIALIZED TABLE` and lets Flink reconcile the table, rather than using the drop/recreate + schema-drift flow that `table`/`streaming_table` use.

- **New table** — created.
- **Unchanged** — a server-side no-op: Confluent diffs the submitted definition against the current one and leaves the table, its data, and its query state untouched. Re-asserting the definition on every run is safe and is the design. (Confluent's docs currently describe every `CREATE OR ALTER` as triggering an evolution; the server's observed behavior is the no-op described here.)
- **Any change** (columns, data types, `WITH` options, or query logic) — evolved **in place**: the table and its topic are kept. For *stateless* queries (projections, filters) the evolution is seamless — no reprocessing, no duplicates. For *stateful* queries, see the warning below. See [Confluent's materialized tables concepts page](https://docs.confluent.io/cloud/current/flink/concepts/materialized-tables.html) for evolution semantics and caveats.
- **`--full-refresh`** — `DROP MATERIALIZED TABLE IF EXISTS` then recreate. Required to change `distributed_by` (fixed at creation), to apply changes an evolution rejects, and to rebuild correct results for a stateful model whose `start_mode` resumes from offsets (see the warning below).

> **Warning — stateful queries can silently reset on evolution**: an in-place evolution discards all Flink processing state (aggregation counts, join state, window state) and restarts the query per the model's `start_mode`. With the default `RESUME_OR_FROM_BEGINNING` (or any other `RESUME_*` form) the restarted query resumes from the previous job's offsets **without reprocessing history**: for a stateful model (`GROUP BY`, joins, windows) the results silently restart from the evolution point — in upsert mode the fresh, small values overwrite the old totals per key — and the table is permanently under-counted. The run still reports success. **To change a stateful model's definition and keep correct results**, either set `start_mode='FROM_BEGINNING'` (every evolution then reprocesses the full input history in place — the table transiently emits partial aggregates while it catches up, but the topic and its data are kept) or run with `--full-refresh` (see the data-loss warning below).

> **Warning — data loss**: dropping a materialized table (including via `--full-refresh`) permanently deletes the backing Kafka topic, all of its data, and the associated Schema Registry schema versions.

Note that the backing topic's (and its Schema Registry schemas') deletion is asynchronous and can lag the catalog drop. Recreating a model under the same name while the old topic still exists can fail in several ways:

- Recreating **any relation shortly after a drop** (not just materialized tables) can be rejected with "The table ... was found, but its Kafka topic does not exist; ... try again later" while the dropped entry's teardown settles. The adapter retries this automatically — it clears within tens of seconds. (After dropping a materialized table, the adapter additionally waits — up to 2 minutes — for the catalog entry itself to disappear before recreating, since the MT drop removes it asynchronously and an immediate recreate fails with "table already exists".) Teardown usually completes within tens of seconds, but under lag it can take several minutes; when a wait or retry budget runs out the run fails with a retriable error — re-run with `dbt retry` (more than once if needed) until the teardown clears.
- With a **different distribution**, creation fails with "a topic with the same name already exists with different partitions or configurations" — re-run once the old topic is fully deleted. (Recreating with the *same* distribution reuses the lingering topic.)
- With **different columns**, creation binds to the lingering topic's registered schema and fails with "Column types of query result and sink ... do not match" — again, re-run once the old topic is gone.
- A lingering topic can also **resurface in the catalog as an inferred regular table** under the model's name. The next plain run then fails with the materialization-switch guard error ("already exists as a regular table or view"); run with `--full-refresh` to drop the inferred table — which deletes the lingering topic — and recreate the materialized table.

If you drop relations outside dbt, note that a materialized table must be dropped with `DROP MATERIALIZED TABLE`: the server silently *accepts* a regular `DROP TABLE` against an MT but phantom-drops it — the catalog entry transiently disappears, same-name creates fail with "table already exists", and the MT later resurfaces intact. Never rely on `DROP TABLE` being rejected; check `IS_MATERIALIZED` in `INFORMATION_SCHEMA.TABLES` first, as the adapter does before every table drop.

**Evolution limits**: not every change can evolve in place — dropping columns is rejected at submission, observed either as a per-column error ("dropping a non-nullable, persisted column is not supported") or as a query/sink schema mismatch ("Column types of query result and sink ... do not match. Cause: Different number of columns."). The fix is `--full-refresh`. (Materialized tables don't use [schema drift detection](#schema-drift-detection) — Flink reconciles the definition instead, and a rejected evolution is the analogous failure mode.)

**Config:**

- `distributed_by` — a `{'columns': [...], 'buckets': N}` mapping, same shape and validation as the other materializations (see [Distributed By](#distributed-by)). Confluent's materialized-table grammar documents `DISTRIBUTED BY (...)` without `HASH`; the adapter emits `DISTRIBUTED BY HASH(...)`, which works in practice.
- `with` — table options, e.g. `{'key.format': 'avro-registry'}`.
- `start_mode` — where the query starts (or, on an in-place evolution, restarts) reading. It applies when the table is created **and is re-applied on every evolution** — e.g. `FROM_BEGINNING` reprocesses the full input history after each definition change (empirically verified; see the stateful-queries warning above). All eight documented forms are accepted (default: `RESUME_OR_FROM_BEGINNING`): `FROM_BEGINNING`, `FROM_NOW`, `RESUME_OR_FROM_BEGINNING`, `RESUME_OR_FROM_NOW`, `FROM_TIMESTAMP('<timestamp>')`, `RESUME_OR_FROM_TIMESTAMP('<timestamp>')`, `FROM_NOW(INTERVAL '<n>' <unit>)`, `RESUME_OR_FROM_NOW(INTERVAL '<n>' <unit>)`. The adapter validates the keyword and its arity but passes the parenthesized argument through verbatim (after rejecting anything that could break out of the DDL — stray quotes, parens, operators); the server validates the argument itself. Note that `FROM_NOW`/`RESUME_OR_FROM_NOW` require a full interval literal — `FROM_NOW(INTERVAL '7' DAY)` — not a plain quoted string, despite what some Confluent docs examples currently show.

`freshness_interval`, `refresh_mode`, and `partition_by` exist in open-source Flink but not in Confluent's dialect; they raise a compile error.

**Switching materializations**: an existing regular table or view cannot be converted to a materialized table, and a materialized table cannot be adopted by the drop-and-recreate materializations. The adapter detects both switches before submitting anything, and both resolve the same way:

- *To* `materialized_table`: a plain run fails with guidance; `--full-refresh` drops the existing relation (and its Flink statements) through the regular drop path, then creates the materialized table.
- *From* `materialized_table` (model changed to `table`/`streaming_table`/`streaming_source`): a plain run fails with guidance — this matters because a materialized table looks like a regular table to the catalog, and without the check an unchanged model would silently "succeed" while Flink kept maintaining the old defining query. `--full-refresh` drops the materialized table (via `DROP MATERIALIZED TABLE` — see the drop note above) and creates the regular relation. Note the drop permanently deletes the backing topic and its data, and `on_schema_drift='ignore'` skips this detection along with the rest of the drift check.

Re-running while Flink is still establishing a freshly created or evolved table can be transiently rejected (`being modified`); the window is brief and the adapter retries automatically.

## Schema Drift Detection

When a table already exists and `--full-refresh` is not specified, the adapter performs drift detection before skipping creation. The check compares **columns**, **WITH options**, and **`distributed_by`** in a single pass and raises one error listing every violation, so you don't have to fix them one at a time. It also detects when the existing relation is a **materialized table** (a reverse materialization switch) and fails with dedicated guidance instead of a drift list — see [Switching materializations](#materialized-table). (`materialized_table` models themselves do not use drift detection — Flink reconciles the re-asserted definition instead; see [Materialized Table](#materialized-table).)

To determine the expected schema, the adapter creates a short-lived temporary table (named `__dbt_tmp_schema_check_<model>`) and issues a single `UNION ALL` query against `INFORMATION_SCHEMA.COLUMNS`, `TABLES`, and `TABLE_OPTIONS` to fetch every piece of metadata at once. For `table` and `streaming_table`, the temp table is created from the model's SELECT query; for `streaming_source`, from the model's column definitions (without the connector). The temp table is dropped in the adapter's post-model hook, which dbt invokes even when the materialization fails (e.g. when drift is detected) — so a run that raises after creating the temp table doesn't leak it. As a backstop for runs that die hard (killed process, lost connectivity) before the hook runs, the temp table name is deterministic per model and the check drops any leftover before creating a new one, so the next drift check reclaims a leak.

### Configuration

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

### Column Drift
- **table, streaming_table**: Compares existing column names and data types with expected columns from the SELECT query. Raises an error if columns are added, removed, renamed, or if data types change. Column reordering is allowed (order doesn't matter for Kafka-backed tables).
- **streaming_source**: Compares existing column names and data types with the column definitions in the model SQL. Raises an error if columns are added, removed, renamed, or if data types change. Uses a temporary table to infer schema from SQL column definitions.

### Distribution Drift
Compares the user-specified `config(distributed_by={...})` against the existing distribution from `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS`. Raises an error if the column list, column order, or — when explicitly specified — the bucket count differ.

**Important limitation**: As with WITH options, the adapter only verifies what the user explicitly requested. If `distributed_by` is unset, drift detection is skipped entirely, because Confluent assigns a default distribution (typically derived from the primary key) to every Kafka-backed table, and INFORMATION_SCHEMA does not distinguish user-specified from auto-assigned distribution. Note that you cannot truly *remove* a distribution: every Kafka-backed table has one. To stop the adapter from comparing against a previously-set `distributed_by`, drop the config and use `--full-refresh` to recreate the table — Confluent will then assign its default distribution.

### WITH Options Drift
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

### Query Logic Changes

Schema drift detection only inspects **column names, data types, and WITH options** — it does not detect changes to the query logic itself. If you modify how a column is computed without changing its name or type, the adapter will see no drift and skip the model.

**Example of undetected change**:
```sql
-- Initial model
select order_id, round(price, 2) as price from {{ ref('source') }}

-- Changed to (different rounding)
select order_id, round(price, 4) as price from {{ ref('source') }}

-- Result: Column name and type are unchanged, so dbt will skip (no error)
```

This is an inherent limitation — `INFORMATION_SCHEMA` only stores schema metadata, not the query that produced the table. If you change query logic, use `--full-refresh` to recreate the table.

### When Drift is Detected
If drift is detected, the run will fail with a compilation error. Use `--full-refresh` to drop and recreate the table with the new schema or options.

## Deterministic Statement Names

Each materialization creates Flink statements with deterministic names derived from the dbt project and model names:

```
{prefix}{project_name}-{model_name}
```

The default prefix is `dbt-`. For `streaming_table`, which creates two statements (a DDL and an INSERT), the DDL gets a `-ddl` suffix: `dbt-{project}-{model}-ddl`.

`materialized_table` differs in two ways. Its defining `CREATE OR ALTER` statement completes immediately — it is not a long-running maintainer (Flink maintains the table server-side) — so the adapter reaps it like any other bounded statement; a failed submission is left in place for debugging (Confluent purges terminal statements after ~30 days). And each run submits under a unique per-run name (`dbt-{project}-{model}-{invocation_id}`), so a re-assert can never collide (409) with a statement lingering from a previous run.

### Flink Naming Constraints

Flink statement names must contain only lowercase alphanumeric characters and hyphens, start with an alphanumeric character, and be at most 100 characters long. The adapter sanitizes names automatically:

- Illegal characters (including underscores) are replaced with hyphens
- A 6-char MD5 hash suffix is appended when characters are replaced, to avoid collisions (e.g. `my_model` and `my.model` produce different names)
- Names exceeding 100 characters are truncated with a 6-char hash suffix

### Custom Statement Names

Override the generated name with the `statement_name` config:

```sql
{{ config(materialized='streaming_table', statement_name='my-custom-name') }}
```

### Statement Lifecycle

On `--full-refresh`, the adapter deletes existing statements before dropping and recreating the table. When no relation exists, orphaned statements are also cleaned up.

For `streaming_table`, the adapter additionally checks the long-running INSERT statement on every re-run. If the statement is missing (e.g. the process crashed between DDL and DML, or the statement was deleted externally) or in a terminal phase (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), `dbt run` resubmits **only the INSERT** under the same deterministic name — the table and its topic state are preserved. No `--full-refresh` required. A `RUNNING` statement, in-flight transitions (`PENDING`, `STOPPING`, `DELETING`), and `DEGRADED` are treated as healthy: the adapter does not interrupt them.

For `streaming_source`, automatic recovery is **not** supported: the CREATE statement also attaches the connector, and Flink does not allow re-attaching a connector to an existing table. If the connector statement is dead, run with `--full-refresh` (which drops and recreates the table). Tracked as a follow-up.

## Compute Pool

By default, every statement runs on the compute pool configured in your profile (`compute_pool_id`). You can override the pool per model — for example to isolate a heavy model or to manage resources — with the `compute_pool_id` config:

```sql
{{ config(materialized='streaming_table', compute_pool_id='lfcp-abc123') }}
```

The override applies to all statements a model submits (DDL, the long-running INSERT, and metadata/drift-check queries). The pool must exist in the same environment and region as the profile, and the API key used must have access to it; Confluent Cloud validates this at submission time. When `compute_pool_id` is omitted, the profile default is used. If the profile sets no default pool either, Confluent Cloud Flink runs the statement on the environment+region default pool.

Statement recovery and cleanup (see [Statement Lifecycle](#statement-lifecycle)) operate by statement name and are pool-agnostic: a model's statement is found, inspected, and — when dead — resubmitted on the model's configured pool regardless of the profile default.

Changing `compute_pool_id` on an existing, healthy model takes effect **only** on the next `--full-refresh` or statement restart — a running statement is not migrated to a new pool, since the pool is a property of the statement (not the table) and isn't part of drift detection.

### Per-environment pools in CI/CD

The same model is often deployed to different compute pools across environments (dev / staging / prod) or regions. Rather than hard-coding a pool, inject it at deploy time with an environment variable:

```sql
{{ config(materialized='streaming_table', compute_pool_id=env_var('FLINK_COMPUTE_POOL')) }}
```

Your CI/CD pipeline sets `FLINK_COMPUTE_POOL` (and typically `statement_name`) per target, keeping a single Git source of truth.

## Adopting Existing Tables and Statements

If you already have a Flink pipeline running — deployed by hand, by a previous tool, or by another team — you can bring it under dbt management without recreating it. A pipeline is two things: a **table** (the relation) and a **statement** (the long-running query that populates it). Map your model to each:

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

- If the statement is **healthy** — `RUNNING`, an in-flight transition (`PENDING`, `STOPPING`, `DELETING`), or `DEGRADED` (any non-terminal phase) — it is adopted as-is: the run skips creation and leaves the statement untouched.
- If the statement is **missing or terminal** (`COMPLETED`, `STOPPED`, `FAILED`, `DELETED`), it is re-submitted under the same name (see [Statement Lifecycle](#statement-lifecycle)).
- The existing **table is never dropped** (only `--full-refresh` drops and recreates).

Adoption is purely name-based — the adapter does not track which tool created a resource, only its name — so there is no separate "import" step.

### Preconditions

- **Schema should match.** Under the default `on_schema_drift='fail'`, the adapter runs [schema drift detection](#schema-drift-detection) comparing the existing table to the model's SELECT before adopting; any mismatch fails the run. With `config(on_schema_drift='ignore')`, enforcement depends on the adoption path:
    - **Healthy statement → skip:** the running statement is left untouched and nothing is re-submitted, so **no drift is enforced at all, including columns.** A mismatch is silently tolerated — you must align the model to the existing schema yourself.
    - **Dead/terminal statement → restart:** the INSERT is re-submitted, so a columns-only check still runs (a column mismatch would also be rejected by Flink); benign options/distribution drift is relaxed.
- **Names are sanitized.** The `statement_name` you configure is normalized to Flink's constraints (see [Flink Naming Constraints](#flink-naming-constraints)) before lookup, so it must match the existing statement's actual name. Statements already named within those constraints (lowercase alphanumeric + hyphens) match verbatim.
- **`streaming_source` cannot recover a dead connector statement** (see above); adoption of a source means pointing `alias` at the existing table, after which re-runs skip while the connector statement is healthy.

## Not Supported

| Materialization | Reason |
|---|---|
| `materialized_view` | dbt's built-in `materialized_view` materialization is not implemented. For a Flink materialized table use the `materialized_table` materialization (declarative, `CREATE OR ALTER MATERIALIZED TABLE`); for a CTAS table that Flink keeps continuously updated use `table`. |
| `incremental` | dbt's batch-incremental semantics does not map to Flink's continuous processing model. Use `streaming_table` instead. |
| `snapshot` | Flink SQL lacks the batch operations (MERGE, UPDATE) required by dbt snapshots. |
