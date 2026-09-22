---
name: confluent-python-standards
description: "Use when reviewing Python code changes in a Confluent dbt adapter project — pull request diffs, inline snippets, or file-level review. Triggers on: \"is this good Python\", \"review this PR\", \"review these changes\", type hints, exception handling, import style, test patterns, anti-patterns. Do NOT trigger for: reviewing Confluent agent skills (use confluent-skill-reviewer); Kafka producers/consumers (use developing-kafka-python-client); Flink SQL or connectors (use flink-udf or confluent-cloud-cdc-tableflow); Schema Registry design (use kafka-schema-registry)."
user-invocable: true
argument-hint: review [file-or-diff-path]
metadata:
  author: confluent
  version: "0.2.0"
  last_updated: "2026-09-22"
  compatibility: Python 3.10+, ruff, mypy
---

# Confluent Python Standards — dbt Adapter Code Review

Review Python code changes in `dbt-confluent` and other Confluent dbt adapter projects against Confluent coding standards.

## Version Detection

Annotations in shipped package code must import on the **oldest supported interpreter**, so use the `requires-python` floor from `pyproject.toml` — not the interpreter you happen to be running.

1. `pyproject.toml` → `requires-python` lower bound (e.g. `>=3.10` → 3.10)
2. `.python-version` describes the *dev environment* only — use it for tooling or test-only code, and say so when it disagrees with the floor
3. Default to **3.10** if neither is present (the Confluent adapter floor)

Then read [`references/typing.md`](references/typing.md) and apply only the sections at or below the detected version.

---

## Core Principle — Defer to dbt-core

The adapter is a thin layer over dbt-core and dbt-adapters. **Before flagging a missing dependency, marker, or instrumentation, check whether dbt already owns that concern.** It is why these are NOT findings:

- **Missing `py.typed` marker** — the `dbt` namespace package cannot carry one, and dbt-core does not ship one
- **Missing OpenTelemetry or JSON-log instrumentation** — dbt's `AdapterLogger` and `fire_event` own observability
- **Missing `confluent-kafka` dependency** — adapters generate Flink SQL DDL through `confluent-sql`; the I/O layer is not theirs

---

## Review

Detect the Python version (above), read [`references/typing.md`](references/typing.md) for annotation syntax and [`references/code-style.md`](references/code-style.md) for patterns, anti-patterns, and exception handling, then apply the relevant sections to the code under review.

Produce findings grouped by severity:

- 🔴 **Blocking** — correctness bug, security issue, or a standard that ruff/mypy will enforce in CI (the PR will fail)
- 🟡 **Warning** — violates a convention; reviewer judgment whether to block
- 🔵 **Nit** — style or clarity, no functional impact

For each finding cite the rule or reference section (e.g. `ruff B904`, `references/code-style.md § Exception chaining`).

### dbt Adapter Conventions

Flag these in any changed file:

- **Package layout**: code must live under `dbt/adapters/<name>/` — NOT `src/`
- **No relative imports** — absolute imports only (no enabled ruff rule catches this; it is a review responsibility)
- **Version pinning**: `~=X.Y.0` pins the minor; bare `~=X.Y` lets minors float — flag the wrong form in `pyproject.toml` changes
- **No `py.typed` marker** — its *presence* is the finding, not absence
