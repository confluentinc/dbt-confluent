---
name: confluent-python-standards
description: "Use when scaffolding a new Confluent Python project (adapter, SDK, CLI, pipeline), auditing/migrating an existing one to modern standards, or reviewing Python code in Confluent projects for style guidance (type hints, exception handling, pathlib, anti-patterns). Triggers on: new Python project, pyproject.toml, ruff, mypy, pre-commit, CI setup, test structure, uv, hatch, changelog, towncrier, supply chain, coverage, make this pythonic, Python type annotations, is this good Python. Use for dbt adapters, Confluent Python SDKs, CLI tools, and data pipelines. Do NOT trigger for: reviewing Confluent agent skills (use confluent-skill-reviewer); building Kafka producers/consumers (use developing-kafka-python-client); Flink SQL or connector development (use flink-udf or confluent-cloud-cdc-tableflow)."
metadata:
  author: confluent
  version: "1.2.0"
  last_updated: "2026-07-09"
  user-invocable: true
  argument-hint: <scaffold|audit|migrate|review> [project-path]
compatibility: Python 3.10+, uv, ruff, mypy, pytest, pre-commit
---

# Confluent Python Standards

Enforce and apply Confluent's converged Python data-tooling standards to new or existing projects. Based on the practices shared by dbt, Dagster, dlt, and the Scientific Python ecosystem.

## Use This Skill vs. Others

| User need | Use this skill | Don't use |
|-----------|---------------|-----------|
| Scaffold / audit / migrate a project | ✅ Yes | — |
| "Is this good Python?" / code review | ✅ Yes | — |
| "Type hints for my function" | ✅ Yes | — |
| "Exception handling patterns" | ✅ Yes | — |
| Set up a Kafka producer/consumer | ❌ No | `developing-kafka-python-client` |
| Build a Flink SQL pipeline | ❌ No | `flink-udf` or `confluent-cloud-cdc-tableflow` |
| Schema Registry schema design | ❌ No | `kafka-schema-registry` |

## Mode Detection

Determine what the user needs before acting:

| User says | Mode |
|-----------|------|
| "scaffold", "new project", "set up a project", "start from scratch" | **Scaffold** — generate project skeleton |
| "audit", "check my project", "review my setup", "what's wrong with" | **Audit** — analyze existing project, produce gap report |
| "migrate", "update to", "modernize", "switch from X to Y" | **Migrate** — apply §15 adoption sequence to existing project |
| "review", "is this pythonic", "type hints", "good python", code snippet provided | **Review** — apply code-style guidance |

If ambiguous, ask: _"Should I scaffold a new project from scratch, audit your existing setup, help migrate to the modern stack, or review code for style?"_

## Version Detection (for Review and Scaffold modes)

Identify the project's minimum Python version before giving type annotation advice or generating code:

1. Check `pyproject.toml` → `requires-python` (e.g., `>=3.12`)
2. Check `.python-version` file
3. Check `setup.py` / `setup.cfg` → `python_requires`
4. Default to **3.10** if not found (the Confluent project floor)

Then load the matching version file:
- Python 3.10 → read [`versions/python-3.10.md`](versions/python-3.10.md)
- Python 3.11 → read [`versions/python-3.11.md`](versions/python-3.11.md)
- Python 3.12 → read [`versions/python-3.12.md`](versions/python-3.12.md)
- Python 3.13 → read [`versions/python-3.13.md`](versions/python-3.13.md)

---

## Scaffold Mode

Generate a complete project skeleton. Ask for:
1. **Package name** (e.g., `dbt-confluent-sink`)
2. **Package type**: adapter / SDK / CLI / pipeline
3. **Python floor version** (default: 3.10, apply SPEC 0 — 36-month support window)
4. **Changelog mechanism**: `towncrier` (fragment-based, like this repo) or `conventional-commits`

Then produce these files — show the full plan and wait for confirmation before writing:

**Plan template:**
```
I will create:
1. pyproject.toml          — packaging, deps, ruff, mypy, pytest, towncrier config
2. src/<pkg>/__init__.py   — src/ layout, py.typed marker
3. tests/unit/.gitkeep
4. tests/functional/.gitkeep
5. .pre-commit-config.yaml — ruff, mypy, gitleaks, TOML/YAML checks
6. .github/workflows/ci.yml — lint + typecheck + unit + functional + coverage
7. CHANGELOG.md            — empty, managed by towncrier/commitizen
8. changes/                — towncrier fragment directory (if towncrier chosen)
```

For configuration details and exact file templates, read [`references/standards.md`](references/standards.md#scaffold-templates).

---

## Audit Mode

Analyze the project at the given path (default: current workspace). Check against the standard stack:

| Area | Standard | Check |
|------|----------|-------|
| Packaging | `pyproject.toml` + `src/` layout, no `setup.py`/`setup.cfg`/`requirements.txt` | Files present/absent |
| Dependency mgmt | `uv.lock` committed, no floating pins | `uv.lock` exists |
| Lint/format | `ruff` with `E,F,W,I,UP,B,SIM,PL,PTH,PT,RUF,D,S` | `[tool.ruff.lint].select` |
| Type checking | `mypy` or `pyright` in CI, strict mode | `[tool.mypy]` / `pyrightconfig.json` |
| Testing | `tests/unit/` + `tests/functional/` split, coverage floor 80–90% | Directory structure + `[tool.pytest.ini_options]` |
| Pre-commit | `ruff-check`, `ruff-format`, `gitleaks`, `check-toml`, `check-yaml`, mypy | `.pre-commit-config.yaml` |
| CI | Python version matrix, cached uv, named required checks | `.github/workflows/` |
| Supply chain | `uv.lock` hashed, `pip-audit`/OSV-Scanner in CI | workflow files |
| Changelog | `towncrier` or Conventional Commits — one mechanism only | `pyproject.toml` + fragment dir |
| Observability | Structured JSON logs + OpenTelemetry for pipelines | imports in `src/` |

Produce a **gap report** with three sections:
- ✅ **Compliant** — already following the standard
- ⚠️ **Partial** — present but misconfigured (show what's wrong)
- ❌ **Missing** — not present at all

For detailed config values and thresholds, read [`references/standards.md`](references/standards.md#audit-thresholds).

---

## Migrate Mode

Apply the §15 adoption sequence — one PR's worth of change at a time, in order:

1. Add `pyproject.toml` + `uv.lock`; delete `requirements.txt`/`setup.py` in the **same PR**
2. Introduce `ruff` (lint + format); auto-fix, hand-fix remainder in a **dedicated PR**
3. Wire `pre-commit` so ruff runs locally before CI
4. Add type checker in permissive mode first → tighten to strict module by module
5. Split tests into `unit/` and `functional/`; add coverage floor once split is stable
6. Add `pip-audit`/OSV-Scanner; pin GitHub Actions to commit SHAs
7. Standardize changelog mechanism and SemVer discipline
8. Layer in OpenTelemetry and structured logging (pipelines only)

Show the full list of files that will be created or modified for the **current step** and wait for confirmation before proceeding. Never apply two steps in one PR.

---

## Confluent-Specific Conventions

These apply on top of the general standards for **all** Confluent Python repos:

- `metadata.author: confluent` in any skill or tool manifest
- `towncrier` is the preferred changelog mechanism (matches this repo and dbt 1.x heritage)
- `dbt-core~=1.11`, `dbt-adapters~=1.16` for dbt adapter projects — pin minor, not patch
- `confluent-kafka[avro,json,protobuf]` for any project producing/consuming Kafka data
- `confluent-sql` for adapter projects (pin minor, allow patch)
- Apache-2.0 license in all `pyproject.toml` files
- `requires-python = ">=3.10.0, < 3.14"` matches current dbt-confluent support matrix
- Entry points via `[project.entry-points."dbt.adapters"]` for dbt adapters

---

## Review Mode

When the user shares Python code or asks style questions:

1. Detect the Python version (see Version Detection above) — load the matching `versions/python-3.1x.md`
2. Read [`references/code-style.md`](references/code-style.md) for patterns and anti-patterns
3. Apply the relevant sections; read [`references/checklists.md`](references/checklists.md) for pre-commit decisions

Common triggers: "is this good Python", "type hints", "exception handling", "make this pythonic", "LBYL vs EAFP", "pathlib vs os.path", code snippet in the message.

---

## Reference Files

| File | When to read |
|------|-------------|
| [`references/standards.md`](references/standards.md) | Generating config files; user asks "why" about a specific standard |
| [`references/code-style.md`](references/code-style.md) | Reviewing or writing Python code — patterns, anti-patterns, exceptions |
| [`references/checklists.md`](references/checklists.md) | Final review before commit; user unsure if conventions are followed |
| [`versions/python-3.10.md`](versions/python-3.10.md) | Project targets Python 3.10 |
| [`versions/python-3.11.md`](versions/python-3.11.md) | Project targets Python 3.11 |
| [`versions/python-3.12.md`](versions/python-3.12.md) | Project targets Python 3.12 |
| [`versions/python-3.13.md`](versions/python-3.13.md) | Project targets Python 3.13 |
