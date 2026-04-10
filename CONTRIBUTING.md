# Contributing to dbt-confluent

## Development setup

```bash
git clone https://github.com/confluentinc/dbt-confluent
cd dbt-confluent
uv sync --dev
```

See the [README](README.md) for code quality checks and test instructions.

## Changelog

We use [towncrier](https://towncrier.readthedocs.io/) to manage the changelog. Every pull request must include a changelog fragment — a small text file describing the change. CI will block merges without one.

### Creating a fragment

Add a file to the `changes/` directory named `<issue-number>.<type>` (e.g., `42.feature`). If there is no associated issue, use `+<short-description>.<type>` (e.g., `+fix-retry-logic.bugfix`).

You can create fragments with:

```bash
uv run towncrier create 42.feature
```

Or simply create the file manually. The file content should be a short description of the change (one or two sentences):

```
Added support for temporal table joins in streaming_table materialization.
```

### Fragment types

| Type | Section in changelog | When to use |
|---|---|---|
| `breaking` | Breaking Changes | Breaking changes that aren't covered by other types |
| `feature` | Features | New functionality |
| `bugfix` | Bugfixes | Bug fixes |
| `doc` | Improved Documentation | Documentation-only changes |
| `removal` | Deprecations and Removals | Removed or deprecated features |
| `misc` | Misc | Minor changes worth noting (refactors, dependency bumps) |
| `trivial` | *(not shown)* | Changes that don't need a changelog entry (CI, typo fixes) |

Use `trivial` when a change doesn't need to appear in the changelog at all — the fragment must still exist to pass CI, but its content won't be rendered.

### Previewing the changelog

To see what the next changelog entry will look like:

```bash
uv run towncrier build --draft
```

### How it works at release time

When cutting a release, a maintainer runs `towncrier build` (without `--draft`). This compiles all fragments into `CHANGELOG.md` and deletes the fragment files. The result is committed as part of the release.
