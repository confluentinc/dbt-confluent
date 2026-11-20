# Releasing dbt-confluent

This document describes how to cut a release of `dbt-confluent`. It is written for maintainers.

## Overview

| Step | What | Where |
|---|---|---|
| 1 | Cut the release branch `vMAJOR.MINOR.x` from `main` | GitHub |
| 2 | Stabilize: final testing, bug bashing, and fixes | PRs into the release branch |
| 3 | Stamp the version and release notes | PR into the release branch |
| 4 | Promote the merge build to a release | Semaphore |
| 5 | Pull the changelog forward and bump `main` to the next `-dev` version | PR into `main` |

Throughout, `X.Y` stands for the minor version being released (e.g. `0.5`), and `X.Y.Z` for the full version (e.g. `0.5.0`).

## 1. Create the release branch

Every new major or minor version gets its own release branch named `vMAJOR.MINOR.x`, such as `v0.5.x`. Check the existing ones to confirm the convention:

```bash
git fetch origin
git branch -r --list 'origin/v*.x'
```

Patch releases (`0.5.1`, `0.5.2`, ...) are cut from the same branch as the minor release, so a new branch is only needed for a new major or minor version.

Create the branch from the tip of an up-to-date `main` and push it with **absolutely no other changes**. Don't tweak the version, don't add commits, just name it correctly and push:

```bash
git switch main
git pull --ff-only
git switch -c vX.Y.x
git push-external -u origin vX.Y.x
```

> **Branch protection is automatic.** Branches whose names match the release branch pattern (`vMAJOR.MINOR.x`) are protected in CI/GitHub by name regexp, so there is no manual protection setup. This also means that **all further changes to the branch must go through pull requests**, and that you must get the name exactly right. The changelog check in `poe_tasks.toml` uses the same pattern (`^v[0-9]+\.[0-9]+\.x$`) to skip the fragment requirement on release branches.

> **Pushing:** the org ruleset on this repo blocks creating branches with a plain `git push`. Use `git push-external` instead.

Keeping the first commit identical to `main` means the release branch's history is exactly `main`'s history at the moment of the cut, which makes merging back and cherry-picking later straightforward.

## 2. Stabilize the release branch

"Final" testing and bug bashing happen against the release branch. Any fixes follow the normal branch and PR rules (see [pr-lifecycle.md](.agents/rules/pr-lifecycle.md)), with the release branch as the PR target. Name the target in the PR title: `[vX.Y.x] Fix the thing`.

Ongoing development continues on `main` in the meantime. Only things that should ship in this release belong on the release branch.

## 3. Stamp the version and release notes

When the release is a "go", open a PR into the release branch that does only two things.

1. **Set the version.** Edit `dbt/adapters/confluent/__version__.py` so it holds the release version with no `-dev` suffix:

   ```python
   version = "X.Y.Z"
   ```

2. **Build the release notes.** Compile the changelog fragments in `changes/` into `CHANGELOG.md`:

   ```bash
   UV_NO_CONFIG=1 uv run towncrier build --version X.Y.Z
   ```

   This inserts a `## dbt-confluent X.Y.Z (date)` section into `CHANGELOG.md` and deletes the consumed fragments. Use `towncrier build --draft` first to preview.

   Then **edit the generated section by hand**. The raw output is a starting point, not the final product. Tighten wording, merge related entries, fix categories, make sure breaking changes are called out prominently, and check that the issue links render correctly. Fragments are written per PR, but release notes are read by users.

Open the PR against the release branch (title it like `[vX.Y.x] Stamp X.Y.Z for release`), then make sure it is releasable:

- All CI checks pass (lint, build, unit tests).
- **The functional tests pass.** CI does not run these. Run them yourself against Confluent Cloud before merging:

  ```bash
  uv run poe functional
  ```

  They skip themselves if the Confluent Cloud environment variables aren't set, so a run where everything was skipped does **not** count as passing. Confirm that tests actually ran.

Merge the PR once everything is green.

## 4. Promote the release in Semaphore

Merging the stamp PR triggers a Semaphore build on the release branch.

1. In Semaphore, find the pipeline for the **merge commit on the release branch**. Don't use the PR's build, because it's a different commit.
2. Check that it passed.
3. Click the **Publish dbt-confluent to PyPi** promotion button.

The promotion (`.semaphore/publish_to_pypi.yml`) builds the package and publishes it to PyPI. Check that the new version shows up on [PyPI](https://pypi.org/project/dbt-confluent/).

> Published versions are immutable on PyPI. If something is wrong, fix it on the release branch and release a patch version rather than trying to replace the artifacts.

## 5. Pull the changes forward into `main`

The stamp PR and any last-minute fixes exist only on the release branch, so bring them to `main`:

1. Branch from an up-to-date `main` and merge or cherry-pick the release branch's new commits. At a minimum this must include the stamp commit, whose **changelog update and deleted fragments** `main` needs. Any fixes that were made on the release branch only must also come across.
2. Set `dbt/adapters/confluent/__version__.py` on `main` to the **next version's `-dev` version**, e.g. `0.6.0-dev` after releasing `0.5.0`. Do **not** carry over the stamped `X.Y.Z` version.
3. Open a PR into `main` and merge it.

Do this promptly so that `main` doesn't keep accumulating changes whose changelog entries were already published, and so that fragments for the released changes don't appear again in the next release's notes.

## Patch releases

Hotfixes after a release are developed against the release branch **first**:

1. Fix and verify on the release branch through a normal PR (`[vX.Y.x] ...`), including a changelog fragment.
2. Follow steps 3 and 4 above to stamp (`X.Y.Z+1`) and promote the patch release.
3. Follow step 5 to merge the fix and the patch release's changelog update into `main`. Unlike a minor release, leave `main`'s `-dev` version alone.

## Checklist

Copy this into a tracking issue or PR description if it's useful.

- [ ] `vX.Y.x` created from `main`, with no extra commits, and pushed
- [ ] Final testing and bug bashing done on the release branch; fixes merged via `[vX.Y.x]` PRs
- [ ] `__version__.py` set to `X.Y.Z`
- [ ] `towncrier build` run and the release notes edited by hand
- [ ] CI is green on the stamp PR
- [ ] Functional tests run and passed (not just skipped)
- [ ] Stamp PR merged into the release branch
- [ ] Semaphore merge build promoted to PyPI, and the new version verified on PyPI
- [ ] Changelog and fixes brought forward to `main`
- [ ] `main`'s `__version__.py` bumped to the next `-dev` version
