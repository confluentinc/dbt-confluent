# Pull Request Lifecycle Rules

## Branches must adhere to the following lifecycle rules:
  1. A branch should be focused on a single logical change or feature. More complex changes should be split into multiple branches and pull requests.
    - At most a handful of small, pithy 'also seen and fixed or improved here' changes can be included, and must always be explicitly pointed out in the pull request description.
  2. A branch should have a human-readable reasonable number of lines of changes relative to its parent branch:
    - At most 500-700 lines of actual primary codebase changes (excluding docs, docstrings, tests).
    - Test suite and / or documentation changes can at most double the number of lines of primary codebase changes for a total maximum of around 2,100 lines including the non-primary codebase changes.
    - package lock files (e.g., package-lock.json, uv.lock, other purely mechanical changes) are not counted towards the line limits.
    - Exceptions to these limits must be explicitly justified in the pull request description, but should be truly exceptional.
  3. Introducing new tooling, development practices, or changes to the development or CI/CD workflow should be done through consensus and discussion, such as through Slack (citing the thread in the PR description) or an issue written by a separate team member.

## Pull requests must adhere to the following rules:
  1. A pull request should be raised only after the branch is up-to-date with its parent branch and passing unit tests and linting.
  2. Do not raise, or advise raising, a pull request for a branch that violates the branch lifecycle rules unless a strong justification is given.
     - Instead, determine ways to bring the branch into compliance with the branch lifecycle rules, such as through splitting separate concerns into independent or stacked branches, compacting wordy separate tests into parameterized ones, better use of fixtures, etc.
     - If it is not possible to bring the branch into compliance, ask the human to provide a strong justification in the pull request description.
  3. When merging a multi-commit pull request, the human should hand-write the ultimate merge commit message, summarizing the changes in a clear and concise manner.

## PR Descriptions
  1. A PR description should clearly outline the single logical change or feature being introduced.
  2. Any small, pithy 'also seen and fixed or improved here' changes must be explicitly pointed out in the PR description.
  3. The PR description should include any justifications for exceptions to the branch or PR lifecycle rules.
  4. Breaking changes to existing released interfaces must be highlighted in the PR description (as well as the corresponding changelog entry).
  5. The PR description should reference any related issues ("Closes #131"), previous PR numbers, or relevant slack threads.
  6. PRs for branches other than main for new tip development should clearly indicate the target branch in the PR title: "[v0.3.x] Fix the thing", "[oauth-integration] Make epic progress"
  7. Security related issues should be called out very clearly in the PR description and should be PR'd in isolated branches.
     - This includes those for "maintenance" dependency updates.
     - Maintenance dependency updates should never be combined with feature development changes. New features which require new dependencies should only introduce those new dependencies and not also update unrelated dependencies.
