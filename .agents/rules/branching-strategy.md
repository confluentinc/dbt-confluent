# Branching Strategy

  1. The main branch is used for the tip of development. It must always pass unit and integration/functional tests.
  2. Larger development epics which might leave main in temporary broken state should be developed against a child integration branch and then have rule-following PRs make that branch grow in reasonable chunks. The integration branch is then PR cycled back into main, bypassing the size rules, since every line of change in the integration branch has already been reviewed and approved through the smaller, rule-following PRs.
    - The only way non-present-in-main code should be introduced into an integration branch is through a rule-following PR being approved.
    - Other techniques may include pseudo-feature flags as implemented by environment variable or other opt-in mechanisms, as pre-determined by developer and target reviewers.
  3. Release branches (`v0.5.x`) will be cut from main when preparing for a release.
    - The release branch is initially forked from main, then pushed to github with zero changes from main. Just name it correctly then push the new branch. It will then be protected.
    - "Final" testing, bugbashing, etc. should be performed on the release branch, with fixes following the normal branch/PR size rules.
    - After deciding that a release is a "go", then raise a PR into the release branch which stamps the new version and changelog date header, then merge it into the release branch. Then run the Semaphore release promotion from the resulting build. Then merge or cherry-pick the release branch bits back into main to get the new changelog entry and any last minute fixes needed to live on in main.
    - Any hotfixes required after the release should be developed against the release branch *first*, verified, then patch release made, then finally the changes merged back into main, including the patch release changelog updates.
