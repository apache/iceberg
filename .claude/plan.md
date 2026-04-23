# Plan: Run Iceberg CI on Fork Compute (Spark-style)

## Background

Currently, Iceberg's CI workflows (java-ci, spark-ci, flink-ci, etc.) all use `on: pull_request`, which means they run on the **upstream apache/iceberg** repo's GitHub Actions quota when a fork PR is opened. This consumes shared ASF resources.

Apache Spark solved this with [PR #32092](https://github.com/apache/spark/pull/32092) using a three-workflow architecture.

## How Spark's Pattern Works

1. **Fork workflow** (`build_main.yml`): Triggers `on: push` to any branch in the fork. Checks out upstream master, merges the fork branch, runs the full test suite — all using the fork's free GitHub Actions quota.
2. **Notify workflow** (`notify_test_workflow.yml`): Triggers `on: pull_request_target` on the upstream repo. Uses the GitHub API to find the corresponding workflow run on the fork, then creates a GitHub Check Run on the PR (status: queued).
3. **Status updater** (`update_build_status.yml`): Runs on a cron schedule (every 15 min) on the upstream repo. Polls open PRs, reads fork metadata from check runs, queries the fork's run status, and updates the check run (green/red).

## Plan for Iceberg

### Phase 1: Fork-side changes (on your fork only, no upstream PR needed)

Modify each CI workflow on your fork to **also trigger on `push`** to your working branches, and add an upstream sync step so tests run against your changes merged with latest `main`.

**For each of the 10 CI workflow files**, apply this pattern:

```yaml
# BEFORE (upstream default):
on:
  push:
    branches:
    - 'main'
    - '0.*'
    - '1.*'
    - '2.*'
    tags:
    - 'apache-iceberg-**'
  pull_request:
    paths-ignore:
    - ...

# AFTER (fork override):
on:
  push:
    branches:
    - '**'              # Trigger on push to ANY branch
    - '!main'           # Except main (no point re-running upstream's CI)
    - '!0.*'
    - '!1.*'
    - '!2.*'
    paths-ignore:
    - ...               # Keep the same path-ignore filters
```

And add a sync step to each workflow's checkout:

```yaml
- uses: actions/checkout@v4
  with:
    fetch-depth: 0
    repository: apache/iceberg
    ref: main
- name: Sync fork branch with upstream main
  if: github.repository != 'apache/iceberg'
  run: |
    git fetch https://github.com/$GITHUB_REPOSITORY.git ${GITHUB_REF##*/}
    git merge --progress --ff-only FETCH_HEAD
```

**Files to modify (on fork):**
1. `.github/workflows/java-ci.yml`
2. `.github/workflows/spark-ci.yml`
3. `.github/workflows/flink-ci.yml`
4. `.github/workflows/hive-ci.yml`
5. `.github/workflows/kafka-connect-ci.yml`
6. `.github/workflows/delta-conversion-ci.yml`
7. `.github/workflows/api-binary-compatibility.yml`
8. `.github/workflows/open-api.yml`
9. `.github/workflows/docs-ci.yml`
10. `.github/workflows/license-check.yml`

### Phase 2: Upstream changes (PR to apache/iceberg — optional, for status reporting)

**File 1: `.github/workflows/notify_test_workflow.yml`** (new)

Triggers on `pull_request_target` (opened/reopened/synchronize). Uses `actions/github-script` to:
- Query the fork's Actions API for the matching workflow run
- Create a GitHub Check Run on the PR linked to the fork's run
- If fork has no Actions enabled, post instructions

**File 2: `.github/workflows/update_build_status.yml`** (new)

Runs on `schedule: "*/15 * * * *"` with `if: github.repository_owner == 'apache'`. For each open PR:
- Read the Check Run metadata (fork owner/repo/run_id)
- Query fork's run status via API
- Update the Check Run (queued → in_progress → completed with success/failure)

## Recommended Approach

**Start with Phase 1 only.** This is entirely within your control — no upstream PR needed. You modify the workflow files on your fork, and CI runs on your fork's compute whenever you push. The only trade-off: you check your fork's Actions tab for results rather than seeing them on the upstream PR.

Phase 2 (status reporting) is a larger infrastructure change that would need community buy-in from the Iceberg project.

## Trade-offs

| Aspect | Benefit | Cost |
|--------|---------|------|
| Fork compute | Free, unlimited for public repos | Must have Actions enabled on fork |
| Status reporting (Phase 2) | Results appear on upstream PR | 15-min polling latency |
| Workflow maintenance | No duplication if modifying in-place | Fork workflows diverge from upstream |
| Upstream impact | Saves ASF shared resources | Phase 2 needs community approval |
