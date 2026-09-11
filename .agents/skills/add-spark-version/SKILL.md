---
name: add-spark-version
description: Add initial support for a new Apache Spark minor version in Apache Iceberg, including the versioned source tree, Gradle wiring, CI and publication decisions, compatibility fixes, and validation. Use for a new Spark line; do not use for patch-version upgrades within an existing line.
---

<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Add a Spark Version

Introduce a new Spark line without losing the history of the versioned Spark sources or accidentally changing which artifacts Iceberg builds and publishes by default.

Keep the workflow agent-neutral: base decisions on repository state and use standard Git, shell, and Gradle commands rather than product-specific features.

## Establish the target

Before editing, identify:

- the target Spark line and full dependency version;
- the supported Scala and JDK versions;
- the closest existing `spark/v<source>` directory to copy;
- whether the target artifacts are in Maven Central or require a temporary repository; and
- whether this is preview-only source support or a version that should be built and published by default.

Inspect the current tree and search for every version registry rather than assuming the file list is unchanged:

```bash
git status --short --branch
find spark -maxdepth 1 -type d -name 'v*' -print
rg -n 'knownSparkVersions|defaultSparkVersions|sparkVersions|spark[0-9]+' \
  gradle.properties gradle settings.gradle build.gradle spark jmh.gradle .github dev site
```

Use a clean branch based on the latest `upstream/main`, preferably in an isolated worktree when another Spark branch is active. Stop if the target version directory or a matching upstream contribution already exists.

## Protect an existing target branch

Before reusing a target branch, inspect both its local ref and the corresponding ref on the chosen remote. If both exist at different commits, stop and determine which tip must be preserved. Fetch a remote-only branch before continuing.

If the target branch exists, create a uniquely named local backup from its exact tip before any reset, rebase, or force-push. After resolving the existing target ref, replace the placeholders and run:

```bash
target_branch="<target-branch>"
existing_target_ref="<resolved-local-or-remote-ref>"
original_target_sha=$(git rev-parse "${existing_target_ref}^{commit}")
backup_branch="${target_branch}-backup-$(date +%Y%m%d-%H%M%S)"

git branch "${backup_branch}" "${original_target_sha}"
test "$(git rev-parse "${backup_branch}^{commit}")" = "${original_target_sha}"
```

Let `git branch` fail rather than overwrite an existing backup name. Keep the backup until the rewritten target branch is published and its remote tip is verified. Push the backup only when explicitly requested. If the target branch does not exist, create it normally from the refreshed base.

## Preserve source history

Create the new versioned tree with two mechanical commits. Replace the placeholders with concrete version lines before running the commands.

```bash
git mv spark/v<source> spark/v<target>
git add -A -- spark
git commit -m "Spark: Move <source> as <target>"

cp -R spark/v<target> spark/v<source>
git add -A -- spark
git commit -m "Spark: Copy back <target> as <source>"
```

Keep these commits free of compatibility edits so Git can follow the move. Use `git add -A -- spark`; narrower old/new pathspecs may fail while one side of the rename is absent. Never delete an existing target tree to force this sequence.

## Wire the new line

Make the support commit after the mechanical commits. Check the current equivalents of all of these integration points:

- `gradle/libs.versions.toml`: add the full Spark dependency version.
- `spark/v<target>/build.gradle`: update the Spark line, version-catalog key, Scala/JDK constraints, module coordinates, exclusions, and optional integrations.
- `gradle.properties`: add the line to `knownSparkVersions`. Change `defaultSparkVersions` only when the new line should become the default.
- `settings.gradle`, `spark/build.gradle`, and `jmh.gradle`: register the core, extensions, runtime, and benchmark projects.
- `.gitignore`: cover generated benchmark or warehouse paths for the new tree.
- `.github/workflows/spark-ci.yml` and `.github/workflows/cve-scan.yml`: add supported combinations while respecting the current matrix-size and JDK/Scala exclusions.
- `dev/stage-binaries.sh` and snapshot publication workflows: add the line only when artifacts should be published. Preview source support and release publication are separate decisions.
- Documentation and source-release rules discovered by the version search: update them only when their stated support or packaging scope includes the new line.

Do not add a broad repository filter for all `org.apache.spark` artifacts when only a prerelease target needs an alternate repository. Scope any temporary repository to the exact target version so older Spark builds continue to resolve from their normal repositories.

Do not modify `versions.props`, `LICENSE`, or `NOTICE` without the explicit discussion required by the repository instructions. If the new dependency graph changes runtime notices, stop and surface that requirement.

## Port compatibility changes

Compile the copied modules first, then adapt only the target tree to the new Spark APIs. Treat Spark Catalyst and execution classes as version-specific implementation details and keep those changes under `spark/v<target>` unless shared code genuinely owns the behavior.

Search for stale source-version references after editing:

```bash
rg -n '<source>|spark<source-without-dot>|Spark <source>' spark/v<target>
git diff --check upstream/main...
```

Review every match instead of replacing blindly: compatibility fallbacks, dependency coordinates, comments, and test expectations may intentionally refer to an older version.

## Validate

First confirm Gradle recognizes the new projects, then assemble all three deliverables:

```bash
./gradlew -DsparkVersions=<target> -DscalaVersion=<scala> projects

./gradlew --no-daemon -DsparkVersions=<target> -DscalaVersion=<scala> \
  -DflinkVersions= -DkafkaVersions= \
  :iceberg-spark:iceberg-spark-<target>_<scala>:assemble \
  :iceberg-spark:iceberg-spark-extensions-<target>_<scala>:assemble \
  :iceberg-spark:iceberg-spark-runtime-<target>_<scala>:assemble
```

Run focused tests for each compatibility change, then the new line's `check` tasks with the same Spark, Scala, Flink, and Kafka properties used by CI. Run Spotless and `git diff --check`. If shared build wiring or repository selection changed, also compile one older supported Spark line as a regression check.

Before publishing, review the three-dot diff from `upstream/main`. The PR description must state the artifact source, supported Scala/JDK matrix, whether the new line is a default and release target, validation performed, and any known preview limitations.
