---
title: "Multi-Engine Support"
bookHidden: true
url: multi-engine-support
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Multi-Engine Support

Apache Iceberg is an open standard for huge analytic tables that can be used by any processing engine.
The community continuously improves Iceberg core library components to enable integrations with different compute engines that power analytics, business intelligence, machine learning, etc.
Connectors for Spark, Flink and Hive are maintained in the main Iceberg repository.

## Multi-Version Support

Processing engine connectors maintained in the iceberg repository are built for multiple versions.

For Spark and Flink, each new version that introduces backwards incompatible upgrade has its dedicated integration codebase and release artifacts.
For example, the code for Iceberg Spark 3.1 integration is under `/spark/v3.1` and the code for Iceberg Spark 3.2 integration is under `/spark/v3.2`.
Different artifacts (`iceberg-spark-3.1_2.12` and `iceberg-spark-3.2_2.12`) are released for users to consume.
By doing this, changes across versions are isolated. 
New features in Iceberg could be developed against the latest features of an engine without breaking support of old APIs in past engine versions.

For Hive, Hive 2 uses the `iceberg-mr` package for Iceberg integration, and Hive 3 requires an additional dependency of the `iceberg-hive3` package.

### Runtime Jar

Iceberg provides a runtime connector Jar for each supported version of Spark, Flink and Hive.
When using Iceberg with these engines, the runtime jar is the only addition to the classpath needed in addition to vendor dependencies.
For example, to use Iceberg with Spark 3.2 and AWS integrations, `iceberg-spark-runtime-3.2_2.12` and AWS SDK dependencies are needed for the Spark installation.

Spark and Flink provide different runtime jars for each supported engine version.
Hive 2 and Hive 3 currently share the same runtime jar.
The runtime jar names and latest version download links are listed in [the tables below](./multi-engine-support/#current-engine-version-lifecycle-status).

### Engine Version Lifecycle

Each engine version undergoes the following lifecycle stages:

1. **Beta**: a new engine version is supported, but still in the experimental stage. Maybe the engine version itself is still in preview (e.g. Spark `3.0.0-preview`), or the engine does not yet have full feature compatibility compared to old versions yet. This stage allows Iceberg to release an engine version support without the need to wait for feature parity, shortening the release time.
2. **Maintained**: an engine version is actively maintained by the community. Users can expect parity for most features across all the maintained versions. If a feature has to leverage some new engine functionalities that older versions don't have, then feature parity across maintained versions is not guaranteed.
3. **Deprecated**: an engine version is no longer actively maintained. People who are still interested in the version can backport any necessary feature or bug fix from newer versions, but the community will not spend effort in achieving feature parity. Iceberg recommends users to move towards a newer version. Contributions to a deprecated version is expected to diminish over time, so that eventually no change is added to a deprecated version.
4. **End-of-life**: a vote can be initiated in the community to fully remove a deprecated version out of the Iceberg repository to mark as its end of life.

## Current Engine Version Lifecycle Status

### Apache Spark

Note that Spark 2.4 and 3.0 artifact names do not comply to the naming convention of later versions for backwards compatibility.

| Version    | Lifecycle Stage    | Runtime Artifact |
| ---------- | ------------------ | ---------------- |
| 2.4        | Deprecated         | [iceberg-spark-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/{{% icebergVersion %}}/iceberg-spark-runtime-{{% icebergVersion %}}.jar) |
| 3.0        | Maintained         | [iceberg-spark3-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/{{% icebergVersion %}}/iceberg-spark3-runtime-{{% icebergVersion %}}.jar) |
| 3.1        | Maintained         | [iceberg-spark-runtime-3.1_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/{{% icebergVersion %}}/iceberg-spark-runtime-3.1_2.12-{{% icebergVersion %}}.jar) |
| 3.2        | Maintained         | [iceberg-spark-runtime-3.2_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/{{% icebergVersion %}}/iceberg-spark-runtime-3.2_2.12-{{% icebergVersion %}}.jar) |

### Apache Flink

Based on the guideline of the Flink community, only the latest 2 minor versions are actively maintained.
Users should continuously upgrade their Flink version to stay up-to-date.

| Version    | Lifecycle Stage   | Runtime Artifact |
| ---------- | ----------------- | ---------------- |
| 1.12       | Deprecated        | [iceberg-flink-runtime-1.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.12/{{% icebergVersion %}}/iceberg-flink-runtime-1.12-{{% icebergVersion %}}.jar) |
| 1.13       | Maintained        | [iceberg-flink-runtime-1.13](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.13/{{% icebergVersion %}}/iceberg-flink-runtime-1.13-{{% icebergVersion %}}.jar) |
| 1.14       | Maintained        | [iceberg-flink-runtime-1.14](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.14/{{% icebergVersion %}}/iceberg-flink-runtime-1.14-{{% icebergVersion %}}.jar) |

### Apache Hive

| Version        | Recommended minor version | Lifecycle Stage   | Runtime Artifact |
| -------------- | ------------------------- | ----------------- | ---------------- |
| 2              | 2.3.8                     | Maintained        | [iceberg-hive-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{% icebergVersion %}}/iceberg-hive-runtime-{{% icebergVersion %}}.jar) |
| 3              | 3.1.2                     | Maintained        | [iceberg-hive-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{% icebergVersion %}}/iceberg-hive-runtime-{{% icebergVersion %}}.jar) |

## Developer Guide

### Maintaining existing engine versions

Iceberg recommends the following for developers who are maintaining existing engine versions:

1. New features should always be prioritized first in the latest version, which is either a maintained or beta version.
2. For features that could be backported, contributors are encouraged to either perform backports to all maintained versions, or at least create some issues to track the backport.
3. If the change is small enough, updating all versions in a single PR is acceptable. Otherwise, using separated PRs for each version is recommended.

### Supporting new engines

Iceberg recommends new engines to build support by importing the Iceberg libraries to the engine's project.
This allows the Iceberg support to evolve with the engine.
Projects such as [Trino](https://trino.io/docs/current/connector/iceberg.html) and [Presto](https://prestodb.io/docs/current/connector/iceberg.html) are good examples of such support strategy.

In this approach, an Iceberg version upgrade is needed for an engine to consume new Iceberg features.
To facilitate engine development against unreleased Iceberg features, a daily snapshot is published in the [Apache snapshot repository](https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/).

If bringing an engine directly to the Iceberg main repository is needed, please raise a discussion thread in the [Iceberg community](../community).