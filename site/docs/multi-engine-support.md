---
title: "Multi-Engine Support"
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
For example, the code for Iceberg Spark 3.4 integration is under `/spark/v3.4` and the code for Iceberg Spark 3.5 integration is under `/spark/v3.5`.
Different artifacts (`iceberg-spark-3.4_2.12` and `iceberg-spark-3.5_2.12`) are released for users to consume.
By doing this, changes across versions are isolated. 
New features in Iceberg could be developed against the latest features of an engine without breaking support of old APIs in past engine versions.

For Hive, Hive 2 uses the `iceberg-mr` package for Iceberg integration, and Hive 3 requires an additional dependency of the `iceberg-hive3` package.

### Runtime Jar

Iceberg provides a runtime connector jar for each supported version of Spark, Flink and Hive.
When using Iceberg with these engines, the runtime jar is the only addition to the classpath needed in addition to vendor dependencies.
For example, to use Iceberg with Spark 3.5 and AWS integrations, `iceberg-spark-runtime-3.5_2.12` and AWS SDK dependencies are needed for the Spark installation.

Spark and Flink provide different runtime jars for each supported engine version.
Hive 2 and Hive 3 currently share the same runtime jar.
The runtime jar names and latest version download links are listed in [the tables below](#current-engine-version-lifecycle-status).

### Engine Version Lifecycle

Each engine version undergoes the following lifecycle stages:

1. **Beta**: a new engine version is supported, but still in the experimental stage. Maybe the engine version itself is still in preview (e.g. Spark `3.0.0-preview`), or the engine does not yet have full feature compatibility compared to old versions yet. This stage allows Iceberg to release an engine version support without the need to wait for feature parity, shortening the release time.
2. **Maintained**: an engine version is actively maintained by the community. Users can expect parity for most features across all the maintained versions. If a feature has to leverage some new engine functionalities that older versions don't have, then feature parity across maintained versions is not guaranteed.
3. **Deprecated**: an engine version is no longer actively maintained. People who are still interested in the version can backport any necessary feature or bug fix from newer versions, but the community will not spend effort in achieving feature parity. Iceberg recommends users to move towards a newer version. Contributions to a deprecated version is expected to diminish over time, so that eventually no change is added to a deprecated version.
4. **End-of-life**: a vote can be initiated in the community to fully remove a deprecated version out of the Iceberg repository to mark as its end of life.

## Current Engine Version Lifecycle Status

### Apache Spark

| Version    | Lifecycle Stage    | Initial Iceberg Support | Latest Iceberg Support | Latest Runtime Jar |
| ---------- | ------------------ | ----------------------- |------------------------| ------------------ |
| 2.4        | End of Life        | 0.7.0-incubating        | 1.2.1                  | [iceberg-spark-runtime-2.4](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-2.4/1.2.1/iceberg-spark-runtime-2.4-1.2.1.jar) |
| 3.0        | End of Life        | 0.9.0                   | 1.0.0                  | [iceberg-spark-runtime-3.0_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.0_2.12/1.0.0/iceberg-spark-runtime-3.0_2.12-1.0.0.jar) |
| 3.1        | Deprecated         | 0.12.0                  | {{ icebergVersion }} | [iceberg-spark-runtime-3.1_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.1_2.12-{{ icebergVersion }}.jar) [1] |
| 3.2        | Maintained         | 0.13.0                  | {{ icebergVersion }} | [iceberg-spark-runtime-3.2_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.2_2.12-{{ icebergVersion }}.jar) |
| 3.3        | Maintained         | 0.14.0                  | {{ icebergVersion }} | [iceberg-spark-runtime-3.3_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.3_2.12-{{ icebergVersion }}.jar) |
| 3.4        | Maintained         | 1.3.0                   | {{ icebergVersion }} | [iceberg-spark-runtime-3.4_2.12](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.4_2.12-{{ icebergVersion }}.jar) |

* [1] Spark 3.1 shares the same runtime jar `iceberg-spark3-runtime` with Spark 3.0 before Iceberg 0.13.0

### Apache Flink

Based on the guideline of the Flink community, only the latest 2 minor versions are actively maintained.
Users should continuously upgrade their Flink version to stay up-to-date.

| Version | Lifecycle Stage | Initial Iceberg Support | Latest Iceberg Support | Latest Runtime Jar                                           |
| ------- | --------------- | ----------------------- | ---------------------- | ------------------------------------------------------------ |
| 1.11    | End of Life     | 0.9.0                   | 0.12.1                 | [iceberg-flink-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.12.1/iceberg-flink-runtime-0.12.1.jar) |
| 1.12    | End of Life     | 0.12.0                  | 0.13.1                 | [iceberg-flink-runtime-1.12](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.12/0.13.2/iceberg-flink-runtime-1.12-0.13.2.jar) [3] |
| 1.13    | End of Life     | 0.13.0                  | 1.0.0                  | [iceberg-flink-runtime-1.13](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.13/1.2.0/iceberg-flink-runtime-1.13-1.0.0.jar) |
| 1.14    | End of Life     | 0.13.0                  | 1.2.0                  | [iceberg-flink-runtime-1.14](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.14/1.2.0/iceberg-flink-runtime-1.14-1.2.0.jar) |
| 1.15    | End of Life     | 0.14.0                  | {{ icebergVersion }} | [iceberg-flink-runtime-1.15](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.15/{{ icebergVersion }}/iceberg-flink-runtime-1.15-{{ icebergVersion }}.jar) |
| 1.16    | Deprecated      | 1.1.0                   | {{ icebergVersion }} | [iceberg-flink-runtime-1.16](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.16/{{ icebergVersion }}/iceberg-flink-runtime-1.16-{{ icebergVersion }}.jar) |
| 1.17    | Maintained      | 1.3.0                   | {{ icebergVersion }} | [iceberg-flink-runtime-1.17](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.17/{{ icebergVersion }}/iceberg-flink-runtime-1.17-{{ icebergVersion }}.jar) |
| 1.18    | Maintained      | 1.5.0                   | {{ icebergVersion }} | [iceberg-flink-runtime-1.18](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.18/{{ icebergVersion }}/iceberg-flink-runtime-1.18-{{ icebergVersion }}.jar) |

* [3] Flink 1.12 shares the same runtime jar `iceberg-flink-runtime` with Flink 1.11 before Iceberg 0.13.0

### Apache Hive

| Version        | Recommended minor version | Lifecycle Stage   | Initial Iceberg Support | Latest Iceberg Support | Latest Runtime Jar |
| -------------- | ------------------------- | ----------------- | ----------------------- | ---------------------- | ------------------ |
| 2              | 2.3.8                     | Maintained        | 0.8.0-incubating        | {{ icebergVersion }} | [iceberg-hive-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{ icebergVersion }}/iceberg-hive-runtime-{{ icebergVersion }}.jar) |
| 3              | 3.1.2                     | Maintained        | 0.10.0                  | {{ icebergVersion }} | [iceberg-hive-runtime](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{ icebergVersion }}/iceberg-hive-runtime-{{ icebergVersion }}.jar) |

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

If bringing an engine directly to the Iceberg main repository is needed, please raise a discussion thread in the [Iceberg community](community.md).
