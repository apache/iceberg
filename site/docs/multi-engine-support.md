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

Multi-engine support is a core tenant of Apache Iceberg.
The community continuously improves Iceberg core library components to enable integrations with different compute engines that power analytics, business intelligence, machine learning, etc.
Support of [Apache Spark](../spark-configuration), [Apache Flink](../flink) and [Apache Hive](../hive) are provided inside the Iceberg main repository.

## Multi-Version Support

Engines maintained within the Iceberg repository has multi-version support.
This means each new version of an engine that introduces backwards incompatible upgrade has its dedicated integration codebase and release artifacts.
For example, integration with Spark 3.1 is under `/spark/v3.1`, and integration with Spark 3.2 is under `/spark/v3.2`, 
and they release different artifacts (`iceberg-spark-3.1_2.12` and `iceberg-spark-3.2_2.12`) for users to consume.
By doing this, changes in minor versions are isolated. New features in Iceberg could be developed against the latest features of an engine without breaking support of old APIs in past engine versions.

## Engine Version Lifecycle

Each engine version undergoes the following lifecycle stages:

1. **Beta**: a new engine version is supported, but still in the experimental stage. Maybe the engine version itself is still in preview (e.g. Spark `3.0.0-preview`), or the engine does not yet have full feature compatibility compared to old versions yet. This stage allows Iceberg to release an engine version support without the need to wait for feature parity, shortening the release time.

2. **Maintained**: an engine version is actively maintained by the community. Users can expect parity for most features across all the maintained versions. If a feature has to leverage some new engine functionalities that older versions don't have, then feature parity is not guaranteed. For code contributors,
    - New features should always be prioritized first in the latest version, even if the latest version is still in beta.
    - For features that could be backported, the contributor is encouraged to either also perform backports in separated PRs, or at least create some issues to track the backport.
    - If the change is small enough, updating all versions at once is acceptable. Otherwise, using separated PRs for each version is recommended.

3. **Deprecating**: an engine version is no longer actively maintained. People who are still interested in the version can backport any necessary feature or bug fix from newer versions. But the community will not spend effort in achieving feature parity. Iceberg recommends users to move towards a newer version. Contributions to a deprecating version is expected to diminish over time, so that eventually no change is added to a deprecating version.

4. **End-of-life**: a vote can be initiated to fully remove a deprecating version out of the Iceberg repository to mark as its end of life.

## Current Engine Version Lifecycle Status

### Apache Spark

| Version    | Lifecycle Stage    | 
| ---------- | ------------------ | 
| 2.4        | Deprecating        | 
| 3.0        | Maintained         |
| 3.1        | Maintained         |
| 3.2        | Beta               |

### Apache Flink

Based on the guideline of the Flink community, only the latest 2 minor versions are actively maintained.
Users should continuously upgrade their Flink version to stay up-to-date.

| Version    | Lifecycle Stage   | 
| ---------- | ----------------- | 
| 1.12       | Deprecating       | 
| 1.13       | Maintained        |
| 1.14       | Maintained        |

### Apache Hive

| Version    | Lifecycle Stage   | 
| ---------- | ----------------- | 
| 2          | Maintained (recommended version >= 2.3)  | 
| 3          | Maintained        |

## Supporting New Engines

Iceberg recommends new engines to build support by importing the Iceberg libraries to the engine's project.
This allows the Iceberg support to evolve with the engine.
Projects such as [Trino](https://trino.io/docs/current/connector/iceberg.html) and [PrestoDB](https://prestodb.io/docs/current/connector/iceberg.html) are good examples of such support strategy.

In this approach, an Iceberg version upgrade is needed for an engine to consume new Iceberg features.
To facilitate engine development against unreleased Iceberg features, a snapshot release version could be found at the [Apache snapshot repository](https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/).

