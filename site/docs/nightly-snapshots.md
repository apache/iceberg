---
title: "Nightly Snapshots"
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

{% set snapshotVersion = icebergVersion.split('.')[0] ~ '.' ~ ((icebergVersion.split('.')[1] | int) + 1) ~ '.0-SNAPSHOT' %}

# Nightly Snapshots

Every night, Apache Iceberg publishes a snapshot of every module to the
[Apache snapshot repository](https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/).
These nightly snapshots let developers build and test against the latest unreleased
changes on `main` before they are included in an official release.

!!! warning "For Iceberg developers only"

    Snapshots are **unreleased**, in-development builds. They are **not** an official
    Apache release, may change or break at any time, and **must not be used in
    production**. For production use, always depend on an official
    [release](releases.md). Per the ASF
    [release policy](https://apache.org/legal/release-policy#publication), unreleased
    artifacts are intended only for developers who are actively participating in
    Iceberg development.

## Snapshot version

Snapshots are published daily at 00:00 UTC under the version `{{ snapshotVersion }}`,
which tracks the next unreleased version (the latest release with its minor version
incremented). Browse the
[snapshot repository listing](https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/)
to see which modules and versions are available.

## Usage

Add the Apache snapshot repository to your build and depend on the `{{ snapshotVersion }}`
version of the Iceberg modules you need. The examples below use `iceberg-core`; replace
it with the artifacts your project requires, such as an engine runtime like
`iceberg-spark-runtime-{{ sparkVersionMajor }}`.

=== "Gradle"

    In `build.gradle`:

    ```gradle
    repositories {
      mavenCentral()
      maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots")
      }
    }

    dependencies {
      implementation "org.apache.iceberg:iceberg-core:{{ snapshotVersion }}"
    }
    ```

=== "Maven"

    In `pom.xml`:

    ```xml
    <repositories>
      <repository>
        <id>apache-snapshots</id>
        <url>https://repository.apache.org/content/repositories/snapshots</url>
        <snapshots>
          <enabled>true</enabled>
        </snapshots>
      </repository>
    </repositories>

    <dependencies>
      <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-core</artifactId>
        <version>{{ snapshotVersion }}</version>
      </dependency>
    </dependencies>
    ```

=== "sbt"

    In `build.sbt`:

    ```scala
    resolvers += "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "{{ snapshotVersion }}"
    ```

=== "Spark"

    Pass the snapshot repository and a snapshot runtime to `spark-shell` (or
    `spark-sql`, `pyspark`, or `spark-submit`):

    ```sh
    spark-shell \
      --repositories https://repository.apache.org/content/repositories/snapshots \
      --packages org.apache.iceberg:iceberg-spark-runtime-{{ sparkVersionMajor }}:{{ snapshotVersion }}
    ```
