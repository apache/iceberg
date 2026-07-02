---
title: "Developer Snapshot Testing"
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

{# Mirrors getProjectVersion() in build.gradle for released versions: bump minor, reset patch to 0, and append -SNAPSHOT. #}
{% set icebergVersionParts = icebergVersion.split('.') %}
{% set snapshotMinorVersion = (icebergVersionParts[1] | int) + 1 %}
{% set snapshotVersion = icebergVersionParts[0] ~ '.' ~ snapshotMinorVersion ~ '.0-SNAPSHOT' %}

# Developer Snapshot Testing

!!! warning "For Iceberg developers only"

    Nightly snapshots are **unreleased** development artifacts. They are **not**
    official Apache releases, are not intended for general use, may change or
    break at any time, and **must not be used in production**. If you are not
    actively participating in Iceberg development or following dev list
    discussions, use an official [release](releases.md) instead.

Every night, Apache Iceberg publishes snapshots of every module from unreleased
changes on `main` to support active Iceberg developers, engine maintainers, and
automated testing.
Per the ASF [release policy](https://apache.org/legal/release-policy#publication),
unreleased artifacts are developer resources for testing ongoing development and
are not a substitute for official releases.

## Snapshot version

Snapshots are published daily at 00:00 UTC under the version `{{ snapshotVersion }}`,
which tracks the next unreleased development version by incrementing the latest
release minor version and resetting the patch version to 0.

## Development-only usage

Active developers who need to validate unreleased Iceberg changes can use the
[Apache snapshot repository](https://repository.apache.org/content/repositories/snapshots/org/apache/iceberg/)
when a development task or [dev list](community.md#mailing-lists) discussion
calls for testing snapshots. Add snapshot dependencies only to temporary local
test builds; do not commit them to production applications, user documentation,
or release validation workflows.

The examples below use `iceberg-core` as the module under test. Replace it only
with the Iceberg module needed for the development task you are validating.

=== "Gradle (development only)"

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

=== "Maven (development only)"

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

=== "sbt (development only)"

    ```scala
    resolvers += "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots"

    libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "{{ snapshotVersion }}"
    ```

=== "Spark (development only)"

    ```sh
    spark-shell \
      --repositories https://repository.apache.org/content/repositories/snapshots \
      --packages org.apache.iceberg:iceberg-spark-runtime-{{ sparkVersionMajor }}:{{ snapshotVersion }}
    ```
