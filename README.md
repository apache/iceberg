<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

![Iceberg](https://iceberg.apache.org/docs/latest/img/Iceberg-logo.png)

[![](https://github.com/apache/iceberg/actions/workflows/java-ci.yml/badge.svg)](https://github.com/apache/iceberg/actions/workflows/java-ci.yml)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://apache-iceberg.slack.com/)

Iceberg is a high-performance format for huge analytic tables. Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time.

Background and documentation is available at <https://iceberg.apache.org>


## Status

Iceberg is under active development at the Apache Software Foundation.

The core Java library that tracks table snapshots and metadata is complete, but still evolving. Current work is focused on adding row-level deletes and upserts, and integration work with new engines like Flink and Hive.

The [Iceberg format specification][iceberg-spec] is being actively updated and is open for comment. Until the specification is complete and released, it carries no compatibility guarantees. The spec is currently evolving as the Java reference implementation changes.

[Java API javadocs][iceberg-javadocs] are available for the main.

[iceberg-javadocs]: https://iceberg.apache.org/javadoc/main
[iceberg-spec]: https://iceberg.apache.org/spec


## Collaboration

Iceberg tracks issues in GitHub and prefers to receive contributions as pull requests.

Community discussions happen primarily on the [dev mailing list][dev-list] or on specific issues.

[dev-list]: mailto:dev@iceberg.apache.org


### Building

Iceberg is built using Gradle with Java 8, 11, or 17.

* To invoke a build and run tests: `./gradlew build`
* To skip tests: `./gradlew build -x test -x integrationTest`
* To fix code style for default versions: `./gradlew spotlessApply`
* To fix code style for all versions of Spark/Hive/Flink:`./gradlew spotlessApply -DallVersions`

Iceberg table support is organized in library modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-arrow` is an optional module for reading Parquet into Arrow memory
* `iceberg-orc` is an optional module for working with tables backed by ORC files
* `iceberg-hive-metastore` is an implementation of Iceberg tables backed by the Hive metastore Thrift client
* `iceberg-data` is an optional module for working with tables directly from JVM applications

Iceberg also has modules for adding Iceberg support to processing engines:

* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg with submodules for each spark versions (use runtime jars for a shaded version)
* `iceberg-flink` contains classes for integrating with Apache Flink (use iceberg-flink-runtime for a shaded version)
* `iceberg-mr` contains an InputFormat and other classes for integrating with Apache Hive
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg

---
**NOTE** 

The tests require Docker to execute. On MacOS (with Docker Desktop), you might need to create a symbolic name to the docker socket in order to be detected by the tests:

```
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```
---

### Engine Compatibility

See the [Multi-Engine Support](https://iceberg.apache.org/multi-engine-support/) page to know about Iceberg compatibility with different Spark, Flink and Hive versions.
For other engines such as Presto or Trino, please visit their websites for Iceberg integration details.

### Implementations

This repository contains the Java implementation of Iceberg. Other implementations can be found at:

* **Go**: [iceberg-go](https://github.com/apache/iceberg-go)
* **PyIceberg** (Python): [iceberg-python](https://github.com/apache/iceberg-python)
* **Rust**: [iceberg-rust](https://github.com/apache/iceberg-rust)
