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

![](site/docs/img/Iceberg-logo.png)

[![](https://github.com/apache/iceberg/actions/workflows/java-ci.yml/badge.svg)](https://github.com/apache/iceberg/actions/workflows/java-ci.yml)
[![](https://github.com/apache/iceberg/actions/workflows/python-ci.yml/badge.svg)](https://github.com/apache/iceberg/actions/workflows/python-ci.yml)
[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://the-asf.slack.com/archives/CF01LKV9S)

Apache Iceberg is a new table format for storing large, slow-moving tabular data. It is designed to improve on the de-facto standard table layout built into Hive, Trino, and Spark.

Background and documentation is available at <https://iceberg.apache.org>


## Status

Iceberg is under active development at the Apache Software Foundation.

The core Java library that tracks table snapshots and metadata is complete, but still evolving. Current work is focused on adding row-level deletes and upserts, and integration work with new engines like Flink and Hive.

The [Iceberg format specification][iceberg-spec] is being actively updated and is open for comment. Until the specification is complete and released, it carries no compatibility guarantees. The spec is currently evolving as the Java reference implementation changes.

[Java API javadocs][iceberg-javadocs] are available for the master.

[iceberg-javadocs]: https://iceberg.apache.org/javadoc/master
[iceberg-spec]: https://iceberg.apache.org/spec


## Collaboration

Iceberg tracks issues in GitHub and prefers to receive contributions as pull requests.

Community discussions happen primarily on the [dev mailing list][dev-list] or on specific issues.

[dev-list]: mailto:dev@iceberg.apache.org


### Building

Iceberg is built using Gradle 5.4.1 with Java 1.8 or Java 11.

* To invoke a build and run tests: `./gradlew build`
* To skip tests: `./gradlew build -x test -x testSpark31 -x integrationTest -x spark31IntegrationTest`

Iceberg table support is organized in library modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-arrow` is an optional module for reading Parquet into Arrow memory
* `iceberg-orc` is an optional module for working with tables backed by ORC files
* `iceberg-hive-metastore` is an implementation of Iceberg tables backed by the Hive metastore Thrift client
* `iceberg-data` is an optional module for working with tables directly from JVM applications

This project Iceberg also has modules for adding Iceberg support to processing engines:

* `iceberg-spark2` is an implementation of Spark's Datasource V2 API in 2.4 for Iceberg (use iceberg-spark-runtime for a shaded version)
* `iceberg-spark3` is an implementation of Spark's Datasource V2 API in 3.0 for Iceberg (use iceberg-spark3-runtime for a shaded version)
* `iceberg-flink` contains classes for integrating with Apache Flink (use iceberg-flink-runtime for a shaded version)
* `iceberg-mr` contains an InputFormat and other classes for integrating with Apache Hive
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg

### Compatibility

Iceberg's Spark integration is compatible with Spark 2.4 and Spark 3.0 using the modules in the following table:

| Iceberg version | Spark 2.4.x   | Spark 3.0.x    |
| --------------- | ------------- | -------------- |
| master branch   | spark-runtime | spark3-runtime |
| 0.9.0           | spark-runtime | spark3-runtime |
| 0.8.0           | spark-runtime |                |

