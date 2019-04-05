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

[![](https://travis-ci.org/apache/incubator-iceberg.svg?branch=master)](https://travis-ci.org/apache/incubator-iceberg)
[![](https://badges.gitter.im/iceberg-tables/Lobby.svg)](https://gitter.im/iceberg-tables/Lobby)

Apache Iceberg (incubating) is a new table format for storing large, slow-moving tabular data. It is designed to improve on the de-facto standard table layout built into Hive, Presto, and Spark.

Background and documentation is available at <https://iceberg.incubator.apache.org>


## Status

Iceberg is under active development in the Apache Incubator.

The core Java library that tracks table snapshots and metadata is complete, but still evolving. Current work is focused on integrating Iceberg into Spark and Presto.

The [Iceberg format specification][iceberg-spec] is being actively updated and is open for comment. Until the specification is complete and released, it carries no compatibility guarantees. The spec is currently evolving as the Java reference implementation changes.

[Java API javadocs][iceberg-javadocs] are available for the 0.6.0 tag.

[iceberg-javadocs]: https://iceberg.apache.org/javadoc/0.6.0/index.html?com/netflix/iceberg/package-summary.html
[iceberg-spec]: https://docs.google.com/document/d/1Q-zL5lSCle6NEEdyfiYsXYzX_Q8Qf0ctMyGBKslOswA/edit?usp=sharing 


## Collaboration

Iceberg tracks issues in GitHub and prefers to receive contributions as pull requests.

Community discussions happen primarily on the [dev mailing list][dev-list] or on specific issues.

[dev-list]: mailto:dev@iceberg.apache.org


### Building

Iceberg is built using Gradle 4.4.

Iceberg table support is organized in library modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API
* `iceberg-core` contains implementations of the Iceberg API and support for Avro data files, **this is what processing engines should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-orc` is an optional module for working with tables backed by ORC files (*experimental*)
* `iceberg-hive` is an implementation of iceberg tables backed by hive metastore thrift client

This project Iceberg also has modules for adding Iceberg support to processing engines:

* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg (use iceberg-runtime for a shaded version)
* `iceberg-data` is a client library used to read Iceberg tables from JVM applications
* `iceberg-pig` is an implementation of Pig's LoadFunc API for Iceberg
* `iceberg-presto-runtime` generates a shaded runtime jar that is used by presto to integrate with iceberg tables

### Compatibility

Iceberg's Spark integration is compatible with the following Spark versions:

| Iceberg version | Spark version |
| --------------- | ------------- |
| 0.2.0+ *        | 2.3.0         |
| 0.3.0+ *        | 2.3.2         |
| master branch   | 2.4.0         |

An asterisk (*) refers to releases under the now deprecated [Netflix/iceberg](https://github.com/Netflix/iceberg) repo.

