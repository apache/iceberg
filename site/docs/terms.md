---
title: "Terms"
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

# Terms

## Catalog

### Overview

You may think of Iceberg as a format for managing data in a single table, but the Iceberg library needs a way to keep track of those tables by name. Tasks like creating, dropping, and renaming tables are the responsibility of a catalog. Catalogs manage a collection of tables that are usually grouped into namespaces. The most important responsibility of a catalog is tracking a table's current metadata, which is provided by the catalog when you load a table.

The first step when using an Iceberg client is almost always initializing and configuring a catalog. The configured catalog is then used by compute engines to execute catalog operations. Multiple types of compute engines using a shared Iceberg catalog allows them to share a common data layer.

A catalog is almost always configured through the processing engine which passes along a set of properties during initialization. Different processing engines have different ways to configure a catalog. When configuring a catalog, it’s always best to refer to the [Iceberg documentation](../docs/latest/configuration.md#catalog-properties) as well as the docs for the specific processing engine being used. Ultimately, these configurations boil down to a common set of catalog properties that will be passed to configure the Iceberg catalog.

### Catalog Implementations

Iceberg catalogs are flexible and can be implemented using almost any backend system. They can be plugged into any Iceberg runtime, and allow any processing engine that supports Iceberg to load the tracked Iceberg tables. Iceberg also comes with a number of catalog implementations that are ready to use out of the box.

This includes:

* REST: a server-side catalog that’s exposed through a REST API
* Hive Metastore: tracks namespaces and tables using a Hive metastore
* JDBC: tracks namespaces and tables in a simple JDBC database
* Nessie: a transactional catalog that tracks namespaces and tables in a database with git-like version control

There are more catalog types in addition to the ones listed here as well as custom catalogs that are developed to include specialized functionality.

### Decoupling Using the REST Catalog

The REST catalog was introduced in the Iceberg 0.14.0 release and provides greater control over how Iceberg catalogs are implemented. Instead of using technology-specific logic contained in the catalog clients, the implementation details of a REST catalog lives on the catalog server. If you’re familiar with Hive, this is somewhat similar to the Hive thrift service that allows access to a hive server over a single port. The server-side logic can be written in any language and use any custom technology, as long as the API follows the [Iceberg REST Open API specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

A great benefit of the REST catalog is that it allows you to use a single client to talk to any catalog backend. This increased flexibility makes
it easier to make custom catalogs compatible with engines like Athena or Starburst without requiring the inclusion of a Jar into the classpath.

## Snapshot

A **snapshot** is the state of a table at some time.

Each snapshot lists all of the data files that make up the table's contents at the time of the snapshot. Data files are stored across multiple [manifest](#manifest-file) files, and the manifests for a snapshot are listed in a single [manifest list](#manifest-list) file.

## Manifest list

A **manifest list** is a metadata file that lists the [manifests](#manifest-file) that make up a table snapshot.

Each manifest file in the manifest list is stored with information about its contents, like partition value ranges, used to speed up metadata operations.

## Manifest file

A **manifest file** is a metadata file that lists a subset of data files that make up a snapshot.

Each data file in a manifest is stored with a [partition tuple](#partition-tuple), column-level stats, and summary information used to prune splits during [scan planning](docs/latest/performance.md#scan-planning).

## Partition spec

A **partition spec** is a description of how to [partition](docs/latest/partitioning.md) data in a table.

A spec consists of a list of source columns and transforms. A transform produces a partition value from a source value. For example, `date(ts)` produces the date associated with a timestamp column named `ts`.

## Partition tuple

A **partition tuple** is a tuple or struct of partition data stored with each data file.

All values in a partition tuple are the same for all rows stored in a data file. Partition tuples are produced by transforming values from row data using a partition spec.

Iceberg stores partition values unmodified, unlike Hive tables that convert values to and from strings in file system paths and keys.

## Snapshot log (history table)

The **snapshot log** is a metadata log of how the table's current snapshot has changed over time.

The log is a list of timestamp and ID pairs: when the current snapshot changed and the snapshot ID the current snapshot was changed to.

The snapshot log is stored in [table metadata as `snapshot-log`](spec.md#table-metadata-fields).
