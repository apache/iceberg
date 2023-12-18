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

### Snapshot

A **snapshot** is the state of a table at some time.

Each snapshot lists all of the data files that make up the table's contents at the time of the snapshot. Data files are stored across multiple [manifest](#manifest-file) files, and the manifests for a snapshot are listed in a single [manifest list](#manifest-list) file.

### Manifest list

A **manifest list** is a metadata file that lists the [manifests](#manifest-file) that make up a table snapshot.

Each manifest file in the manifest list is stored with information about its contents, like partition value ranges, used to speed up metadata operations.

### Manifest file

A **manifest file** is a metadata file that lists a subset of data files that make up a snapshot.

Each data file in a manifest is stored with a [partition tuple](#partition-tuple), column-level stats, and summary information used to prune splits during [scan planning](docs/latest/performance.md#scan-planning).

### Partition spec

A **partition spec** is a description of how to [partition](docs/latest/partitioning.md) data in a table.

A spec consists of a list of source columns and transforms. A transform produces a partition value from a source value. For example, `date(ts)` produces the date associated with a timestamp column named `ts`.

### Partition tuple

A **partition tuple** is a tuple or struct of partition data stored with each data file.

All values in a partition tuple are the same for all rows stored in a data file. Partition tuples are produced by transforming values from row data using a partition spec.

Iceberg stores partition values unmodified, unlike Hive tables that convert values to and from strings in file system paths and keys.

### Snapshot log (history table)

The **snapshot log** is a metadata log of how the table's current snapshot has changed over time.

The log is a list of timestamp and ID pairs: when the current snapshot changed and the snapshot ID the current snapshot was changed to.

The snapshot log is stored in [table metadata as `snapshot-log`](spec.md#table-metadata-fields).
