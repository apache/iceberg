---
title: Reliability
url: reliability
aliases:
    - "tables/reliability"
menu:
    main:
        parent: Tables
        identifier: tables_reliability
        weight: 0
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

# Reliability

Iceberg was designed to solve correctness problems that affect Hive tables running in S3.

Hive tables track data files using both a central metastore for partitions and a file system for individual files. This makes atomic changes to a table's contents impossible, and eventually consistent stores like S3 may return incorrect results due to the use of listing files to reconstruct the state of a table. It also requires job planning to make many slow listing calls: O(n) with the number of partitions.

Iceberg tracks the complete list of data files in each [snapshot](../../../terms#snapshot) using a persistent tree structure. Every write or delete produces a new snapshot that reuses as much of the previous snapshot's metadata tree as possible to avoid high write volumes.

Valid snapshots in an Iceberg table are stored in the table metadata file, along with a reference to the current snapshot. Commits replace the path of the current table metadata file using an atomic operation. This ensures that all updates to table data and metadata are atomic, and is the basis for [serializable isolation](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable).

This results in improved reliability guarantees:

* **Serializable isolation**: All table changes occur in a linear history of atomic table updates
* **Reliable reads**: Readers always use a consistent snapshot of the table without holding a lock
* **Version history and rollback**: Table snapshots are kept as history and tables can roll back if a job produces bad data
* **Safe file-level operations**. By supporting atomic changes, Iceberg enables new use cases, like safely compacting small files and safely appending late data to tables

This design also has performance benefits:

* **O(1) RPCs to plan**: Instead of listing O(n) directories in a table to plan a job, reading a snapshot requires O(1) RPC calls
* **Distributed planning**: File pruning and predicate push-down is distributed to jobs, removing the metastore as a bottleneck
* **Finer granularity partitioning**: Distributed planning and O(1) RPC calls remove the current barriers to finer-grained partitioning


## Concurrent write operations

Iceberg supports multiple concurrent writes using optimistic concurrency.

Each writer assumes that no other writers are operating and writes out new table metadata for an operation. Then, the writer attempts to commit by atomically swapping the new table metadata file for the existing metadata file.

If the atomic swap fails because another writer has committed, the failed writer retries by writing a new metadata tree based on the new current table state.

### Cost of retries

Writers avoid expensive retry operations by structuring changes so that work can be reused across retries.

For example, appends usually create a new manifest file for the appended data files, which can be added to the table without rewriting the manifest on every attempt.

### Retry validation

Commits are structured as assumptions and actions. After a conflict, a writer checks that the assumptions are met by the current table state. If the assumptions are met, then it is safe to re-apply the actions and commit.

For example, a compaction might rewrite `file_a.avro` and `file_b.avro` as `merged.parquet`. This is safe to commit as long as the table still contains both `file_a.avro` and `file_b.avro`. If either file was deleted by a conflicting commit, then the operation must fail. Otherwise, it is safe to remove the source files and add the merged file.


## Compatibility

By avoiding file listing and rename operations, Iceberg tables are compatible with any object store. No consistent listing is required.
