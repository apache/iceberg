---
title: Performance
url: performance
aliases:
    - "tables/performance"
menu:
    main:
        parent: Tables
        identifier: tables_performance
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

# Performance

* Iceberg is designed for huge tables and is used in production where a *single table* can contain tens of petabytes of data.
* Even multi-petabyte tables can be read from a single node, without needing a distributed SQL engine to sift through table metadata.

## Scan planning

Scan planning is the process of finding the files in a table that are needed for a query.

Planning in an Iceberg table fits on a single node because Iceberg's metadata can be used to prune *metadata* files that aren't needed, in addition to filtering *data* files that don't contain matching data.

Fast scan planning from a single node enables:

* Lower latency SQL queries -- by eliminating a distributed scan to plan a distributed scan
* Access from any client -- stand-alone processes can read data directly from Iceberg tables

### Metadata filtering

Iceberg uses two levels of metadata to track the files in a snapshot.

* **Manifest files** store a list of data files, along each data file's partition data and column-level stats
* A **manifest list** stores the snapshot's list of manifests, along with the range of values for each partition field

For fast scan planning, Iceberg first filters manifests using the partition value ranges in the manifest list. Then, it reads each manifest to get data files. With this scheme, the manifest list acts as an index over the manifest files, making it possible to plan without reading all manifests.

In addition to partition value ranges, a manifest list also stores the number of files added or deleted in a manifest to speed up operations like snapshot expiration.

### Data filtering

Manifest files include a tuple of partition data and column-level stats for each data file.

During planning, query predicates are automatically converted to predicates on the partition data and applied first to filter data files. Next, column-level value counts, null counts, lower bounds, and upper bounds are used to eliminate files that cannot match the query predicate.

By using upper and lower bounds to filter data files at planning time, Iceberg uses clustered data to eliminate splits without running tasks. In some cases, this is a [10x performance improvement](https://conferences.oreilly.com/strata/strata-ny-2018/cdn.oreillystatic.com/en/assets/1/event/278/Introducing%20Iceberg_%20Tables%20designed%20for%20object%20stores%20Presentation.pdf
).
