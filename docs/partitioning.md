---
title: Partitioning
url: partitioning
aliases:
    - "tables/partitioning"
menu:
    main:
        parent: Tables
        identifier: tables_partitioning
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

# Partitioning

## What is partitioning?

Partitioning is a way to make queries faster by grouping similar rows together when writing.

For example, queries for log entries from a `logs` table would usually include a time range, like this query for logs between 10 and 12 AM:

```sql
SELECT level, message FROM logs
WHERE event_time BETWEEN '2018-12-01 10:00:00' AND '2018-12-01 12:00:00';
```

Configuring the `logs` table to partition by the date of `event_time` will group log events into files with the same event date. Iceberg keeps track of that date and will use it to skip files for other dates that don't have useful data.

Iceberg can partition timestamps by year, month, day, and hour granularity. It can also use a categorical column, like `level` in this logs example, to store rows together and speed up queries.


## What does Iceberg do differently?

Other tables formats like Hive support partitioning, but Iceberg supports *hidden partitioning*.

* Iceberg handles the tedious and error-prone task of producing partition values for rows in a table.
* Iceberg avoids reading unnecessary partitions automatically. Consumers don't need to know how the table is partitioned and add extra filters to their queries.
* Iceberg partition layouts can evolve as needed.

### Partitioning in Hive

To demonstrate the difference, consider how Hive would handle a `logs` table.

In Hive, partitions are explicit and appear as a column, so the `logs` table would have a column called `event_date`. When writing, an insert needs to supply the data for the `event_date` column:

```sql
INSERT INTO logs PARTITION (event_date)
  SELECT level, message, event_time, format_time(event_time, 'YYYY-MM-dd')
  FROM unstructured_log_source;
```

Similarly, queries that search through the `logs` table must have an `event_date` filter in addition to an `event_time` filter.

```sql
SELECT level, count(1) as count FROM logs
WHERE event_time BETWEEN '2018-12-01 10:00:00' AND '2018-12-01 12:00:00'
  AND event_date = '2018-12-01';
```

If the `event_date` filter were missing, Hive would scan through every file in the table because it doesn't know that the `event_time` column is related to the `event_date` column.

### Problems with Hive partitioning

Hive must be given partition values. In the logs example, it doesn't know the relationship between `event_time` and `event_date`.

This leads to several problems:

* Hive can't validate partition values -- it is up to the writer to produce the correct value
    - Using the wrong format, `2018-12-01` instead of `20181201`, produces silently incorrect results, not query failures
    - Using the wrong source column, like `processing_time`, or time zone also causes incorrect results, not failures
* It is up to the user to write queries correctly
    - Using the wrong format also leads to silently incorrect results
    - Users that don't understand a table's physical layout get needlessly slow queries -- Hive can't translate filters automatically
* Working queries are tied to the table's partitioning scheme, so partitioning configuration cannot be changed without breaking queries

### Iceberg's hidden partitioning

Iceberg produces partition values by taking a column value and optionally transforming it. Iceberg is responsible for converting `event_time` into `event_date`, and keeps track of the relationship.

Table partitioning is configured using these relationships. The `logs` table would be partitioned by `date(event_time)` and `level`.

Because Iceberg doesn't require user-maintained partition columns, it can hide partitioning. Partition values are produced correctly every time and always used to speed up queries, when possible. Producers and consumers wouldn't even see `event_date`.

Most importantly, queries no longer depend on a table's physical layout. With a separation between physical and logical, Iceberg tables can evolve partition schemes over time as data volume changes. Misconfigured tables can be fixed without an expensive migration.

For details about all the supported hidden partition transformations, see the [Partition Transforms](../../../spec/#partition-transforms) section.

For details about updating a table's partition spec, see the [partition evolution](../evolution/#partition-evolution) section.
