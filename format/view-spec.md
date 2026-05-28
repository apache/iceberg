---
title: "View Spec"
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

# Iceberg View Spec

## Background and Motivation

Most compute engines (e.g. Trino and Apache Spark) support views. A view is a logical table that can be referenced by future queries. Views do not contain any data. Instead, the query stored by the view is executed every time the view is referenced by another query.

Each compute engine stores the metadata of the view in its proprietary format in the metastore of choice. Thus, views created from one engine can not be read or altered easily from another engine even when engines share the metastore as well as the storage system. This document standardizes the view metadata for ease of sharing the views across engines.

## Goals

* A common metadata format for view metadata, similar to how Iceberg supports a common table format for tables.

## Overview

View metadata storage mirrors how Iceberg table metadata is stored and retrieved. View metadata is maintained in metadata files. All changes to view state create a new view metadata file and completely replace the old metadata using an atomic swap. Like Iceberg tables, this atomic swap is delegated to the metastore that tracks tables and/or views by name. The view metadata file tracks the view schema, custom properties, current and past versions, as well as other metadata.

Each metadata file is self-sufficient. It contains the history of the last few versions of the view and can be used to roll back the view to a previous version.

### Metadata Location

An atomic swap of one view metadata file for another provides the basis for making atomic changes. Readers use the version of the view that was current when they loaded the view metadata and are not affected by changes until they refresh and pick up a new metadata location.

Writers create view metadata files optimistically, assuming that the current metadata location will not be changed before the writer's commit. Once a writer has created an update, it commits by swapping the view's metadata file pointer from the base location to the new location.

### Materialized Views

Materialized views are a type of view with precomputed results from the view query stored as a table.
When queried, engines may return the precomputed data for the materialized views, shifting the cost of query execution to the precomputation step.

Iceberg materialized views are implemented as a combination of an Iceberg view and an underlying Iceberg table, the "storage-table", which stores the precomputed data.
Materialized View metadata is a superset of View metadata with an additional pointer to the storage table. The storage table is an Iceberg table with additional materialized view refresh state metadata.
Refresh metadata contains information about the "source tables", "source views", and/or "source materialized views", which are the tables/views/materialized views referenced in the query definition of the materialized view.

## Specification

### Terms

* **Schema** -- Names and types of fields in a view.
* **Version** -- The state of a view at some point in time.
* **Storage table** -- Iceberg table that stores the precomputed data of a materialized view.
* **Refresh state** -- A record stored in the storage table's snapshot summary that captures the state of source tables and views at the time of the last refresh operation.
* **Dependency graph** -- The graph of all source tables, views, and materialized views that a materialized view depends on, including nested dependencies.
* **Source table** -- A table reference that occurs in the query definition of a materialized view.
* **Source view** -- A view reference that occurs in the query definition of a materialized view.
* **Source materialized view** -- A materialized view reference that occurs in the query definition of a materialized view.

### View Metadata

The view version metadata file has the following fields:

| Requirement | Field name           | Description |
|-------------|----------------------|-------------|
| _required_  | `view-uuid`          | A UUID that identifies the view, generated when the view is created. Implementations must throw an exception if a view's UUID does not match the expected UUID after refreshing metadata |
| _required_  | `format-version`     | An integer version number for the view format; must be 1 |
| _required_  | `location`           | The view's base location; used to create metadata file locations |
| _required_  | `schemas`            | A list of known schemas |
| _required_  | `current-version-id` | ID of the current version of the view (`version-id`) |
| _required_  | `versions`           | A list of known [versions](#versions) of the view [1] |
| _required_  | `version-log`        | A list of [version log](#version-log) entries with the timestamp and `version-id` for every change to `current-version-id` |
| _optional_  | `properties`         | A string to string map of view properties [2] |

Notes:

1. The number of versions to retain is controlled by the view property: `version.history.num-entries`.
2. Properties are used for metadata such as `comment` and for settings that affect view maintenance. This is not intended to be used for arbitrary metadata.

#### Versions

Each version in `versions` is a struct with the following fields:

| Requirement | Field name          | Description                                                                   |
|-------------|---------------------|-------------------------------------------------------------------------------|
| _required_  | `version-id`        | ID for the version                                                            |
| _required_  | `schema-id`         | ID of the schema for the view version                                         |
| _required_  | `timestamp-ms`      | Timestamp when the version was created (ms from epoch)                        |
| _required_  | `summary`           | A string to string map of [summary metadata](#summary) about the version      |
| _required_  | `representations`   | A list of [representations](#representations) for the view definition         |
| _optional_  | `default-catalog`   | Catalog name to use when a reference in the SELECT does not contain a catalog |
| _required_  | `default-namespace` | Namespace to use when a reference in the SELECT is a single identifier        |
| _optional_  | `storage-table`     | A [storage table identifier](#storage-table-identifier) of the storage table |

When `default-catalog` is `null` or not set, the catalog in which the view is stored must be used as the default catalog.

When `storage-table` is `null` or not set, the entity is a common view, otherwise it is a materialized view. The storage table must be in the same catalog as the materialized view.

#### Summary

Summary is a string to string map of metadata about a view version. Common metadata keys are documented here.

| Requirement | Key              | Value |
|-------------|------------------|-------|
| _optional_  | `engine-name`    | Name of the engine that created the view version |
| _optional_  | `engine-version` | Version of the engine that created the view version |

#### Representations

View definitions can be represented in multiple ways. Representations are documented ways to express a view definition.

A view version can have more than one representation. All representations for a version must express the same underlying definition. Engines are free to choose the representation to use.

View versions are immutable. Once a version is created, it cannot be changed. This means that representations for a version cannot be changed. If a view definition changes (or new representations are to be added), a new version must be created.

Each representation is an object with at least one common field, `type`, that is one of the following:

* `sql`: a SQL SELECT statement that defines the view

Representations further define metadata for each type.

##### SQL representation

The SQL representation stores the view definition as a SQL SELECT, with metadata such as the SQL dialect.

A view version can have multiple SQL representations of different dialects, but only one SQL representation per dialect.

| Requirement | Field name          | Type           | Description |
|-------------|---------------------|----------------|-------------|
| _required_  | `type`              | `string`       | Must be `sql` |
| _required_  | `sql`               | `string`       | A SQL SELECT statement |
| _required_  | `dialect`           | `string`       | The dialect of the `sql` SELECT statement (e.g., "trino" or "spark") |

For example:

```sql
USE prod.default
```
```sql
CREATE OR REPLACE VIEW event_agg (
    event_count COMMENT 'Count of events',
    event_date) AS
SELECT
    COUNT(1), CAST(event_ts AS DATE)
FROM events
GROUP BY 2
```

This create statement would produce the following `sql` representation metadata:

| Field name          | Value |
|---------------------|-------|
| `type`              | `"sql"` |
| `sql`               | `"SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2"` |
| `dialect`           | `"spark"` |

If a create statement does not include column names or comments before `AS`, the fields should be omitted.

The `event_count` (with the `Count of events` comment) and `event_date` field aliases must be part of the view version's `schema`.

#### Version log

The version log tracks changes to the view's current version. This is the view's history and allows reconstructing what version of the view would have been used at some point in time.

Note that this is not the version's creation time, which is stored in each version's metadata. A version can appear multiple times in the version log, indicating that the view definition was rolled back.

Each entry in `version-log` is a struct with the following fields:

| Requirement | Field name     | Description |
|-------------|----------------|-------------|
| _required_  | `timestamp-ms` | Timestamp when the view's `current-version-id` was updated (ms from epoch) |
| _required_  | `version-id`   | ID that `current-version-id` was set to |

#### Storage Table Identifier

The table identifier for the storage table that stores the precomputed results.

| Requirement | Field name     | Description |
|-------------|----------------|-------------|
| _required_  | `namespace`    | A list of strings for namespace levels |
| _required_  | `name`         | A string specifying the name of the table |

### Storage table metadata

This section describes additional metadata for the storage table that supplements the regular table metadata and is required for materialized views.
The `refresh-state` property is set on the [snapshot summary](https://iceberg.apache.org/spec/#snapshots) property of a storage table snapshot to provide information about the state of the precomputed data.

| Requirement | Field name      | Description |
|-------------|-----------------|-------------|
| _optional_  | `refresh-state` | A [refresh state](#refresh-state) record stored as a JSON-encoded string |

#### Freshness

A materialized view is **fresh** when the storage table represents the result of the current view query.

A change to the materialized view's definition produces a new `view-version-id`; any storage-table snapshot recorded at a prior `view-version-id` is not fresh and should not be consumed until refreshed.

#### Refresh state

The refresh state record captures the state of dependencies in the materialized view's dependency graph. A dependency is recorded in `source-states` as either a `table` entry (a source table or an upstream materialized view's storage table) and/or a `view` entry. Upstream materialized views can be stored as a `view` and a `table` entry.

The refresh state has the following fields:

| Requirement | Field name                   | Description |
|-------------|------------------------------|-------------|
| _required_  | `view-version-id`            | The `version-id` of the materialized view when the refresh operation was performed |
| _required_  | `source-states`              | A list of [source state](#source-state) records |
| _required_  | `refresh-start-timestamp-ms` | A timestamp of when the refresh operation was started |

##### Producer: Recording Refresh State

Producers may selectively choose a subset of their dependencies to record — for example, skipping non-Iceberg sources or recording an empty list. See [Appendix B](#appendix-b-what-counts-as-a-dependency) for strategies on how to store dependency state.

When writing the refresh state, producers:

- **Must** record `view-version-id` and `refresh-start-timestamp-ms`.
- **Should** include all distinct source states for the inputs they chose to track (diamond dependency pattern).
- **May** leave `source-states` empty (e.g., when sources are non-Iceberg or freshness is determined by a mechanism outside this spec).

##### Consumer: Evaluating Refresh State

Consumers may use any combination of the following to assess the state of dependencies used to produce the storage table.

- **Recency policy.** Accept the storage table when `refresh-start-timestamp-ms` falls within a staleness window. A recency policy bounds data age but does not establish freshness.
- **Trust the recorded `source-states`.** Compare each entry against the current catalog state — `snapshot-id` for tables, `version-id` for views, optionally recursive verification for upstream materialized views recorded by their storage tables. Also confirm that the recorded `view-version-id` equals the materialized view's current `view-version-id`.
- **Verify by parsing the view query.** Derive the dependency set from the SQL and confirm every dependency is covered by `source-states` and matches the current state. Treat any uncovered dependency as undetermined.

If a consumer's assessment passes, it reads from the storage table. If not, the consumer may fail the query, evaluate the view query directly, or apply another strategy.

#### Source state

Source state records capture the state of objects referenced by a materialized view. Each record has a `type` field that determines its form:

| Type    | Description |
|---------|-------------|
| `table` | An Iceberg table — either a source table in the dependency graph, or the storage table of an upstream materialized view |
| `view`  | An Iceberg view in the dependency graph |

An upstream materialized view may be recorded as a `view` entry referencing its view metadata and one ore more `table` entries referencing its storage table or other source tables. These source table entries might be determined by recursively expanding its own dependencies.

#### Source table state

A source table record captures the state of a source table (including a source materialized view's storage table) at the time of the last refresh operation.

| Requirement | Field name    | Description |
|-------------|---------------|-------------|
| _required_  | `type`        | A string that must be set to `table` |
| _required_  | `name`        | A string specifying the name of the source table |
| _required_  | `namespace`   | A list of strings for namespace levels |
| _optional_  | `catalog`     | An optional name of the catalog. If not set, the catalog is the same as the materialized view's |
| _required_  | `uuid`        | The uuid of the source table |
| _required_  | `snapshot-id` | The snapshot-id of the source table that was read during the refresh operation |
| _optional_  | `ref`         | Branch name of the source table being referenced in the view query |

When `ref` is `null` or not set, it defaults to `main`.

#### Source view state

A source view record captures the state of a source view at the time of the last refresh operation.

| Requirement | Field name   | Description |
|-------------|--------------|-------------|
| _required_  | `type`       | A string that must be set to `view` |
| _required_  | `name`       | A string specifying the name of the source view |
| _required_  | `namespace`  | A list of strings for namespace levels |
| _optional_  | `catalog`    | An optional name of the catalog. If not set, the catalog is the same as the materialized view's |
| _required_  | `uuid`       | The uuid of the source view |
| _required_  | `version-id` | The version-id of the source view that was read during the refresh operation |

#### Storage table creation and configuration

When processing a `CREATE MATERIALIZED VIEW` statement, query engines must:

1. Create the storage table as a regular Iceberg table with any specified configurations (partitioning, sort order, compression, etc.).
2. Create the materialized view metadata with a `storage-table` reference pointing to the created storage table.

The storage table must exist and be accessible before or at the time the materialized view metadata is committed.

A storage table that has not yet been refreshed has no snapshots. After a refresh, even if the query result is empty, the storage table will contain a snapshot with the `refresh-state` property in its summary. Consumers can use the presence of a snapshot with `refresh-state` to distinguish a never-refreshed storage table from one that was refreshed with an empty result.

## Appendix A: Examples

### View Example

The JSON metadata file format is described using an example below.

Imagine the following sequence of operations:

```sql
USE prod.default
```
```sql
CREATE OR REPLACE VIEW event_agg (
    event_count COMMENT 'Count of events',
    event_date)
COMMENT 'Daily event counts'
AS
SELECT
    COUNT(1), CAST(event_ts AS DATE)
FROM events
GROUP BY 2
```

The metadata JSON file created looks as follows.

The path is intentionally similar to the path for Iceberg tables and uses a `metadata` directory.

```
s3://bucket/warehouse/default.db/event_agg/metadata/00001-(uuid).metadata.json
```
```json
{
  "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
  "format-version" : 1,
  "location" : "s3://bucket/warehouse/default.db/event_agg",
  "current-version-id" : 1,
  "properties" : {
    "comment" : "Daily event counts"
  },
  "versions" : [ {
    "version-id" : 1,
    "timestamp-ms" : 1573518431292,
    "schema-id" : 1,
    "default-catalog" : "prod",
    "default-namespace" : [ "default" ],
    "summary" : {
      "engine-name" : "Spark",
      "engine-version" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
      "dialect" : "spark"
    } ]
  } ],
  "schemas": [ {
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 1,
      "name" : "event_count",
      "required" : false,
      "type" : "int",
      "doc" : "Count of events"
    }, {
      "id" : 2,
      "name" : "event_date",
      "required" : false,
      "type" : "date"
    } ]
  } ],
  "version-log" : [ {
    "timestamp-ms" : 1573518431292,
    "version-id" : 1
  } ]
}
```

Each change creates a new metadata JSON file.
In the below example, the underlying SQL is modified by specifying the fully-qualified table name.

```sql
USE prod.other_db;
CREATE OR REPLACE VIEW default.event_agg (
    event_count COMMENT 'Count of events',
    event_date)
COMMENT 'Daily event counts'
AS
SELECT
    COUNT(1), CAST(event_ts AS DATE)
FROM prod.default.events
GROUP BY 2
```

Updating the view produces a new metadata file that completely replaces the old:

```
s3://bucket/warehouse/default.db/event_agg/metadata/00002-(uuid).metadata.json
```
```json
{
  "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
  "format-version" : 1,
  "location" : "s3://bucket/warehouse/default.db/event_agg",
  "current-version-id" : 2,
  "properties" : {
    "comment" : "Daily event counts"
  },
  "versions" : [ {
    "version-id" : 1,
    "timestamp-ms" : 1573518431292,
    "schema-id" : 1,
    "default-catalog" : "prod",
    "default-namespace" : [ "default" ],
    "summary" : {
      "engine-name" : "Spark",
      "engine-version" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
      "dialect" : "spark"
    } ]
  }, {
    "version-id" : 2,
    "timestamp-ms" : 1573518981593,
    "schema-id" : 1,
    "default-catalog" : "prod",
    "default-namespace" : [ "default" ],
    "summary" : {
      "engine-name" : "Spark",
      "engine-version" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM prod.default.events\nGROUP BY 2",
      "dialect" : "spark"
    } ]
  } ],
  "schemas": [ {
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 1,
      "name" : "event_count",
      "required" : false,
      "type" : "int",
      "doc" : "Count of events"
    }, {
      "id" : 2,
      "name" : "event_date",
      "required" : false,
      "type" : "date"
    } ]
  } ],
  "version-log" : [ {
    "timestamp-ms" : 1573518431292,
    "version-id" : 1
  }, {
    "timestamp-ms" : 1573518981593,
    "version-id" : 2
  } ]
}
```

### Materialized View Example

Imagine the following operation, which creates a materialized view that precomputes daily event counts:

```sql
USE prod.default
```
```sql
CREATE MATERIALIZED VIEW event_agg_mv (
    event_count COMMENT 'Count of events',
    event_date)
COMMENT 'Precomputed daily event counts'
AS
SELECT
    COUNT(1), CAST(event_ts AS DATE)
FROM events
GROUP BY 2
```

The materialized view metadata JSON file looks as follows:

```
s3://bucket/warehouse/default.db/event_agg_mv/metadata/00001-(uuid).metadata.json
```
```json
{
  "view-uuid": "b2a12651-3038-4a72-8a31-5027ab84da35",
  "format-version" : 1,
  "location" : "s3://bucket/warehouse/default.db/event_agg_mv",
  "current-version-id" : 1,
  "properties" : {
    "comment" : "Precomputed daily event counts"
  },
  "versions" : [ {
    "version-id" : 1,
    "timestamp-ms" : 1573518431292,
    "schema-id" : 1,
    "default-catalog" : "prod",
    "default-namespace" : [ "default" ],
    "summary" : {
      "engine-name" : "Spark",
      "engine-version" : "3.4.1"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
      "dialect" : "spark"
    } ],
    "storage-table" : {
      "namespace" : [ "default" ],
      "name" : "event_agg_mv__storage"
    }
  } ],
  "schemas": [ {
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 1,
      "name" : "event_count",
      "required" : false,
      "type" : "int",
      "doc" : "Count of events"
    }, {
      "id" : 2,
      "name" : "event_date",
      "required" : false,
      "type" : "date"
    } ]
  } ],
  "version-log" : [ {
    "timestamp-ms" : 1573518431292,
    "version-id" : 1
  } ]
}
```

After a refresh operation, the storage table's snapshot summary contains the `refresh-state` property.
The following is an example of the `refresh-state` JSON value stored in the snapshot summary of the storage table:

```json
{
  "view-version-id" : 1,
  "refresh-start-timestamp-ms" : 1573518435000,
  "source-states" : [ {
    "type" : "table",
    "namespace" : [ "default" ],
    "name" : "events",
    "uuid" : "d4a10b5c-1e8a-4b72-9d67-3f4a8c9e1b2d",
    "snapshot-id" : 6148331192489823102
  } ]
}
```

## Appendix B: What counts as a dependency

The dependencies of a materialized view are determined by parsing the view query:

- **Base Iceberg tables** in the dependency graph are recorded by `snapshot-id`.
- **Iceberg views** in the dependency graph are recorded by `version-id`. A view's own dependencies are transitive dependencies of the materialized view and appear as additional entries in `source-states`.
- **Upstream materialized views** in the dependency graph are treated as their storage tables and recorded by the storage table's `snapshot-id`. Their own freshness is established recursively from their `refresh-state`.

### Example

The query under examination:

- `A` (the materialized view being refreshed): `SELECT ... FROM B JOIN C ON ...`
- `B` (regular view): `SELECT ... FROM E JOIN D ON ...`
- `C` (materialized view): `SELECT ... FROM F JOIN G ON ...`
- `D` (materialized view): `SELECT ... FROM H WHERE ...`
- `E`, `F`, `G`, `H`: base Iceberg tables

`A`'s direct dependencies are `B` and `C`. Because `B` is a regular view, its own dependencies (`E` and `D`) are transitively included in `A`'s `source-states`. `C` and `D` are upstream materialized views; they appear in `A`'s `source-states` as their storage tables.

```
A [MV — being refreshed]
├── B [VIEW]                            <-- recorded in A: version-id
│   ├── E [TABLE]                       <-- recorded in A: snapshot-id
│   └── D [MV]                          <-- recorded in A: storage-table snapshot-id
│       ┄┄┄┄┄┄ recursive boundary ┄┄┄┄┄┄
│       └── H [TABLE]                   (D's dependency; verified via D's refresh-state)
└── C [MV]                              <-- recorded in A: storage-table snapshot-id
    ┄┄┄┄┄┄ recursive boundary ┄┄┄┄┄┄
    ├── F [TABLE]                       (C's dependency; verified via C's refresh-state)
    └── G [TABLE]                       (C's dependency; verified via C's refresh-state)
```

`A`'s `source-states`:

| type    | name          | recorded id        |
|---------|---------------|--------------------|
| `view`  | `B`           | `version-id: 5`    |
| `table` | `E`           | `snapshot-id: 101` |
| `table` | `C` (storage) | `snapshot-id: 12`  |
| `table` | `D` (storage) | `snapshot-id: 14`  |

`F`, `G`, and `H` do not appear in `A`'s `source-states` directly; they belong to `C` and `D`'s dependency sets and are reached recursively through `C` and `D`'s refresh states.

A consumer establishes `A`'s freshness by checking each entry in `source-states` against the current catalog state. For `C` and `D`, the consumer compares the recorded storage-table snapshot to the current snapshot, then recurses into their `refresh-state` to verify each is itself fresh.
