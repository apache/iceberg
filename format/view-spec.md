---
title: "View Spec"
url: view-spec
toc: true
disableSidebar: true
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

## Specification

### Terms

* **Schema** -- Names and types of fields in a view.
* **Version** -- The state of a view at some point in time.

### View Metadata

The view version metadata file has the following fields:

| Requirement | Field name           | Description |
|-------------|----------------------|-------------|
| _required_  | `format-version`     | An integer version number for the view format; must be 1 |
| _required_  | `location`           | The view's base location; used to create metadata file locations |
| _required_  | `current-schema-id`  | ID of the current schema of the view, if known |
| _required_  | `schemas`            | A list of known schemas |
| _required_  | `current-version-id` | ID of the current version of the view (`version-id`) |
| _required_  | `versions`           | A list of known [versions](#versions) of the view [1] |
| _required_  | `version-log`        | A list of [version log](#version-log) entries with the timestamp and `version-id` for every change to `current-version-id` |
| _optional_  | `properties`         | A string to string map of view properties [2] |

Notes:
1. The number of versions to retain is controlled by the table property: `version.history.num-entries`.
2. Properties are used for metadata such as `comment` and for settings that affect view maintenance. This is not intended to be used for arbitrary metadata.

#### Versions

Each version in `versions` is a struct with the following fields:

| Requirement | Field name        | Description                                                              |
|-------------|-------------------|--------------------------------------------------------------------------|
| _required_  | `version-id`      | ID for the version                                                       |
| _required_  | `schema-id`       | ID of the schema for the view version                                    |
| _required_  | `timestamp-ms`    | Timestamp when the version was created (ms from epoch)                   |
| _required_  | `summary`         | A string to string map of [summary metadata](#summary) about the version |
| _required_  | `representations` | A list of [representations](#representations) for the view definition    |

#### Summary

Summary is a string to string map of metadata about a view version. Common metadata keys are documented here.

| Requirement | Key              | Value |
|-------------|------------------|-------|
| _required_  | `operation`      | Operation that caused this metadata to be created; must be `create` or `replace` |
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
| _optional_  | `default-catalog`   | `string`       | Catalog name to use when a reference in the SELECT does not contain a catalog |
| _optional_  | `default-namespace` | `list<string>` | Namespace to use when a reference in the SELECT is a single identifier |
| _optional_  | `field-aliases`     | `list<string>` | Column names optionally specified in the create statement |
| _optional_  | `field-comments`    | `list<string>` | Column descriptions (COMMENT) optionally specified in the create statement |

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
| `default-catalog`   | `"prod"` |
| `default-namespace` | `["default"]` |
| `field-aliases`     | `["event_count", "event_date"]` |
| `field-comments`    | `["Count of events", null]` |

If a create statement does not include column names or comments before `AS`, the fields should be omitted.

#### Version log

The version log tracks changes to the view's current version. This is the view's history and allows reconstructing what version of the view would have been used at some point in time.

Note that this is not the version's creation time, which is stored in each version's metadata. A version can appear multiple times in the version log, indicating that the view definition was rolled back.

Each entry in `version-log` is a struct with the following fields:

| Requirement | Field name     | Description |
|-------------|----------------|-------------|
| _required_  | `timestamp-ms` | Timestamp when the view's `current-version-id` was updated (ms from epoch) |
| _required_  | `version-id`   | ID that `current-version-id` was set to |

## Appendix A: An Example

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
```
{
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
    "summary" : {
      "operation" : "create",
      "engine-name" : "Spark",
      "engineVersion" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
      "dialect" : "spark",
      "default-catalog" : "prod",
      "default-namespace" : [ "default" ],
      "field-aliases" : ["event_count", "event_date"],
      "field-comments" : ["Count of events", null]
    } ]
  } ],
  "current-schema-id": 1,
  "schemas": [ {
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 1,
      "name" : "col1",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "col2",
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

```sql
USE prod.other_db;
CREATE OR REPLACE VIEW default.event_agg (
    event_count,
    event_date)
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
```
{
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
    "summary" : {
      "operation" : "create",
      "engine-name" : "Spark",
      "engineVersion" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2",
      "dialect" : "spark",
      "default-catalog" : "prod",
      "default-namespace" : [ "default" ],
      "field-aliases" : ["event_count", "event_date"],
      "field-comments" : ["Count of events", null]
    } ]
  }, {
    "version-id" : 2,
    "timestamp-ms" : 1573518981593,
    "summary" : {
      "operation" : "create",
      "engine-name" : "Spark",
      "engineVersion" : "3.3.2"
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM prod.default.events\nGROUP BY 2",
      "dialect" : "spark",
      "default-catalog" : "prod",
      "default-namespace" : [ "default" ],
      "field-aliases" : ["event_count", "event_date"]
    } ]
  } ],
  "current-schema-id": 1,
  "schemas": [ {
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 1,
      "name" : "col1",
      "required" : false,
      "type" : "int"
    }, {
      "id" : 2,
      "name" : "col2",
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
