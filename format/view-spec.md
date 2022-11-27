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
Each metadata file is self-sufficient. It contains the history of the last few operations performed on the view and can be used to roll back the view to a previous version.

### Metadata Location

An atomic swap of one view metadata file for another provides the basis for making atomic changes. Readers use the version of the view that was current when they loaded the view metadata and are not affected by changes until they refresh and pick up a new metadata location.

Writers create view metadata files optimistically, assuming that the current metadata location will not be changed before the writer’s commit. Once a writer has created an update, it commits by swapping the view's metadata file pointer from the base location to the new location.

## Specification

### Terms

* **Schema** -- Names and types of fields in a view.
* **Version** -- The state of a view at some point in time.

### View Metadata

The view version metadata file has the following fields:

| Required/Optional | Field Name | Description |
|-------------------|------------|-------------|
| Required | format-version | An integer version number for the view format. Currently, this must be 1. Implementations must throw an exception if the view's version is higher than the supported version. |
| Required | location | The view's base location. This is used to determine where to store view metadata files. |
| Required | current-version-id | Current version of the view. Set to ‘1’ when the view is first created. |
| Optional | properties | A string to string map of view properties. This is used for metadata such as "comment" and for settings that affect view maintenance. This is not intended to be used for arbitrary metadata. |
| Required | versions | An array of structs describing the known versions of the view. The number of versions to retain is controlled by the table property: “version.history.num-entries”. See section [Versions](#versions). |
| Required | version-log | A list of timestamp and version ID pairs that encodes changes to the current version for the view. Each time the current-version-id is changed, a new entry should be added with the last-updated-ms and the new current-version-id. |
| Optional | schemas | A list of schemas, the same as the ‘schemas’ field from Iceberg table spec. |
| Optional | current-schema-id | ID of the current schema of the view |

#### Versions

Field "versions" is an array of structs with the following fields:

| Required/Optional | Field Name | Description |
|-------------------|------------|-------------|
| Required | version-id | Monotonically increasing id indicating the version of the view. Starts with 1. |
| Required | timestamp-ms | Timestamp expressed in ms since epoch at which the version of the view was created. |
| Required | summary | A string map summarizes the version changes, including `operation`, described in [Summary](#summary). |
| Required | representations | A list of "representations" as described in [Representations](#representations). |

#### Version Log

Field “version-log” is an array of structs that describe when each version was considered "current". Creation time is different and is stored in each version's metadata. This allows you to reconstruct what someone would have seen at some point in time. If the view has been updated and rolled back, this will show it. The struct has the following fields:

| Required/Optional | Field Name | Description |
|-------------------|------------|-------------|
| Required | timestamp-ms | The timestamp when the referenced version was made the current version |
| Required | version-id | Version id of the view  |

#### Summary

Field “summary” is a string map with the following keys. Only `operation` is required. Engines may store additional key-value pairs in this map.

| Required/Optional | Key | Value |
|-------------------|-----|-------|
| Required | operation | A string value indicating the view operation that caused this metadata to be created. Allowed values are “create” and “replace”. |
| Optional | engine-version | A string value indicating the version of the engine that performed the operation |

#### Representations

Each representation is stored as an object with only one common field "type".
The rest of the fields are interpreted based on the type.

##### Original View Definition in SQL

This type of representation stores the original view definition in SQL and its SQL dialect.

| Required/Optional | Field Name | Description |
|-------------------|------------|-------------|
| Required | type | A string indicating the type of representation. It is set to "sql" for this type. |
| Required | sql | A string representing the original view definition in SQL |
| Required | dialect | A string specifying the dialect of the ‘sql’ field. It can be used by the engines to detect the SQL dialect. |
| Optional | schema-id | ID of the view's schema when the version was created |
| Optional | default-catalog | A string specifying the catalog to use when the table or view references in the view definition do not contain an explicit catalog. |
| Optional | default-namespace | The namespace to use when the table or view references in the view definition do not contain an explicit namespace. Since the namespace may contain multiple parts, it is serialized as a list of strings. |
| Optional | field-aliases | A list of strings of field aliases optionally specified in the create view statement. The list should have the same length as the schema's top level fields. See the example below. |
| Optional | field-docs | A list of strings of field comments optionally specified in the create view statement. The list should have the same length as the schema's top level fields. See the example below. |

For `CREATE VIEW v (alias_name COMMENT 'docs', alias_name2, ...) AS SELECT col1, col2, ...`,
the field aliases are 'alias_name', 'alias_name2', and etc., and the field docs are 'docs', null, and etc.

## Appendix A: An Example

The JSON metadata file format is described using an example below.

Imagine the following sequence of operations:

* `CREATE TABLE base_tab(c1 int, c2 varchar);`
* `INSERT INTO base_tab VALUES (1,’one’), (2,’two’);`
* `CREATE VIEW common_view AS SELECT * FROM base_tab;`
* `CREATE OR REPLACE VIEW common_view AS SELECT count(*) AS my_cnt FROM base_tab;`

The metadata JSON file created at the end of step 3 looks as follows. The file path looks like:
`s3://my_company/my/warehouse/anorwood.db/common_view`

The path is intentionally similar to the path for iceberg tables and contains a ‘metadata’ directory. (`METASTORE_WAREHOUSE_DIR/<dbname>.db/<viewname>/metadata`)

The metadata directory contains View Version Metadata files. The text after '=>' symbols describes the fields.
```
{
  "format-version" : 1, => JSON format. Will change as format evolves.
  "location" : "s3n://my_company/my/warehouse/anorwood.db/common_view",
  "current-version-id" : 1, => current / latest version of the view. ‘1’ here since this metadata was created when the view was created.
  "properties" : {  => shows properties of the view
    "comment" : "View captures all the data from the table" => View comment
  },
  "versions" : [ { => Last few versions of the view.
    "version-id" : 1,
    "parent-version-id" : -1,
    "timestamp-ms" : 1573518431292,
    "summary" : {
      "operation" : "create", => View operation that caused this metadata to be created
      "engineVersion" : "presto-350", => Version of the engine that performed the operation (create / replace)
    },
    "representations" : [ { => SQL metadata of the view
      "type" : "sql",
      "sql" : "SELECT *\nFROM\n  base_tab\n", => original view SQL
      "dialect" : "presto",
      "schema-id" : 1,
      "default-catalog" : "iceberg",
      "default-namespace" : [ "anorwood" ]
    } ],
  } ],
  "version-log" : [ { => Log of the created versions
    "timestamp-ms" : 1573518431292,
    "version-id" : 1
  } ],
  "schemas": [ { => Schema of the view expressed in Iceberg types
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 0,
      "name" : "c1",
      "required" : false,
      "type" : "int",
      "doc" : "" => Column comment
    }, {
      "id" : 1,
      "name" : "c2",
      "required" : false,
      "type" : "string",
      "doc" : ""
    } ]
  } ],
  "current-schema-id": 1
}
```

The Iceberg / view library creates a new metadata JSON file every time the view undergoes a DDL change. This way the history of how the view evolved can be maintained. Following metadata JSON file was created at the end of Step 4.

```
{
  "format-version" : 1,
  "location" : "s3n://my_company/my/warehouse/anorwood.db/common_view",
  "current-version-id" : 2,
  "properties" : {  => shows properties of the view
    "comment" : "View captures count of the data from the table"
  },
  "versions" : [ {
    "version-id" : 1,
    "parent-version-id" : -1,
    "timestamp-ms" : 1573518431292,
    "summary" : {
      "operation" : "create",
      "engineVersion" : "presto-350",
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT *\nFROM\n  base_tab\n",
      "dialect" : "presto",
      "schema-id" : 1,
      "default-catalog" : "iceberg",
      "default-namespace" : [ "anorwood" ]
    } ],
    "properties" : { }
  }, {
    "version-id" : 2,
    "parent-version-id" : 1, => Version 2 was created on top of version 1, making parent-version-id 1
    "timestamp-ms" : 1573518440265,
    "summary" : {
      "operation" : "replace", => The ‘replace’ operation caused this latest version creation
      "engineVersion" : "spark-2.4.4",
    },
    "representations" : [ {
      "type" : "sql",
      "sql" : "SELECT \"count\"(*) my_cnt\nFROM\n  base_tab\n", => Note the updated text from the ‘replace’ view statement
      "dialect" : "spark",
      "schema-id" : 2,
      "default-catalog" : "iceberg",
      "default-namespace" : [ "anorwood" ]
    },
  } ],
  "version-log" : [ {
    "timestamp-ms" : 1573518431292,
    "version-id" : 1
  }, {
    "timestamp-ms" : 1573518440265,
    "version-id" : 2
  } ],
  "schemas": [ { => Schema of the view expressed in Iceberg types
    "schema-id": 1,
    "type" : "struct",
    "fields" : [ {
      "id" : 0,
      "name" : "c1",
      "required" : false,
      "type" : "int",
      "doc" : "" => Column comment
    }, {
      "id" : 1,
      "name" : "c2",
      "required" : false,
      "type" : "string",
      "doc" : ""
    } ]
  }, { => Schema change is reflected here
    "schema-id": 2,
    "type" : "struct",
    "fields" : [ {
      "id" : 0,
      "name" : "my_cnt",
      "required" : false,
      "type" : "long",
      "doc" : ""
    } ]
  } ],
  "current-schema-id": 2
}
```
