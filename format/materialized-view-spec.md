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

# Iceberg Materialized View Spec

## Background and Motivation
Iceberg views are a powerful tool to abstract complex queries and share them among different engines.
However, such views are not materialized by default, which means that they are re-computed every time they are queried.
This can be inefficient for complex queries that are computed frequently.
Iceberg Materialized views are a way to store the results of an Iceberg view to reuse the computation in subsequent queries.

## Goals 
The goal of this spec is to define the metadata associated with materialized views in Iceberg.
Such metadata allows creating and querying Iceberg materialized views across different engines.

## Specification
A materialized view is an Iceberg view with a respective Iceberg table that stores the results of the view query.
An Iceberg view is considered a materialized view if it has the `iceberg.materialized.view` property set to `true`.
A materialized view must also reference the storage table identifier in its `iceberg.materialized.view.storage.table` property.

The specification for the materialized view properties on the view is as follows:
| Property name                              | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `iceberg.materialized.view`                | This property is used to mark whether a view is a materialized view. If set to `true`, the view is treated as a materialized view.|
| `iceberg.materialized.view.storage.table`  | This property specifies the identifier of the storage table associated with the materialized view.|

In addition to the properties on the view, the storage table associated with the materialized view has the following properties:

| Property name                        | Description                                                                   |
|--------------------------------------|-------------------------------------------------------------------------------|
| `iceberg.base.snapshot.[UUID]`       | These properties store the snapshot IDs of the base tables at the time the materialized view's data was last updated. Each property is prefixed with `iceberg.base.snapshot.` followed by the UUID of the base table.|
| `iceberg.view.version`               | This property stores the version of the view that this storage table is associated with at the time of materialization|
| `iceberg.child.view.version.[UUID]`  | These properties store the version of the child views that the top level view is associated with at the time of materialization. Each property is prefixed with `iceberg.child.view.version.` followed by the UUID of the child view.|

A storage table is considered fresh if all the following conditions are true:
* For each table `T` with `UUID` in `iceberg.base.snapshot.[UUID]`, the current snapshot ID of `T` is equal to the value stored in `iceberg.base.snapshot.[UUID]`.
* The version of the view using the table as the storage table is equal to the value stored in `iceberg.view.version`.
* For each child view `V` with `UUID` in `iceberg.child.view.version.[UUID]`, the version of `V` is equal to the value stored in `iceberg.child.view.version.[UUID]`.

Implementations may elect to leverage the storage table with more relaxed freshness conditions, such as allowing base table stored snapshots to be different from current snapshots, as long as the respective snapshot timestamp difference is within a given time range.

