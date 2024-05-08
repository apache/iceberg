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
An Iceberg view is considered a materialized view if it has the `materialized.view` property set to `true`.
A materialized view must also reference the storage table identifier in its `materialized.view.storage.table` property.

The specification for the materialized view properties on the view is as follows:
| Property name                              | Description                                                             |
|--------------------------------------------|-------------------------------------------------------------------------|
| `materialized.view`                | This property is used to mark whether a view is a materialized view. If set to `true`, the view is treated as a materialized view.|
| `materialized.view.storage.table`  | This property specifies the identifier of the storage table associated with the materialized view.|

In addition to the properties on the view, the storage table associated with the materialized view has the following properties:

| Property name                        | Description                                                                   |
|--------------------------------------|-------------------------------------------------------------------------------|
| `base.snapshot.[UUID]`       | These properties store the snapshot IDs of the base tables at the time the materialized view's data was last updated. Each property is prefixed with `base.snapshot.` followed by the UUID of the base table.|
| `view.version`               | This property stores the version of the view that this storage table is associated with at the time of materialization|
| `child.view.version.[UUID]`  | If the top level materialized view is nested on top of other views, these properties store the version of the child views that the top level view is associated with at the time of materialization. Each property is prefixed with `child.view.version.` followed by the UUID of the child view.|

A storage table is considered fresh if all the following conditions are true:
* For each table `T` with `UUID` in `base.snapshot.[UUID]`, the current snapshot ID of `T` is equal to the value stored in `base.snapshot.[UUID]`.
* The version of the view using the table as the storage table is equal to the value stored in `view.version`.
* For each child view `V` with `UUID` in `child.view.version.[UUID]`, the version of `V` is equal to the value stored in `child.view.version.[UUID]`.

Implementations may elect to leverage the storage table with more relaxed freshness conditions, such as allowing base table stored snapshots to be different from current snapshots, as long as the respective snapshot timestamp difference is within a given time range.

## Examples
The examples below illustrate the role of different metadata associated with the storage table in determining the freshness of the materialized view. 

### Example 1:
This example highlights the role of the snapshot ID in determining the freshness of a materialized view.

**Scenario:**
- **View Name:** `event_summary`
- **Base Table:** `event`
- **Base Table UUID:** `123e4567`
- **View Definition:** `SELECT event_type, COUNT(*) AS total_events FROM event GROUP BY event_type`

**Example States:**
1. **Initial State:**
  - The materialized view `event_summary` is created using the current snapshot of the `event` table.
  - Property `base.snapshot.123e4567` is set with the snapshot ID at the time of creation, say `123`.

2. **Fresh State:**
  - No updates have occurred in the `event` table, so the current snapshot ID, `123` matches the value stored in `base.snapshot.123e4567`.

3. **Stale State:**
  - The `event` table receives an update, changing the current snapshot ID to `456`. No other changes have taken place.
  - When `event_summary` is queried again, the snapshot ID in `base.snapshot.123e4567` (i.e., `123`) no longer matches the current snapshot ID of the `event` table (i.e., `456`).
  - The view is now considered stale due to the snapshot ID mismatch.

### Example 2:
This example highlights the role of the view version in determining the freshness of a materialized view.

**Scenario:**
- **View Name:** `monthly_event_report`
- **Base Table:** `event`
- **View Definition:** `SELECT MONTH(event_date) AS month, COUNT(*) AS count FROM event GROUP BY MONTH(event_date)`

**Example states:**
1. **Initial State:**
  - `monthly_event_report` is created using the current data in the `event` table.
  - The property `view.version` is set with the view version at the time of creation, say `1`.

2. **Fresh State:**
  - The view `monthly_event_report` version has not changed, so the current view version, `1`, matches the value stored in `view.version`.
  - The storage table is considered fresh.

3. **Stale State:**
  - The definition of `monthly_event_report` is updated (e.g., adding more grouping conditions or changing the aggregation). No other changes have taken place.
  - This change updates the view version, say to `2`.
  - When `monthly_event_report` is queried again, the version recorded in `view.version`, i.e., `1`, does not match the current version of the view, i.e., `2`.
  - The view is now considered stale due to the top level view version mismatch, i.e., the storage table does not contain a valid result for the current view version.

### Example 3:
This example highlights the role of the view version in determining the freshness of a materialized view.

**Scenario:**
- **View Name:** `event_analysis`
- **Base Tables:** `event1`, `event2`
- **Child Views:** `event_type_count` (based on `event1`), `event_region_count` (based on `event2`)
- **Child View UUIDs:** `event_type_count` - `456e7890`, `event_region_count` - `789e0123`
- **View Definitions:**
  - `event_analysis`: `SELECT E1.event_type, E1.count, E2.region, E2.count FROM EventTypeCount E1 JOIN EventRegionCount E2 ON E1.event_type = E2.event_type`
  - `event_type_count`: `SELECT event_type, COUNT(*) AS count FROM event1 GROUP BY event_type`
  - `event_region_count`: `SELECT region, COUNT(*) AS count FROM event2 GROUP BY region`

**Steps:**
1. **Initial State:**
  - `event_analysis` is created using data joined from `event_type_count` and `event_region_count`.
  - Properties for child view versions are set based on the current state.
  - `child.view.version.456e7890` is set with the version of `event_type_count`, say `1`.
  - `child.view.version.789e0123` is set with the version of `event_region_count`, say `1`.

2. **Fresh State:**
  - Child views have not undergone any version changes. The view versions stored in `child.view.version.456e7890` and `child.view.version.789e0123` are both still set to `1`, matching the current versions of the child views.
  - The storage table is considered fresh. 

3. **Stale State:**
  - `event_type_count` undergoes an update in its definition, changing its view version, say to `2`. No other changes have taken place.
  - Despite no changes to `event_region_count` or `event_analysis`, the mismatch in `event_type_count` view version, i.e., `2`,  versus the stored `child.view.version.456e7890-` renders the storage table for `event_analysis` stale.
  - The view is now considered stale due to one child view version mismatch.
