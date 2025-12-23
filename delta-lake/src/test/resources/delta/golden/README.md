# Delta Lake Golden Tables

## Overview
This directory contains the **Delta Lake Golden Tables**, a set of reference data tables used to verify compliance with the Delta Lake protocol.

These tables cover various features of the Delta Lake specification (e.g., different protocol versions, partition strategies, checkpoints, and data types). They act as a "compliance suite" to ensure that this project's reader/writer implementation can correctly handle data produced by the official Delta Lake reference implementation.

## Origin & Attribution
These resources were sourced directly from the open-source Delta Lake project.

* **Source Repository:** [delta-io/delta](https://github.com/delta-io/delta)
* **Module Path:** `connectors/golden-tables/src`
* **License:** Apache License 2.0 (See [LICENSE](https://github.com/delta-io/delta/blob/master/LICENSE.txt) in the project)

## Purpose
The primary goal of these tables is **integration testing**.

Each folder represents a specific "edge case" or feature configuration in Delta Lake. By running the conversion against
these tables, we verify that:
1.  **Protocol Versioning:** We respect `minReaderVersion` and `minWriterVersion`.
2.  **Feature Support:** We can read tables with `checkpoints`, `vacuumed` files, `deletion vectors`, or `column mapping`.
3.  **Data Consistency:** We read the correct number of rows and column values as defined by the transaction log (`_delta_log`).

## Table Inventory & Explanations

The tables are typically named after the specific feature they test. Below are explanations of common patterns found in this suite:

### 1. Basic Protocol Tests
* **`primitives` / `all-types`**: Tables containing every supported primitive data type (INT, LONG, STRING, TIMESTAMP, etc.) to ensure type mapping is correct.

### 2. Transaction Log Features
* **`checkpoint`**: Tables that have been checkpointed (compaction of the JSON log into Parquet). Tests if the reader can reconstruct state from a checkpoint file + subsequent JSON deltas.
* **`vacuum`**: Tables where old data files have been physically deleted. Tests if the reader correctly ignores files no longer referenced in the log.
* **`streaming`**: Tables created via streaming sinks, often containing specific metadata relating to streaming query progress.

### 3. Partitioning
* **`partitioned`**: Tables partitioned by one or more columns (e.g., `date`, `region`). Tests if the reader correctly prunes partitions and reconstructs the data.
* **`partitioned-with-null`**: Partitioned tables that include `null` values in the partition keys (a common edge case for directory structures).

### 4. Advanced Features
* **`delete` / `merge` / `update`**: Tables where data has been modified. Tests if the reader correctly applies the latest version of the data (resolving tombstones/deletions).
* **`change-data-feed`**: Tables with Change Data Feed (CDF) enabled.
* **`deletion-vectors`**: Tables using Deletion Vectors (a newer protocol feature for efficient deletes). *Note: Ensure our reader supports Protocol V3 if using these.*
* **`column-mapping`**: Tables using ID-based or Name-based column mapping (allows renaming columns without rewriting data files).

## Updating
To update these tables, copy the latest resources from the `master` branch of the [delta-io/delta](https://github.com/delta-io/delta) repository under `connectors/golden-tables/src/test/resources/golden`.

---
*Note: This content is for testing purposes only and is derived from the Delta Lake open-source project.*