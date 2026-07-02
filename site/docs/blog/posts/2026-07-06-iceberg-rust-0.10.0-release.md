---
date: 2026-07-06
title: Apache Iceberg Rust 0.10.0 Release
slug: apache-iceberg-rust-0.10.0-release
authors:
  - iceberg-pmc
categories:
  - release
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

The Apache Iceberg community is pleased to announce version 0.10.0 of [iceberg-rust](https://github.com/apache/iceberg-rust).

This release covers development work from early March through late June 2026 and is the result of merging **238 PRs** from **40 contributors**. See the [changelog] for the complete list of changes.

[changelog]: https://github.com/apache/iceberg-rust/blob/main/CHANGELOG.md#v0100---2026-06-29

`iceberg-rust` is a native Rust implementation of the Apache Iceberg specification, providing high-performance libraries for reading, writing, and managing Iceberg tables in Rust applications and through Python bindings (`pyiceberg-core`).

<!-- more -->

## Release Highlights

There have been plenty of contributions across the `iceberg-rust` library; here's a highlight of a few.

### Schema Evolution

This release adds the [`update_schema` action](https://github.com/apache/iceberg-rust/pull/2120), enabling column addition and removal.

### Snapshot Expiration

A new `ExpireSnapshotsAction` ([#2591](https://github.com/apache/iceberg-rust/pull/2591), [#2664](https://github.com/apache/iceberg-rust/pull/2664), [#2667](https://github.com/apache/iceberg-rust/pull/2667)) provides a high-level API for expiring old snapshots, respecting `history.expire.*` table properties and cleaning up associated statistics file metadata.

### Custom Runtime Support

The [`Runtime` trait](https://github.com/apache/iceberg-rust/pull/2308) can now be injected into catalogs, decoupling `iceberg-rust` from a hard tokio dependency at the catalog layer. This enables embedding in applications with custom async runtimes.

### Writer Improvements

- **Content-defined chunking** ([#2375](https://github.com/apache/iceberg-rust/pull/2375), [#2561](https://github.com/apache/iceberg-rust/pull/2561)): CDC table properties are now respected for optimizing Parquet file boundaries
- **[Purge table support](https://github.com/apache/iceberg-rust/pull/2232)**: Catalogs now offer a `purge_table` method, deleting both metadata and data files

### Enhanced DataFusion Integration

- **[EXPLAIN shows pushed-down limits](https://github.com/apache/iceberg-rust/pull/2360)**: `IcebergTableScan` now displays limit pushdown in EXPLAIN output for query debugging
- **[IsNaN predicate pushdown](https://github.com/apache/iceberg-rust/pull/2592)**: NaN-preserving numeric expressions are now pushed into scans

### Reader and Scan Improvements

- **[Scan I/O metrics](https://github.com/apache/iceberg-rust/pull/2349)**: `ArrowReader` now exposes bytes read during scans
- **[FixedBinary(N) support](https://github.com/apache/iceberg-rust/pull/2348)**: Added support for fixed-length binary types
- **[Snappy codec for Avro](https://github.com/apache/iceberg-rust/pull/2573)**: Avro files compressed with Snappy can now be read

### Table Encryption (Foundational)

Significant groundwork has been laid toward Iceberg's table encryption specification. Writes to encrypted tables are [explicitly blocked](https://github.com/apache/iceberg-rust/pull/2626) until the full write path is complete. End-to-end read and write support for encrypted tables is expected in a future release. See the [table encryption RFC](https://github.com/apache/iceberg-rust/pull/2183) for the full design.

### Storage Improvements

#### New Storage Backend - HuggingFace Hub

The [HuggingFace Hub storage backend](https://github.com/apache/iceberg-rust/pull/2375) was introduced, enabling direct access to Iceberg tables hosted on HuggingFace datasets.

#### Other improvements

- **[`delete_stream` on Storage trait](https://github.com/apache/iceberg-rust/pull/2216)**: Batch delete operations for efficient cleanup
- **[OpenDAL resolving storage](https://github.com/apache/iceberg-rust/pull/2231)**: Automatically resolves the appropriate storage backend based on file path scheme
- **[Timeout layer for OpenDAL](https://github.com/apache/iceberg-rust/pull/2455)**: Added `TimeoutLayer` inside `RetryLayer` to prevent indefinite hangs
- **[S3 virtual-host-style addressing](https://github.com/apache/iceberg-rust/pull/2330)**: Default to virtual-host-style S3 addressing for broader compatibility

## Bug Fixes

Notable correctness fixes or other validation improvements in this release:

- **[INT96 timestamp values](https://github.com/apache/iceberg-rust/pull/2301)**: Fixed incorrect Parquet INT96 timestamp interpretation
- **[Nested type column indices](https://github.com/apache/iceberg-rust/pull/2307)**: Fixed `build_fallback_field_id_map` producing incorrect indices for schemas with nested types
- **[Delete-only manifests in FastAppend](https://github.com/apache/iceberg-rust/pull/2545)**: Preserved delete-only manifests that were incorrectly dropped
- **[DeleteFileIndex lost wakeup](https://github.com/apache/iceberg-rust/pull/2696)**: Fixed a hang caused by a lost-wakeup race in `get_deletes_for_data_file`
- **[Row group byte range filtering](https://github.com/apache/iceberg-rust/pull/2615)**: Fixed row duplication for sub-row-group file splits
- **[Cross-engine decimal compatibility](https://github.com/apache/iceberg-rust/pull/2538)**: Added space in decimal type serialization for compatibility with other engines
- **[NaN pushdown correctness](https://github.com/apache/iceberg-rust/pull/2351)**: Fixed NaN pushdown to correctly handle NaN values
- **[Puffin magic number validation](https://github.com/apache/iceberg-rust/pull/2416)**: Added missing validation for magic number at the start of a Puffin file
- **[Glue catalog filtering](https://github.com/apache/iceberg-rust/pull/2570)**: Only list Iceberg tables, not all Glue tables
- **[Empty insert handling](https://github.com/apache/iceberg-rust/pull/2712)**: INSERT operations that produce no rows now correctly return a single row with count 0
- **[Name-mapped field IDs](https://github.com/apache/iceberg-rust/pull/2612)**: Projection and predicate pushdown now correctly use name-mapped field IDs for Parquet files without embedded field IDs
- **[Snapshot summary total fields](https://github.com/apache/iceberg-rust/pull/2589)**: Total values are now omitted if the previous summary had unparsable or no previous total.

## Breaking Changes

Here's a few notable breaking changes.
Given `iceberg-rust` has not yet reached version `1.x`, breaking changes are adopted for a better API long-term.

- **[Purge table](https://github.com/apache/iceberg-rust/pull/2232)**: Added `purge_table` to the `Catalog` trait; some catalog implementations moved their data-deletion logic from `drop_table` to `purge_table`
- **[Compression codec enum](https://github.com/apache/iceberg-rust/pull/2288)**: Codec enum variant changes introducing Snappy, and including compression levels in Zstd and Gzip.
- **[Custom Runtime in Catalog](https://github.com/apache/iceberg-rust/pull/2308)**: Added `with_runtime` to the `CatalogBuilder` trait
- **[DefaultLocationGenerator](https://github.com/apache/iceberg-rust/pull/2604)**: `new()` now borrows `TableMetadata` instead of taking ownership
- **[MSRV](https://github.com/apache/iceberg-rust/pull/2652)**: Bumped to Rust 1.94

## Dependency Updates

This release included a number of dependency updates, including upgrading to Apache DataFusion 53 and Apache Arrow 58.
For a full list of dependency versions, the best resources to review are the Cargo manifests.

## Infrastructure Improvements

- **Public API stability**: [cargo-public-api checks](https://github.com/apache/iceberg-rust/pull/2525) on every PR to catch unintentional API breakage
- **Trusted publishing**: [Migrated to trusted publishing](https://github.com/apache/iceberg-rust/pull/2593) for crates.io releases
- **Standalone compilation**: [CI verifies each crate compiles independently](https://github.com/apache/iceberg-rust/pull/2389)
- **Security**: Pinned all third-party actions to Apache-approved SHAs; ASF allowlist check on every PR
- **Performance**: [Disabled debuginfo and incremental compilation](https://github.com/apache/iceberg-rust/pull/2672) in CI; [Rust caches saved only from main](https://github.com/apache/iceberg-rust/pull/2669)

## What's Next

Work is already underway on several features for upcoming releases:

- **Table encryption**: With the foundational encryption stack landed, the community is working toward end-to-end encrypted reads and writes
- **Additional commit types**: Support for `RewriteDataFiles`, `RowDelta`, and `Overwrite` commit operations
- **Extensibility**: Continued investment in abstractions for pluggable storage backends and runtime configuration

## Getting Involved

The `iceberg-rust` project welcomes contributions. We use GitHub [issues](https://github.com/apache/iceberg-rust/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try `iceberg-rust` with your workloads and report any issues you encounter
2. Review the [contributor guide](https://github.com/apache/iceberg-rust/blob/main/CONTRIBUTING.md)
3. Look for [good first issues](https://github.com/apache/iceberg-rust/contribute)

Code review is also a very welcome contribution - please provide feedback on pull requests where you feel comfortable to do so!

For more information, visit the [iceberg-rust repository](https://github.com/apache/iceberg-rust).
