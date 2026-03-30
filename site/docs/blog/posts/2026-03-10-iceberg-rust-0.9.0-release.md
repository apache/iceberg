---
date: 2026-03-10
title: Apache Iceberg Rust 0.9.0 Release
slug: apache-iceberg-rust-0.9.0-release  # this is the blog url
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

The Apache Iceberg community is pleased to announce version 0.9.0 of [iceberg-rust](https://github.com/apache/iceberg-rust).

This release covers development work from early January through early March 2026 and is the result of merging **109 PRs** from **28 contributors**, including **8 new contributors**. See the [changelog] for the complete list of changes.

[changelog]: https://github.com/apache/iceberg-rust/blob/main/CHANGELOG.md#v090---2026-03-10

`iceberg-rust` is a native Rust implementation of the Apache Iceberg specification, providing high-performance libraries for reading, writing, and managing Iceberg tables in Rust applications and through Python bindings (`pyiceberg-core`).

<!-- more -->

## Release Highlights

### Trait-Based Storage Architecture

The most significant change in this release is a new trait-based storage architecture that decouples `iceberg-rust` from any single storage backend. The new [`Storage` trait](https://github.com/apache/iceberg-rust/pull/2061) provides a clean abstraction for file I/O, with [integration into FileIO](https://github.com/apache/iceberg-rust/pull/2116) to make it the primary extension point for adding new storage backends.

This release includes two native storage implementations built on this trait:

- **[LocalFsStorage](https://github.com/apache/iceberg-rust/pull/2094)**: A native local filesystem backend
- **[MemoryStorage](https://github.com/apache/iceberg-rust/pull/2097)**: An in-memory backend useful for testing

The existing OpenDAL-based storage was [moved to its own crate](https://github.com/apache/iceberg-rust/pull/2207) (`iceberg-storage-opendal`), keeping the core library lightweight while preserving full OpenDAL support as an optional dependency. See the [Storage trait RFC](https://github.com/apache/iceberg-rust/pull/1885) for the design rationale.

### Enhanced DataFusion Integration

DataFusion integration continues to expand with DDL and query optimization improvements:

- **CREATE TABLE**: [Native support](https://github.com/apache/iceberg-rust/pull/1972) for creating Iceberg tables through DataFusion SQL, with [schema validation for partition projection](https://github.com/apache/iceberg-rust/pull/2008)
- **DROP TABLE**: [Support for dropping tables](https://github.com/apache/iceberg-rust/pull/2033) through DataFusion SQL
- **LIMIT pushdown**: [Pushes row limits](https://github.com/apache/iceberg-rust/pull/2006) into Iceberg scans to avoid reading unnecessary data
- **Predicate pushdown**: Added support for [Boolean](https://github.com/apache/iceberg-rust/pull/2082), [IsNaN](https://github.com/apache/iceberg-rust/pull/2142), [Timestamp](https://github.com/apache/iceberg-rust/pull/2069), [Binary](https://github.com/apache/iceberg-rust/pull/2048), and [LIKE/StartsWith](https://github.com/apache/iceberg-rust/pull/2014) predicates
- **INSERT INTO**: [Support for inserting data](https://github.com/apache/iceberg-rust/pull/2005) into Iceberg tables through DataFusion SQL, with automatic sort-based clustering for partitioned writes

### Reader Performance Improvements

The Arrow reader received several performance optimizations as part of an [ongoing effort](https://github.com/apache/iceberg-rust/issues/2172) to reduce I/O overhead during reads:

- **Byte range coalescing**: [Implemented `get_byte_ranges`](https://github.com/apache/iceberg-rust/pull/2181) on `AsyncFileReader` to coalesce small reads into fewer I/O operations
- **Fast-path single-threaded reads**: [Optimized `ArrowReader::read`](https://github.com/apache/iceberg-rust/pull/2020) for single-concurrency scenarios by avoiding unnecessary async overhead
- **Metadata size hints**: [Added Parquet metadata size hint option](https://github.com/apache/iceberg-rust/pull/2173) to `ArrowReaderBuilder`, eliminating unnecessary `stat()` calls when the metadata size is known
- **File size propagation**: [Pass data file and delete file sizes](https://github.com/apache/iceberg-rust/pull/2175) to the reader to avoid redundant metadata lookups
- **Reduced builder overhead**: [Avoided redundant](https://github.com/apache/iceberg-rust/pull/2176) `create_parquet_record_batch_stream_builder()` calls
- **Timestamp support**: [Added timestamp type support](https://github.com/apache/iceberg-rust/pull/2180) in column creation

### 38-Digit Decimal Precision

The `rust_decimal` crate was [replaced with `fastnum`](https://github.com/apache/iceberg-rust/pull/2063), enabling full 38-digit decimal precision as required by the Iceberg specification. This also includes a [fix for decimal byte representation](https://github.com/apache/iceberg-rust/pull/1998) used in hashing.

## Bug Fixes

- **Table upgrades**: [Fixed v2 to v3 table upgrades](https://github.com/apache/iceberg-rust/pull/2010) that previously failed
- **Sort order validation**: [Reserved sort order IDs](https://github.com/apache/iceberg-rust/pull/1978) can no longer contain fields
- **Partition field IDs**: [Fixed reuse of partition field IDs](https://github.com/apache/iceberg-rust/pull/2011) in `AddSpec`
- **REST catalog security**: [Filtered sensitive headers](https://github.com/apache/iceberg-rust/pull/2117) from error logs to prevent credential leakage
- **SqlCatalog**: [Fixed `SqlCatalogBuilder`](https://github.com/apache/iceberg-rust/pull/2079) not persisting the supplied catalog name
- **S3Tables**: [Correctly interpret warehouse](https://github.com/apache/iceberg-rust/pull/2115) as `table_location` for S3Tables

## Breaking Changes

This release includes several breaking changes:

- **Storage architecture**: The [Storage trait](https://github.com/apache/iceberg-rust/pull/2116) is now integrated with FileIO, and [OpenDAL storage moved](https://github.com/apache/iceberg-rust/pull/2207) to `iceberg-storage-opendal`
- **MSRV**: [Bumped to Rust 1.92.0](https://github.com/apache/iceberg-rust/pull/2224)
- **Decimal**: [Replaced `rust_decimal` with `fastnum`](https://github.com/apache/iceberg-rust/pull/2063) for 38-digit precision support

## Dependency Updates

- [Upgraded to DataFusion 52.2](https://github.com/apache/iceberg-rust/pull/1997)
- Various updates to Arrow, Parquet, and other dependencies

## Infrastructure Improvements

- **Security scanning**: [Added CodeQL workflow](https://github.com/apache/iceberg-rust/pull/2151) for GitHub Actions security analysis
- **Workflow permissions**: [Added explicit least-privilege permissions](https://github.com/apache/iceberg-rust/pull/2163) to workflows
- **Faster tests**: [Adopted nextest](https://github.com/apache/iceberg-rust/pull/2078) for faster test execution with [parallel integration tests](https://github.com/apache/iceberg-rust/pull/2076)
- **Python tooling**: [Switched to uv](https://github.com/apache/iceberg-rust/pull/2129) for Python dependency management

## What's Next

Work is already underway on several features for upcoming releases:

- **Storage trait expansion**: With the new trait-based storage architecture landed in 0.9.0, the community is looking to [integrate additional storage backends](https://github.com/apache/iceberg-rust/issues/2208), such as `object_store`, making it easy for users to opt into alternative storage implementations.
- **Table encryption**: [AES-GCM encryption support](https://github.com/apache/iceberg-rust/pull/2026) is in progress, bringing Iceberg's table-level encryption spec to Rust with Java-compatible ciphertext formats. See the [table encryption RFC](https://github.com/apache/iceberg-rust/pull/2183) for the full design, including envelope encryption with a chained key hierarchy and KMS integration.

## Getting Involved

The `iceberg-rust` project welcomes contributions. We use GitHub [issues](https://github.com/apache/iceberg-rust/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try `iceberg-rust` with your workloads and report any issues you encounter
2. Review the [contributor guide](https://github.com/apache/iceberg-rust/blob/main/CONTRIBUTING.md)
3. Look for [good first issues](https://github.com/apache/iceberg-rust/contribute)

For more information, visit the [iceberg-rust repository](https://github.com/apache/iceberg-rust).
