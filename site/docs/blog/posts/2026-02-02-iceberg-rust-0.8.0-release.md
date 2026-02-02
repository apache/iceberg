---
date: 2026-02-02
title: Apache Iceberg Rust 0.8.0 Release
slug: apache-iceberg-rust-0.8.0-release  # this is the blog url
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

The Apache Iceberg community is pleased to announce version 0.8.0 of [iceberg-rust](https://github.com/apache/iceberg-rust).

This release covers development work from late November 2025 through early January 2026 and is the result of merging **144 PRs** from **37 contributors**. See the [changelog] for the complete list of changes.

[changelog]: https://github.com/apache/iceberg-rust/blob/main/CHANGELOG.md#v080---2026-01-06

`iceberg-rust` is a native Rust implementation of the Apache Iceberg specification, providing high-performance libraries for reading, writing, and managing Iceberg tables in Rust applications and through Python bindings (`pyiceberg-core`).

<!-- more -->

## Release Highlights

### V3 Metadata Support

This release introduces [support for Iceberg V3 metadata format](https://github.com/apache/iceberg-rust/pull/1682), enabling `iceberg-rust` to work with the latest version of the Iceberg table specification. This includes support for V3 manifests with delete file content and enhanced metadata handling.

### Enhanced DataFusion Integration

The DataFusion integration received significant improvements:

- **INSERT INTO partitioned tables**: [Native support](https://github.com/apache/iceberg-rust/pull/1827) for inserting data into partitioned Iceberg tables
- **Partition column projection**: [Implemented project node](https://github.com/apache/iceberg-rust/pull/1602) to efficiently add partition columns during query execution
- **Partitioning node**: [Added partitioning operator](https://github.com/apache/iceberg-rust/pull/1620) to define output partitioning for better parallelism
- **Partition-aware sorting**: [New `sort_by_partition` operator](https://github.com/apache/iceberg-rust/pull/1618) to sort input data by partition values
- **SQLLogicTest integration**: [Added comprehensive testing framework](https://github.com/apache/iceberg-rust/pull/1764) for DataFusion operations
- **TaskWriter**: [Implemented writer interface](https://github.com/apache/iceberg-rust/pull/1769) for DataFusion execution

### Advanced Delete File Handling

`iceberg-rust` now has substantially improved support for Iceberg's delete files:

- **Shared delete file caching**: [Optimized performance](https://github.com/apache/iceberg-rust/pull/1941) by caching and sharing delete files across multiple readers
- **Mixed delete types**: [Support for both position and equality deletes](https://github.com/apache/iceberg-rust/pull/1778) on the same FileScanTask
- **Partial schema deletes**: [Handle equality delete files](https://github.com/apache/iceberg-rust/pull/1782) containing only equality columns
- **Binary type support**: [Added binary type support](https://github.com/apache/iceberg-rust/pull/1848) in equality delete processing
- **Performance fixes**: Fixed [stack overflow with large deletes](https://github.com/apache/iceberg-rust/pull/1915) and [scan deadlocks](https://github.com/apache/iceberg-rust/pull/1937)
- **Row group filtering**: [Improved filtering](https://github.com/apache/iceberg-rust/pull/1779) when FileScanTask contains byte ranges
- **Case-sensitive deletes**: [Added support](https://github.com/apache/iceberg-rust/pull/1930) for case-sensitive equality delete matching

### Enhanced Reader Capabilities

The Arrow reader received several important enhancements:

- **Position-based projection**: [Support for Parquet files without field IDs](https://github.com/apache/iceberg-rust/pull/1777), enabling reads from migrated tables
- **PartitionSpec support**: [Added partition awareness](https://github.com/apache/iceberg-rust/pull/1821) to FileScanTask and RecordBatchTransformer
- **Schema evolution**: [Fixed handling](https://github.com/apache/iceberg-rust/pull/1750) of Parquet files with schema changes
- **Compressed metadata**: [Support for reading](https://github.com/apache/iceberg-rust/pull/1802) compressed table metadata
- **Date32 support**: Improved handling of [Date32 values](https://github.com/apache/iceberg-rust/pull/1792) and [struct defaults](https://github.com/apache/iceberg-rust/pull/1847)
- **Binary deserialization**: [Added support](https://github.com/apache/iceberg-rust/pull/1820) for deserializing bytes values
- **`_file` metadata column**: [Support for Iceberg's `_file` column](https://github.com/apache/iceberg-rust/pull/1824) to expose source file information

### Advanced Writers

New writer implementations provide flexible approaches to data organization:

- **Clustered and Fanout writers**: [Implemented writers](https://github.com/apache/iceberg-rust/pull/1735) for different data distribution strategies
- **Configurable FanoutWriter**: [Made writer configuration](https://github.com/apache/iceberg-rust/pull/1962) more flexible
- **Non-consuming builders**: [Refactored writer builders](https://github.com/apache/iceberg-rust/pull/1889) to not consume self, allowing reuse

### Catalog Improvements

Multiple catalog implementations received updates:

- **SqlCatalog**: Added [update_table support](https://github.com/apache/iceberg-rust/pull/1911), [register_table](https://github.com/apache/iceberg-rust/pull/1724), and a [builder pattern](https://github.com/apache/iceberg-rust/pull/1666)
- **S3TablesCatalog**: [Implemented update_table](https://github.com/apache/iceberg-rust/pull/1594) for AWS S3 Tables
- **Glue**: [Improved concurrency error handling](https://github.com/apache/iceberg-rust/pull/1875) for Glue catalog
- **MemoryCatalog**: [Fixed namespace handling](https://github.com/apache/iceberg-rust/pull/1970) to return absolute NamespaceIdents
- **REST catalog**: [Improved authentication](https://github.com/apache/iceberg-rust/pull/1712) and [made types public](https://github.com/apache/iceberg-rust/pull/1901) with documentation

### Schema Conversion

[New capability](https://github.com/apache/iceberg-rust/pull/1928) to convert Arrow schemas to Iceberg schemas with auto-assigned field IDs, simplifying integration with Arrow-based systems.

### PyIceberg-core Improvements

The Python bindings received multiple enhancements:

- **Smaller artifacts**: Multiple optimizations ([#1841](https://github.com/apache/iceberg-rust/pull/1841), [#1844](https://github.com/apache/iceberg-rust/pull/1844)) reduced package size
- **ABI3 support**: [Using pyo3 abi3-py310](https://github.com/apache/iceberg-rust/pull/1843) for better Python version compatibility
- **RecordBatchTransformerBuilder**: [Updated API](https://github.com/apache/iceberg-rust/pull/1857) for more ergonomic usage

## Breaking Changes

This release includes several breaking changes:

- **Runtime**: [Dropped smol runtime support](https://github.com/apache/iceberg-rust/pull/1900) in favor of tokio
- **MSRV**: [Bumped to Rust 1.88](https://github.com/apache/iceberg-rust/pull/1902)
- **API cleanup**: [Removed deprecated `remove_all`](https://github.com/apache/iceberg-rust/pull/1863) from FileIO
- **SnapshotProducer**: [Simplified validation methods](https://github.com/apache/iceberg-rust/pull/1853) by removing redundant parameters
- **Enums**: [Removed wildcard patterns](https://github.com/apache/iceberg-rust/pull/1925) from exhaustive enums

## Dependency Updates

- [Upgraded to DataFusion 51](https://github.com/apache/iceberg-rust/pull/1899) from DataFusion 48
- [Upgraded to Arrow 57](https://github.com/apache/iceberg-rust/pull/1899) from Arrow 53
- [Upgraded to apache-avro 0.21.0](https://github.com/apache/iceberg-rust/pull/1881)
- [Upgraded to OpenDAL v0.55](https://github.com/apache/iceberg-rust/pull/1895)
- [Migrated from tera to minijinja](https://github.com/apache/iceberg-rust/pull/1798) for template rendering

## Infrastructure Improvements

- **Parallel testing**: [CI now runs unit tests](https://github.com/apache/iceberg-rust/pull/1833) in parallel using a matrix strategy
- **Python tooling**: [Migrated to uv](https://github.com/apache/iceberg-rust/pull/1796) for faster Python dependency management
- **License checking**: [Configured to ignore target/](https://github.com/apache/iceberg-rust/pull/1954) directory
- **GitHub workflow**: [Added failure notifications](https://github.com/apache/iceberg-rust/pull/1870)

## Getting Involved

The `iceberg-rust` project welcomes contributions. We use GitHub [issues](https://github.com/apache/iceberg-rust/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try `iceberg-rust` with your workloads and report any issues you encounter
2. Review the [contributor guide](https://github.com/apache/iceberg-rust/blob/main/CONTRIBUTING.md)
3. Look for [good first issues](https://github.com/apache/iceberg-rust/contribute)

For more information, visit the [iceberg-rust repository](https://github.com/apache/iceberg-rust).
