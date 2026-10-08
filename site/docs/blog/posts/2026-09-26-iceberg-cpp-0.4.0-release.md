---
date: 2026-09-26
title: Apache Iceberg C++ 0.4.0 Release
slug: apache-iceberg-cpp-0.4.0-release
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

The Apache Iceberg community is pleased to announce the 0.4.0 release of Apache [Iceberg C++](https://github.com/apache/iceberg-cpp). This release includes [over 160 pull requests](https://github.com/apache/iceberg-cpp/compare/v0.3.0...v0.4.0) from 26 contributors, including 12 first-time contributors.

`iceberg-cpp` is a native C++ implementation of the Apache Iceberg table format, providing libraries for reading, writing, and managing Iceberg tables in C++ applications.

<!-- more -->

## Release Highlights

### Iceberg v3 Support
- [v3 type definitions](https://github.com/apache/iceberg-cpp/pull/752) and [v3 geometry and geography support](https://github.com/apache/iceberg-cpp/pull/880)
- Column default values end to end: [representation, serialization, and validation](https://github.com/apache/iceberg-cpp/pull/746), [Parquet reads of missing fields](https://github.com/apache/iceberg-cpp/pull/792), [Avro reads of missing fields](https://github.com/apache/iceberg-cpp/pull/800), and [`UpdateSchema` support](https://github.com/apache/iceberg-cpp/pull/793)
- Row lineage: [writing snapshot row lineage fields at the top level](https://github.com/apache/iceberg-cpp/pull/791), [reading row lineage metadata columns](https://github.com/apache/iceberg-cpp/pull/822), and [retrying stale row-lineage validation](https://github.com/apache/iceberg-cpp/pull/794)
- Deletion vectors: [`deletion-vector-v1` blob read and write](https://github.com/apache/iceberg-cpp/pull/777), [merging multiple DVs in `MergingSnapshotUpdate`](https://github.com/apache/iceberg-cpp/pull/708), and [Puffin and DV support moved into the core library](https://github.com/apache/iceberg-cpp/pull/900)

### Table Update APIs
- New snapshot-producing operations: [`DeleteFiles`](https://github.com/apache/iceberg-cpp/pull/709), [`RowDelta`](https://github.com/apache/iceberg-cpp/pull/721), [`OverwriteFiles`](https://github.com/apache/iceberg-cpp/pull/741), [`RewriteFiles`](https://github.com/apache/iceberg-cpp/pull/751), [`ReplacePartitions`](https://github.com/apache/iceberg-cpp/pull/776) (now [exposed on `Table` and `Transaction`](https://github.com/apache/iceberg-cpp/pull/925)), and [merge append](https://github.com/apache/iceberg-cpp/pull/699)
- [Retryable and cleanup-safe table updates](https://github.com/apache/iceberg-cpp/pull/868), [`FileCleanupStrategy` hardened with retries and parallel deletes](https://github.com/apache/iceberg-cpp/pull/649), and [cleanup of delete files from expired manifests](https://github.com/apache/iceberg-cpp/pull/779)
- Catalog operations including [`RenameTable` in `InMemoryCatalog`](https://github.com/apache/iceberg-cpp/pull/742) and [purge support in `DropTable` for the in-memory and SQL catalogs](https://github.com/apache/iceberg-cpp/pull/744)

### Catalogs and FileIO
- REST catalog gains [SigV4 authentication](https://github.com/apache/iceberg-cpp/pull/616), a [session-aware catalog](https://github.com/apache/iceberg-cpp/pull/750), [OAuth2 token exchange sessions](https://github.com/apache/iceberg-cpp/pull/867), and [per-table FileIO bound from vended storage credentials](https://github.com/apache/iceberg-cpp/pull/719)
- [`ResolvingFileIO`](https://github.com/apache/iceberg-cpp/pull/828) selects a FileIO by location scheme and forwards vended credentials, backed by a [registry-driven resolution](https://github.com/apache/iceberg-cpp/pull/889), with [S3-compatible schemes](https://github.com/apache/iceberg-cpp/pull/703) and the [`oss` scheme](https://github.com/apache/iceberg-cpp/pull/893) supported
- Hive Metastore groundwork with [vendored HMS IDL and generated bindings](https://github.com/apache/iceberg-cpp/pull/749), an [`iceberg_hive` library](https://github.com/apache/iceberg-cpp/pull/753), and [`HmsClient` connection lifecycle and URI parsing](https://github.com/apache/iceberg-cpp/pull/796)
- Closer alignment with the Java implementation for [REST table update serialization](https://github.com/apache/iceberg-cpp/pull/716) and [REST error handling](https://github.com/apache/iceberg-cpp/pull/763)

### Scan Planning and Performance
- [Lazy scan planning streams](https://github.com/apache/iceberg-cpp/pull/873) and a [fallible iterator utility](https://github.com/apache/iceberg-cpp/pull/905)
- Parallelism for [reading manifests](https://github.com/apache/iceberg-cpp/pull/697), [writing manifests](https://github.com/apache/iceberg-cpp/pull/778), and [update and scan processing](https://github.com/apache/iceberg-cpp/pull/770), plus an [LRU cache](https://github.com/apache/iceberg-cpp/pull/891)
- Metadata tables with a [base metadata table interface](https://github.com/apache/iceberg-cpp/pull/607) and [streaming `SnapshotsTable` scans](https://github.com/apache/iceberg-cpp/pull/801)
- Arrow data access via [`ArrowRowBuilder`](https://github.com/apache/iceberg-cpp/pull/780) and [reading list columns as `large_list`](https://github.com/apache/iceberg-cpp/pull/714)
- Metrics with [commit and scan reporting integration](https://github.com/apache/iceberg-cpp/pull/701), [Parquet NaN metrics collected during writes](https://github.com/apache/iceberg-cpp/pull/727), and a [unique-value optimization for `notEq`/`notIn`](https://github.com/apache/iceberg-cpp/pull/754)

### Logging
- A pluggable logging stack built up across the release: [`LogLevel`](https://github.com/apache/iceberg-cpp/pull/722), the [`Logger` interface and default logger](https://github.com/apache/iceberg-cpp/pull/723), a [`std::cerr` backend](https://github.com/apache/iceberg-cpp/pull/724), [logging macros](https://github.com/apache/iceberg-cpp/pull/725), an [optional spdlog backend behind `ICEBERG_SPDLOG`](https://github.com/apache/iceberg-cpp/pull/726), and a [`Loggers` registry](https://github.com/apache/iceberg-cpp/pull/737)
- [Transaction commit lifecycle logging](https://github.com/apache/iceberg-cpp/pull/890) as the first consumer

### Build Changes
- [Meson build support has been removed](https://github.com/apache/iceberg-cpp/pull/935); CMake is now the only supported build system
- Dependency upgrades to [Arrow 25.0.0](https://github.com/apache/iceberg-cpp/pull/858) and [nanoarrow 0.9.0](https://github.com/apache/iceberg-cpp/pull/871)

## Contributors

```
$ git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn v0.3.0..v0.4.0
    23  Junwang Zhao
    14  Gang Wu
    13  Manu Zhang
    13  Zehua Zou
    13  wzhuo
    10  Abanoub Doss
    10  Minh Vu
     9  Xin Huang
     8  kamcheungting-db
     7  Jiajia Li
     5  YangJie
     3  Kevin Liu
     3  Rahul Goel
     3  Xinli Shang
     3  lishuxu
     3  liuxiaoyu
     2  Anupam Yadav
     2  Joey
     2  Yuya Ebihara
     1  Huangshi Tian
     1  Innocent Djiofack
     1  Rahul Shivu Mahadev
     1  Sreesh Maheshwar
     1  Timothy Wang
     1  ZhaoXuan
     1  kid
```

This release welcomes 12 first-time contributors to Apache Iceberg C++: @kamcheungting-db, @All-less, @goel-skd, @abnobdoss, @ebyhr, @huan233usc, @yadavay-amzn, @timothyw553, @LuciferYang, @Angelia-Wang, @rahulsmahadev, and @u70b3.

We thank all contributors for their efforts in making this release possible!

## Roadmap for 0.5.0

The community is tracking the next release in [#959](https://github.com/apache/iceberg-cpp/issues/959), which focuses on the remaining Iceberg v3 feature support.

## Getting Involved

We welcome questions and contributions from all interested. Issues can be filed on [GitHub](https://github.com/apache/iceberg-cpp/issues), and questions can be directed to GitHub or the [Iceberg dev mailing list](https://iceberg.apache.org/community/#mailing-lists).
