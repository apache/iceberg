---
date: 2026-01-26
title: Apache Iceberg C++ 0.2.0 Release
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

The Apache Iceberg community is pleased to announce the 0.2.0 release of Apache Iceberg C++. This release includes [over 200 merged pull requests](https://github.com/apache/iceberg-cpp/compare/v0.1.0...v0.2.0) from 23 distinct contributors.

The release notes below are not exhaustive and only expose selected highlights of the release. Many other bugfixes and improvements have been made: we refer you to the [complete changelog](https://github.com/apache/iceberg-cpp/releases/tag/v0.2.0) for details.

<!-- more -->

## Release Highlights

### Table Scan and Data Access
- Support for v2 deletes and metadata column reads
- Enhanced ManifestReader with projection and filtering
- Implemented file scan task reader with Arrow C Stream integration

### Table Operations
- Schema evolution: add, delete, update, and move columns
- Table updates: properties, sort order, partition spec, location, and statistics
- Transaction API with snapshot management (fast append)

### REST Catalog
- Full REST Catalog client with namespace operations and table CRUD operations
- Support for create, load, drop, list, update, and stage-create table operations
- Integration test coverage

### Expression System
- Complete expression framework with literal expressions, type casting, and binary serialization
- Inclusive/strict metrics evaluators, manifest evaluator, and residual evaluator
- Aggregate expressions and projection evaluators

### Performance and I/O
- Optimized Avro reader/writer with direct encoding and multi-block support
- Configurable Avro and Parquet readers/writers

### Catalog and Metadata
- InMemoryCatalog implementation with table management
- Location provider and partition path generation
- Schema selection, projection, and table metadata builder

### Miscellaneous
- Meson build system support
- Initial documentation website and devcontainer
- Improved code organization and type safety with validation

## Contributors

```
$ git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn v0.1.0..v0.2.0
    47  Feiyang Li
    47  Junwang Zhao
    41  Gang Wu
    17  Zhuo Wang
    15  Xinli Shang
    14  Zehua Zou
     9  Guotao Yu
     7  Xiao Dong
     6  Chao Liu
     3  Jiajia Li
     3  Shuxu Li
     2  Kevin Liu
     2  William Ayd
     2  Zhiyuan Liang
     1  Dipanshu Pandey
     1  Subham
     1  liuxiaoyu
     1  slfan1989
```

We thank all contributors for their efforts in making this release possible!

## Roadmap for 0.3.0

The community is actively working on the next release, see [#523](https://github.com/apache/iceberg-cpp/issues/523).

## Getting Involved

We welcome questions and contributions from all interested. Issues can be filed on [GitHub](https://github.com/apache/iceberg-cpp/issues), and questions can be directed to GitHub or the [Iceberg dev mailing list](https://iceberg.apache.org/community/#mailing-lists).
