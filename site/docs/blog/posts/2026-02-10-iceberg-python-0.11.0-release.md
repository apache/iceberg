---
date: 2026-02-10
title: Apache Iceberg Python 0.11.0 Release
slug: apache-iceberg-python-0.11.0-release
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

The Apache Iceberg community is pleased to announce the 0.11.0 release of Apache [Iceberg Python](https://github.com/apache/iceberg-python). This release includes over [380 pull requests](https://github.com/apache/iceberg-python/compare/pyiceberg-0.10.0...pyiceberg-0.11.0) from 50+ contributors, including 28 first-time contributors.

These notes cover the highlights. For the full list, see the [changelog](https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.11.0).

<!-- more -->

## Release Highlights

### Reads and Writes

- [DeleteFileIndex](https://github.com/apache/iceberg-python/pull/2918) for faster delete file lookups
- [Generator-based writes](https://github.com/apache/iceberg-python/pull/2671) to reduce memory pressure
- [Relaxed `field-id` constraint](https://github.com/apache/iceberg-python/pull/2662) on `add_files`
- [Connection reuse](https://github.com/apache/iceberg-python/pull/2543) for remote S3 signing
- [Multi-process safe ExecutorFactory](https://github.com/apache/iceberg-python/pull/2546)
- [Fixed O(N²) manifest cache growth](https://github.com/apache/iceberg-python/pull/2951)

### Snapshot Management

- [Roll back to a specific snapshot ID](https://github.com/apache/iceberg-python/pull/2878)
- [Roll back to a point in time](https://github.com/apache/iceberg-python/pull/2879)
- [Set the current snapshot directly](https://github.com/apache/iceberg-python/pull/2871)

### Catalog Improvements

- [Entra ID (Azure AD) auth manager](https://github.com/apache/iceberg-python/pull/2974)
- [Configurable namespace separator](https://github.com/apache/iceberg-python/pull/2826) for REST catalogs and a new [`namespace_exists` method](https://github.com/apache/iceberg-python/pull/2972)
- [`rename_table`](https://github.com/apache/iceberg-python/pull/2588) now validates source and destination namespaces exist
- [X-Client-Version header](https://github.com/apache/iceberg-python/pull/2910) on REST requests
- [AWS profile support](https://github.com/apache/iceberg-python/pull/2948) for Glue and fsspec S3 FileIO
- [`anon` property](https://github.com/apache/iceberg-python/pull/2661) for fsspec ADLS FileIO and [S3 `addressing_style`](https://github.com/apache/iceberg-python/pull/2517) support

### ORC Read Support

[Full ORC read support](https://github.com/apache/iceberg-python/pull/2432) was added to the PyArrow I/O layer.

### Sort Order Evolution

[Sort order can now be updated](https://github.com/apache/iceberg-python/pull/2552) on existing tables without recreating them.

### Supported Endpoints in ConfigResponse

REST catalogs can now [declare which endpoints they support](https://github.com/apache/iceberg-python/pull/2848) via the `ConfigResponse`. The client checks operations against that set, falling back to defaults for older servers that don't return the field. If you call something the server doesn't support, you get a clear error instead of a cryptic failure.

### REST Scan Planning

Table scans can now be [planned server-side](https://github.com/apache/iceberg-python/pull/2864) by REST catalogs. This release adds synchronous planning where the client sends a scan request and the server returns file scan tasks to be read. Async planning (submit, poll, cancel) is planned for a future release.

## Breaking Changes

- [Dropped Python 3.9](https://github.com/apache/iceberg-python/pull/2554)
- [Removed methods deprecated for 0.11.0](https://github.com/apache/iceberg-python/pull/2983)

## Infrastructure Improvements

- [Python 3.13 support](https://github.com/apache/iceberg-python/pull/2863)
- [aarch64 wheel builds](https://github.com/apache/iceberg-python/pull/2973)
- [Migrated to UV](https://github.com/apache/iceberg-python/pull/2601) for dependency management
- [pyiceberg-core 0.8.0](https://github.com/apache/iceberg-python/pull/2928) with DataFusion 51

## Contributors

```
$ git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn pyiceberg-0.10.0..pyiceberg-0.11.0
    44  Kevin Liu
    30  Drew Gallardo
    19  Alex Stephen
    16  Fokko Driesprong
     6  Gabriel Igliozzi
     4  Aniket
     4  Neelesh Salian
     4  Soham
     4  Somasundaram Sekar
     3  Brad Cannon
     3  Jaime Fernández
     3  Manu Zhang
     2  Luke Fitzgerald
     2  Yujiang Zhong
     1  Alex
     1  Anton-Tarazi
     1  Bharath Krishna
     1  Ehsan Totoni
     1  Elton SV
     1  FANNG
     1  Gowthami B
     1  Grégoire Clément
     1  Hanzhi Wang
     1  Honah (Jonas) J.
     1  Ian Atkinson
     1  Jayce Slesar
     1  Jiajia Li
     1  Kevin Jiao
     1  Kyle Heath
     1  Leandro Martelli
     1  Luiz Otavio Vilas Boas Oliveira
     1  NNSatyaKarthik
     1  NikitaMatskevich
     1  Quentin FLEURENT NAMBOT
     1  Ragnar Dahlén
     1  Raúl Cumplido
     1  Stanley Law
     1  Stats
     1  Thomas Powell
     1  Tiansu
     1  Tom
     1  Victorien
     1  Yftach Zur
     1  Yuya Ebihara
     1  chao qian
     1  fcfangcc
     1  gmweaver
     1  jeroko
     1  jtuglu1
     1  nathanbijleveld
     1  vinjai
```

We thank all contributors for their efforts in making this release possible!

## Getting Involved

The PyIceberg project welcomes contributions. We use GitHub [issues](https://github.com/apache/iceberg-python/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try PyIceberg with your workloads and report any issues you encounter
2. Review the [contributor guide](https://py.iceberg.apache.org/contributing/#getting-started)
3. Look for [good first issues](https://github.com/apache/iceberg-python/contribute)

For more information, visit the [PyIceberg repository](https://github.com/apache/iceberg-python) or the [documentation](https://py.iceberg.apache.org/).
