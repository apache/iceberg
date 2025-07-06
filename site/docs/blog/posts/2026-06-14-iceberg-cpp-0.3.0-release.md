---
date: 2026-06-14
title: Apache Iceberg C++ 0.3.0 Release
slug: apache-iceberg-cpp-0.3.0-release
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

The Apache Iceberg community is pleased to announce the 0.3.0 release of Apache [Iceberg C++](https://github.com/apache/iceberg-cpp). This release includes [over 140 pull requests](https://github.com/apache/iceberg-cpp/compare/v0.2.0...v0.3.0) from 23 contributors, including 11 first-time contributors.

`iceberg-cpp` is a native C++ implementation of the Apache Iceberg table format, providing libraries for reading, writing, and managing Iceberg tables in C++ applications.

<!-- more -->

## Release Highlights

### Scan Planning and Data Access
- [Incremental scan APIs](https://github.com/apache/iceberg-cpp/pull/559), [incremental append scans](https://github.com/apache/iceberg-cpp/pull/590), and [incremental changelog scans](https://github.com/apache/iceberg-cpp/pull/611) for planning table changes between snapshots
- Merge-on-read data access with a [MOR file scan task reader](https://github.com/apache/iceberg-cpp/pull/657), [delete filter support](https://github.com/apache/iceberg-cpp/pull/650), and a [DeleteLoader for v2 position and equality delete files](https://github.com/apache/iceberg-cpp/pull/610)
- [Column selection in table scan planning](https://github.com/apache/iceberg-cpp/pull/550) and [ManifestGroup file filtering](https://github.com/apache/iceberg-cpp/pull/664)
- [Roaring-based position bitmaps](https://github.com/apache/iceberg-cpp/pull/595), a [position delete index](https://github.com/apache/iceberg-cpp/pull/605), and [range coalescing for position deletes](https://github.com/apache/iceberg-cpp/pull/645)

### Table Operations and Maintenance
- [MergingSnapshotUpdate](https://github.com/apache/iceberg-cpp/pull/682) lays the groundwork for table overwrite, delete, update, and various maintainances.
- [SnapshotManager](https://github.com/apache/iceberg-cpp/pull/542) support and [retried transaction commits](https://github.com/apache/iceberg-cpp/pull/626)
- Snapshot expiration cleanup strategies for [reachable file cleanup](https://github.com/apache/iceberg-cpp/pull/592) and [incremental file cleanup](https://github.com/apache/iceberg-cpp/pull/648)
- [Partition statistics updates](https://github.com/apache/iceberg-cpp/pull/538) and [schema update mapping](https://github.com/apache/iceberg-cpp/pull/561)

### Catalogs and Integrations
- REST catalog improvements including [initial OAuth2 support](https://github.com/apache/iceberg-cpp/pull/577), [OAuth2 token auto-refresh](https://github.com/apache/iceberg-cpp/pull/646), [basic authentication](https://github.com/apache/iceberg-cpp/pull/564), [snapshot loading mode](https://github.com/apache/iceberg-cpp/pull/543), [namespace separators](https://github.com/apache/iceberg-cpp/pull/617), and [server-side scan planning endpoints](https://github.com/apache/iceberg-cpp/pull/614)
- [S3 FileIO integration](https://github.com/apache/iceberg-cpp/pull/548) built on Arrow filesystem support
- FileIO interface enrichment with new [InputFile and OutputFile interfaces](https://github.com/apache/iceberg-cpp/pull/641) and [bulk delete support](https://github.com/apache/iceberg-cpp/pull/659)
- [SQL catalog support](https://github.com/apache/iceberg-cpp/pull/693) backed by SQLite, PostgreSQL, and MySQL stores

### Metrics and Observability
- Metrics reporter support with [report JSON serialization and reporter loading](https://github.com/apache/iceberg-cpp/pull/589)
- [Avro writer metrics](https://github.com/apache/iceberg-cpp/pull/604) and [Parquet writer metrics](https://github.com/apache/iceberg-cpp/pull/651)

### Metadata and File Format Support
- Puffin support with [basic data structures](https://github.com/apache/iceberg-cpp/pull/588), [format constants and JSON serialization](https://github.com/apache/iceberg-cpp/pull/603), and [file reader/writer support](https://github.com/apache/iceberg-cpp/pull/624)
- Iceberg v3 support for the [unknown type](https://github.com/apache/iceberg-cpp/pull/662) and [nanosecond timestamp types](https://github.com/apache/iceberg-cpp/pull/653)
- Expression serialization with [operation JSON serialization](https://github.com/apache/iceberg-cpp/pull/532), [expression JSON serialization](https://github.com/apache/iceberg-cpp/pull/553), and [typed literal binding after serialization](https://github.com/apache/iceberg-cpp/pull/562)

## Contributors

```
$ git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn v0.2.0..v0.3.0
    26  Gang Wu
    26  Junwang Zhao
    11  wzhuo
     8  Kevin Liu
     8  Xinli Shang
     7  Feiyang Li
     7  Zehua Zou
     6  lishuxu
     4  Innocent Djiofack
     4  liuxiaoyu
     3  Guotao Yu
     3  ZhaoXuan
     3  slfan1989
     2  Manu Zhang
     2  Minh Vu
     2  Sebastian Baunsgaard
     2  SkylerLin
     2  姚军
     1  Jiajia Li
     1  Maxim Zibitsker
     1  Sandeep Gottimukkala
     1  Sung Yun
     1  Yingfan Guo
```

This release welcomes 11 first-time contributors to Apache Iceberg C++: @evindj, @manuzhang, @mzibitsker, @fallintoplace, @gsandeep1241, @Baunsgaard, @linguoxuan, @sungwy, @sentomk, @zhaoxuan1994, and @SYaoJun.

We thank all contributors for their efforts in making this release possible!

## Roadmap for 0.4.0

The community is actively tracking the next release in [#637](https://github.com/apache/iceberg-cpp/issues/637), with a focus on filling out Iceberg v3 support and expanding table maintenance APIs.

## Getting Involved

We welcome questions and contributions from all interested. Issues can be filed on [GitHub](https://github.com/apache/iceberg-cpp/issues), and questions can be directed to GitHub or the [Iceberg dev mailing list](https://iceberg.apache.org/community/#mailing-lists).
