---
date: 2026-09-03
title: Apache Iceberg Python 0.12.0 Release
slug: apache-iceberg-python-0.12.0-release
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

The Apache Iceberg community is pleased to announce the 0.12.0 release of Apache [Iceberg Python](https://github.com/apache/iceberg-python). This release includes over [185 pull requests](https://github.com/apache/iceberg-python/compare/pyiceberg-0.11.1...pyiceberg-0.12.0) from 50+ contributors, including 40 first-time contributors.

These notes cover the highlights. For the full list, see the [changelog](https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.12.0).

<!-- more -->

## Release Highlights

### View Support

This release adds initial support for Iceberg views:

- [ViewMetadata read support and `create_view`](https://github.com/apache/iceberg-python/pull/2154) for the REST catalog
- [`load_view`](https://github.com/apache/iceberg-python/pull/3224) for the REST catalog
- [Registering existing views](https://github.com/apache/iceberg-python/pull/3288)
- [View object API](https://github.com/apache/iceberg-python/pull/3338)
- [Pagination support for `list_views`](https://github.com/apache/iceberg-python/pull/3349)
- [Case-insensitive `View.sql_for`](https://github.com/apache/iceberg-python/pull/3407), returning `None` for an unknown dialect
- [`Identifier` support in `drop_view`](https://github.com/apache/iceberg-python/pull/3408)
- [More examples for working with views](https://github.com/apache/iceberg-python/pull/3414)

### Reads and Writes

- [Incremental append scan](https://github.com/apache/iceberg-python/pull/3512)
- [`pa.RecordBatchReader` support](https://github.com/apache/iceberg-python/pull/3335) in `Table.append`/`overwrite`
- [`dictionary_columns` option](https://github.com/apache/iceberg-python/pull/3461) for memory-efficient reads via `to_arrow()`/`to_arrow_batch_reader()`
- [Geometry and Geography types](https://github.com/apache/iceberg-python/pull/2859), with a new `geoarrow` extra
- [Commit retry and concurrency validation](https://github.com/apache/iceberg-python/pull/3320) for writes, plus [additional concurrency safety checks](https://github.com/apache/iceberg-python/pull/3049) and [validation of replaced data files during commit retries](https://github.com/apache/iceberg-python/pull/3811)
- [Configurable manifest cache size](https://github.com/apache/iceberg-python/pull/2993)
- [zstd blob decompression](https://github.com/apache/iceberg-python/pull/3575) in Puffin files
- [File format writer API](https://github.com/apache/iceberg-python/pull/3119), including a [`ParquetFormatModel`](https://github.com/apache/iceberg-python/pull/3381)
- [Fixed `deepcopy`](https://github.com/apache/iceberg-python/pull/3295) for `And`, `Or`, and `Not` expressions
- [Fixed strict `NotEqualTo`/`NotIn` pruning](https://github.com/apache/iceberg-python/pull/3521) with partial nulls or NaNs
- [Fixed `NOT STARTS WITH` projection](https://github.com/apache/iceberg-python/pull/3528) for truncated partitions
- [Fixed precision loss](https://github.com/apache/iceberg-python/pull/3405) in large integral string conversions
- [Fixed `ManifestEntry.snapshot_id` setter](https://github.com/apache/iceberg-python/pull/3257) writing to the wrong index
- [Fixed `DELETED` manifest entry `snapshot_id`](https://github.com/apache/iceberg-python/pull/3237) in `OverwriteFiles`
- [Preserved dictionary encoding](https://github.com/apache/iceberg-python/pull/3595) in `to_arrow_batch_reader`
- [Fixed nanosecond timestamp parsing](https://github.com/apache/iceberg-python/pull/3614) for sub-microsecond digits

### Performance

- [Pruned manifests](https://github.com/apache/iceberg-python/pull/3011) in snapshot overwrite operations
- [Balanced-tree partition filters](https://github.com/apache/iceberg-python/pull/3264) to avoid `RecursionError` on large expressions
- [Streamed manifest entries](https://github.com/apache/iceberg-python/pull/3287) for the `add_files` duplicate-file check

### Catalog and REST Improvements

- [REST loadCredentials support](https://github.com/apache/iceberg-python/pull/3499) and [storage-credentials in `LoadTableResult`](https://github.com/apache/iceberg-python/pull/3042)
- [Pagination support](https://github.com/apache/iceberg-python/pull/3347) for `list_namespaces` and [`list_tables`](https://github.com/apache/iceberg-python/pull/3348), with a [shared `page-size` option](https://github.com/apache/iceberg-python/pull/3377)
- Renamed `rest-scan-planning-enabled` to [`scan-planning-mode`](https://github.com/apache/iceberg-python/pull/3376)
- [`overwrite` option](https://github.com/apache/iceberg-python/pull/3290) for `register_table`
- [S3 server-side encryption configs](https://github.com/apache/iceberg-python/pull/3173) for `FsspecFileIO`
- [SigV4 retry configuration defaults](https://github.com/apache/iceberg-python/pull/3063) for REST

### CLI

- [`--warehouse` flag](https://github.com/apache/iceberg-python/pull/3080) for the REST catalog, replacing the short-lived `--prefix` flag added earlier in this cycle
- [`--version` flag](https://github.com/apache/iceberg-python/pull/3206), deprecating the `version` subcommand
- [`--purge` option](https://github.com/apache/iceberg-python/pull/3718) for `drop table`

## Breaking Changes

- [Raised `pyarrow` minimum from 17.0.0 to 18.0.0](https://github.com/apache/iceberg-python/pull/3606) for native UUID type support
- [Bumped `pyiceberg-core` from `>=0.5.1,<0.9.0` to `>=0.10.1,<0.11.0`](https://github.com/apache/iceberg-python/pull/3711), which bundles DataFusion 53.x behind a new `datafusion` extra
- [`NoopCatalog.table_exists` now returns `False`](https://github.com/apache/iceberg-python/pull/3284) instead of raising
- [Explicitly deleting a data file that's already missing now raises](https://github.com/apache/iceberg-python/pull/3818) instead of failing silently

## Infrastructure Improvements

- [Python 3.14 support](https://github.com/apache/iceberg-python/pull/3299)
- [Windows unit test job](https://github.com/apache/iceberg-python/pull/3723) added to CI
- [CodeQL security scanning](https://github.com/apache/iceberg-python/pull/3060)
- Migrated lockfile management to [uv-pre-commit](https://github.com/apache/iceberg-python/pull/3141) with `uv.lock` freshness enforced in CI
- [Third-party GitHub Actions pinned](https://github.com/apache/iceberg-python/pull/3172) to Apache-approved SHAs
- [Least-privilege workflow permissions](https://github.com/apache/iceberg-python/pull/3082)
- [PR auto-merge enabled](https://github.com/apache/iceberg-python/pull/3815)

## Contributors

```
$ git shortlog --perl-regexp --author='^((?!dependabot\[bot\]).*)$' -sn pyiceberg-0.11.1..pyiceberg-0.12.0
    36  Yuya Ebihara
    34  Kevin Liu
    12  Anas Khan
     9  Alex Stephen
     5  Neelesh Salian
     5  Minh Vu
     3  vishnu prakash
     3  Sotaro Hikita
     3  Junwang Zhao
     3  GayathriSrividya
     3  Fokko Driesprong
     2  Yingjian Wu
     2  Tanmay Rauth
     2  Sreesh Maheshwar
     2  Paul Mathew
     2  jj.lee
     2  Jared Yu (余启正)
     2  geruh
     2  Gabriel Igliozzi
     2  David Dallakyan
     2  David
     2  ChangHyeon Im
     2  Aaron Niskode-Dossett
     1  Yong Zheng
     1  Vova Kot
     1  Thomas Pfeiffer
     1  Sung Yun
     1  spr0els
     1  Sidra
     1  Ruiyang Wang
     1  Rob Reeves
     1  rcjverhoef
     1  R. Conner Howell
     1  Pucheng Yang
     1  Platon G. Gimp
     1  Noritaka Sekiyama
     1  Mrutunjay Kinagi
     1  Kristofer Gaudel
     1  Koen Denecker
     1  Jared Yu
     1  James Bornholt
     1  Federico
     1  David Zhao
     1  committobetter
     1  Chris Qiu
     1  CalebWeisgerber
     1  BharatDeva
     1  Ben Lai
     1  barking-code
     1  Antonio
     1  Adam
     1  achasnovskiy
     1  abnobdoss
```

We thank all contributors, including our 40 first-time contributors, for their efforts in making this release possible!

## Getting Involved

The PyIceberg project welcomes contributions. We use GitHub [issues](https://github.com/apache/iceberg-python/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try PyIceberg with your workloads and report any issues you encounter
2. Review the [contributor guide](https://py.iceberg.apache.org/contributing/#getting-started)
3. Look for [good first issues](https://github.com/apache/iceberg-python/contribute)

For more information, visit the [PyIceberg repository](https://github.com/apache/iceberg-python) or the [documentation](https://py.iceberg.apache.org/).
</content>
