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

The Apache Iceberg community is pleased to announce the 0.12.0 release of Apache [Iceberg Python](https://github.com/apache/iceberg-python).

The 0.12.0 release covers development work from early February through late August 2026 and is the result of merging over [470 pull requests](https://github.com/apache/iceberg-python/compare/pyiceberg-0.11.1...pyiceberg-0.12.0) from 58 contributors, including more than 40 first-time contributors.

These notes cover the highlights. For the full list, see the [changelog](https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.12.0).

<!-- more -->

## Release Highlights

### View Support

This release adds read support for Iceberg views. Views can be created, loaded, listed, registered, and dropped through the REST catalog, and are exposed through a `View` object.

```python
view = catalog.create_view(
    identifier=("default", "recent_orders"),
    schema=schema,
    view_version=view_version,
)

view = catalog.load_view("default.recent_orders")
view.sql_for("spark")
```

- [ViewMetadata read support and `create_view`](https://github.com/apache/iceberg-python/pull/2154) for the REST catalog
- [`load_view`](https://github.com/apache/iceberg-python/pull/3224) and the [`View` object API](https://github.com/apache/iceberg-python/pull/3338)
- [Registering existing views](https://github.com/apache/iceberg-python/pull/3288) via `register_view`
- [Pagination support for `list_views`](https://github.com/apache/iceberg-python/pull/3349)
- [More examples for working with views](https://github.com/apache/iceberg-python/pull/3414) and a [REST integration test suite](https://github.com/apache/iceberg-python/pull/3406)

For older REST servers that support the view endpoints but do not advertise them in the `ConfigResponse`, set the `view-endpoints-supported` catalog property.

### Commit Retry and Concurrency Validation

Writes now [retry on concurrent commits and validate for conflicts](https://github.com/apache/iceberg-python/pull/3320). When a catalog commit fails with `CommitFailedException`, `Transaction.commit_transaction()` automatically refreshes for new commits, validates for conflicts, and does a retry. 


### Incremental Append Scan

`Table.incremental_append_scan()` ([#3512](https://github.com/apache/iceberg-python/pull/3512)) reads the rows added by append snapshots within a snapshot range, projected onto the table's current schema.

```python
scan = table.incremental_append_scan(
    from_snapshot_id_exclusive=last_processed_snapshot_id,
    to_snapshot_id_inclusive=table.current_snapshot().snapshot_id,
)
new_rows = scan.to_arrow()
```

### Geometry and Geography Types

The v3 [`geometry` and `geography` primitive types](https://github.com/apache/iceberg-python/pull/2859) are now supported: schema parsing and serialization, Avro mapping via WKB bytes, and PyArrow/Parquet integration.

### File Format Writer API

PyIceberg has begun using the [File Format API](https://iceberg.apache.org/blog/apache-iceberg-file-format-api/)!


### Catalog and REST Improvements

- [REST `loadCredentials` support](https://github.com/apache/iceberg-python/pull/3499) with longest-prefix resolution for a target location, and [storage credentials in `LoadTableResult`](https://github.com/apache/iceberg-python/pull/3042)
- Pagination support for [`list_namespaces`](https://github.com/apache/iceberg-python/pull/3347) and [`list_tables`](https://github.com/apache/iceberg-python/pull/3348), with a [shared `rest-page-size` option](https://github.com/apache/iceberg-python/pull/3377)
- [`overwrite` option](https://github.com/apache/iceberg-python/pull/3290) for `register_table`
- [SigV4 retry configuration defaults](https://github.com/apache/iceberg-python/pull/3063) for REST
- [S3 server-side encryption configs](https://github.com/apache/iceberg-python/pull/3173) for `FsspecFileIO`
- [Glue `create_table` support](https://github.com/apache/iceberg-python/pull/3058) for S3 Tables federated databases
- An [`iceberg_type` column](https://github.com/apache/iceberg-python/pull/3263) for `SqlCatalog`, so Iceberg tables can be distinguished from other entries, [with filtering fixed](https://github.com/apache/iceberg-python/pull/3709) for existing catalogs
- [Catalog properties for the shared catalog test suite](https://github.com/apache/iceberg-python/pull/2982) and [coverage for nonexistent tables and namespaces](https://github.com/apache/iceberg-python/pull/2990)

### CLI

- [`--warehouse` flag](https://github.com/apache/iceberg-python/pull/3080) for the REST catalog, replacing the short-lived `--prefix` flag added earlier in this cycle
- [`--version` flag](https://github.com/apache/iceberg-python/pull/3206), deprecating the `version` subcommand, which now also [skips catalog loading](https://github.com/apache/iceberg-python/pull/3146)
- [`--purge` option](https://github.com/apache/iceberg-python/pull/3718) for `drop table`

## Bug Fixes

Notable correctness fixes in this release:

- [Fixed strict `NotEqualTo`/`NotIn` pruning](https://github.com/apache/iceberg-python/pull/3521) for files with partial nulls or NaNs
- [Corrected `NOT STARTS WITH` projection](https://github.com/apache/iceberg-python/pull/3528) for truncated partitions, along with [residual evaluation](https://github.com/apache/iceberg-python/pull/3503) and the [string-based `starts_with`/`not_starts_with` methods](https://github.com/apache/iceberg-python/pull/3501)
- [Fixed the `ManifestEntry.snapshot_id` setter](https://github.com/apache/iceberg-python/pull/3257) writing to the wrong index
- [Fixed the `DELETED` manifest entry `snapshot_id`](https://github.com/apache/iceberg-python/pull/3237) in `OverwriteFiles`
- [Preserved a manifest min sequence number of 0](https://github.com/apache/iceberg-python/pull/3660) rather than dropping it
- [Stripped the spec-mandated deletion vector blob framing](https://github.com/apache/iceberg-python/pull/3576) before deserializing, with a [Spark interop test](https://github.com/apache/iceberg-python/pull/3476) covering the read path
- [Fixed `deepcopy`](https://github.com/apache/iceberg-python/pull/3295) for `And`, `Or`, and `Not` expressions
- [Fixed precision loss](https://github.com/apache/iceberg-python/pull/3405) in large integral string conversions, with [Long bounds returned for decimal conversion](https://github.com/apache/iceberg-python/pull/3470), [bounds sentinels for long date literals](https://github.com/apache/iceberg-python/pull/3546), and an [overflow sentinel in `LongLiteral.to(FloatType)`](https://github.com/apache/iceberg-python/pull/3502)
- [Padded sub-microsecond digits](https://github.com/apache/iceberg-python/pull/3614) when parsing nanosecond timestamps, and [handled non-UTC nanosecond timestamps](https://github.com/apache/iceberg-python/pull/3142)
- [Used minimal byte length](https://github.com/apache/iceberg-python/pull/3746) when encoding decimals for negative powers of two
- [Preserved dictionary encoding](https://github.com/apache/iceberg-python/pull/3595) in `to_arrow_batch_reader`
- [Fixed `delete_data_file` on partitioned tables](https://github.com/apache/iceberg-python/pull/3780) and [residual `NotNaN` for null partition values](https://github.com/apache/iceberg-python/pull/3689)
- [Avoided committing update builders](https://github.com/apache/iceberg-python/pull/3354) after exceptions
- [Preserved `write_default`](https://github.com/apache/iceberg-python/pull/3601) when applying a name mapping
- [Handled zero-byte files](https://github.com/apache/iceberg-python/pull/3353) in fsspec `__len__`, [extracted the ADLS `account_name` from the URI hostname](https://github.com/apache/iceberg-python/pull/3005), and [parsed S3 virtual addressing as a boolean](https://github.com/apache/iceberg-python/pull/3492)
- [Stopped reusing `TSaslClientTransport`](https://github.com/apache/iceberg-python/pull/3357) to eliminate server-side SASL noise
- [Rejected unsupported identity transform types](https://github.com/apache/iceberg-python/pull/3517), [empty `source-ids`](https://github.com/apache/iceberg-python/pull/3411), and [decimal precision outside the valid range](https://github.com/apache/iceberg-python/pull/3585)

### Catalog and REST Improvements

- [REST loadCredentials support](https://github.com/apache/iceberg-python/pull/3499) and [storage-credentials in `LoadTableResult`](https://github.com/apache/iceberg-python/pull/3042)
- [Pagination support](https://github.com/apache/iceberg-python/pull/3347) for `list_namespaces` and [`list_tables`](https://github.com/apache/iceberg-python/pull/3348), with a [shared `page-size` option](https://github.com/apache/iceberg-python/pull/3377)
- Renamed `rest-scan-planning-enabled` to [`scan-planning-mode`](https://github.com/apache/iceberg-python/pull/3376)
- [`overwrite` option](https://github.com/apache/iceberg-python/pull/3290) for `register_table`
- [S3 server-side encryption configs](https://github.com/apache/iceberg-python/pull/3173) for `FsspecFileIO`
- [SigV4 retry configuration defaults](https://github.com/apache/iceberg-python/pull/3063) for REST

## Breaking Changes

- [Raised `pyarrow` minimum from 17.0.0 to 18.0.0](https://github.com/apache/iceberg-python/pull/3606) for native UUID type support
- [Bumped `pyiceberg-core` from `>=0.5.1,<0.9.0` to `>=0.10.1,<0.11.0`](https://github.com/apache/iceberg-python/pull/3711), which bundles DataFusion 53.x behind a new `datafusion` extra
- [`NoopCatalog.table_exists` now returns `False`](https://github.com/apache/iceberg-python/pull/3284) instead of raising
- [Explicitly deleting a data file that's already missing now raises](https://github.com/apache/iceberg-python/pull/3818) instead of failing silently

## Infrastructure Improvements

- [Python 3.14 support](https://github.com/apache/iceberg-python/pull/3299)
- [Windows unit test job](https://github.com/apache/iceberg-python/pull/3723) added to CI
- [CodeQL security scanning](https://github.com/apache/iceberg-python/pull/3060) for GitHub Actions, and a [documented Iceberg security model](https://github.com/apache/iceberg-python/pull/3425)
- Migrated lockfile management to [uv-pre-commit](https://github.com/apache/iceberg-python/pull/3141) with [`uv.lock` freshness enforced in CI](https://github.com/apache/iceberg-python/pull/3144)
- [Third-party GitHub Actions pinned](https://github.com/apache/iceberg-python/pull/3172) to Apache-approved SHAs, with an [allowlist check on every PR](https://github.com/apache/iceberg-python/pull/3550) and [least-privilege workflow permissions](https://github.com/apache/iceberg-python/pull/3082)
- [Papermill-based tests](https://github.com/apache/iceberg-python/pull/3330) that execute the documentation examples, and a [release verification script](https://github.com/apache/iceberg-python/pull/3777)
- [PR auto-merge enabled](https://github.com/apache/iceberg-python/pull/3815)

## Getting Involved

The PyIceberg project welcomes contributions! We use GitHub [issues](https://github.com/apache/iceberg-python/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

The easiest way to get started is to:

1. Try PyIceberg with your workloads and report any issues you encounter
2. Review the [contributor guide](https://py.iceberg.apache.org/contributing/#getting-started)
3. Look for [good first issues](https://github.com/apache/iceberg-python/contribute)

Code review is also a very welcome contribution - please provide feedback on pull requests where you feel comfortable to do so!

For more information, visit the [PyIceberg repository](https://github.com/apache/iceberg-python) or the [documentation](https://py.iceberg.apache.org/).
