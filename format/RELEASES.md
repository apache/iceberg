---
title: "Specification Releases"
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

# Specification Releases

Specifications are adopted by community vote and take effect when the vote
passes. They are not tied to the release of any implementation, so specification
changes are tracked here rather than in the release notes of the Java or any
other implementation.

This page records changes to the specifications under the `format` directory.
Changes to the REST catalog specification are not covered here.

## What is recorded

A specification version is listed once it is adopted, with a summary of its main
changes. The full set of changes in each table specification version is
described in [Appendix E](spec.md#appendix-e-format-version-changes).

After a version is adopted, every change to it is recorded here. A change is
listed under the most recent version it affects. Many changes also apply to
earlier versions, because the specifications describe all versions in a single
document; such changes are not repeated under each version.

Each entry links the vote that adopted the change. Changes that do not require a
vote, such as grammar, spelling and minor formatting fixes, are not recorded.

## Table specification

### Version 4

Version 4 is under development and has not been adopted. Changes are tracked in
[Appendix E](spec.md#version-4).

Main changes:

* Relative locations in metadata fields

### Version 3

Adopted on 2025-05-23 ([vote](https://lists.apache.org/thread/9oncq0j0222nrm6scvm89xdq3gozg24p)).

Main changes:

* New data types:
    * `timestamp_ns` and `timestamptz_ns`
    * `unknown`
    * `variant`
    * `geometry` and `geography`
* Default value support for columns
* Multi-argument transforms for partitioning and sorting
* Row lineage tracking
* Binary deletion vectors
* Table encryption keys

Changes since adoption:

| Date | Change | Pull request | Vote |
| ---------- | ------------------------------------------------------- | ------- | ---- |
| 2026-08-19 | Clarify content file uniqueness within a snapshot | [#17198](https://github.com/apache/iceberg/pull/17198) | [vote](https://lists.apache.org/thread/orwny91fqyfmzx7w7p6wj7cgmndvd8r3) |
| 2026-06-13 | Clarify the result type of the day partition transform in manifests | [#16446](https://github.com/apache/iceberg/pull/16446) | [vote](https://lists.apache.org/thread/gz432tvboxvno2v7g3l17c8tbtxckxrb) |
| 2026-05-18 | Clarify conventions for non-default CRS in geospatial types | [#15834](https://github.com/apache/iceberg/pull/15834) | [vote](https://lists.apache.org/thread/8z0kc8vz1379lghwlcwl0jmsoym6shd7) |
| 2025-10-14 | Clarify restrictions for geometry types | [#14250](https://github.com/apache/iceberg/pull/14250) | [vote](https://lists.apache.org/thread/v96xj6kxwclyl52d97dkcvzxytfypyp3) |
| 2025-09-18 | Bring back `added-rows` in snapshot fields | [#14048](https://github.com/apache/iceberg/pull/14048) | [vote](https://lists.apache.org/thread/5gff76hvqy7r1lksrt9ld9h3pdp03nb5) |
| 2025-07-31 | Correct the type of `snapshot-id` in table statistics | [#13513](https://github.com/apache/iceberg/pull/13513) | [vote](https://lists.apache.org/thread/z6loky5225j1vszorm199ywwncjwf6qz) |

### Version 2

Adopted on 2021-08-02 ([vote](https://lists.apache.org/thread/ws2gg52d124p7bx9jgrn3kctrtfgtltp)).

Main changes:

* Row-level updates and deletes using position and equality delete files
* Sequence numbers to order changes across snapshots, manifests and files
* Stricter requirements for writers

Changes since adoption:

| Date | Change | Pull request | Vote |
| ---------- | ------------------------------------------------------- | ------- | ---- |
| 2025-09-15 | Deprecate position delete files with row data | [#14045](https://github.com/apache/iceberg/pull/14045) | [vote](https://lists.apache.org/thread/tfy96bqmz1bmdxr73x17w3xxj3yzs606) |
| 2025-03-06 | Add an implementation note on `current-snapshot-id` | [#12334](https://github.com/apache/iceberg/pull/12334) | [vote](https://lists.apache.org/thread/54r4nm7qmr4vxhdpwmbx5rntynspskl7) |
| 2025-01-24 | Document optional snapshot summary fields | [#11660](https://github.com/apache/iceberg/pull/11660) | [vote](https://lists.apache.org/thread/mz01jwt69osqxhx9d3dd9xzncv9yncd0) |
| 2024-08-04 | Clarify file system tables | [#10833](https://github.com/apache/iceberg/pull/10833) | [vote](https://lists.apache.org/thread/1jm9zd6rtgjsvly3x099wvgdbohjn29v) |

### Version 1

Version 1 was adopted before specification changes were voted on, so there is no
vote to link for the version itself.

No changes recorded. Clarifications that also apply to v1 are listed under the
most recent version they affect.

## View specification

Adopted on 2022-04-03 in [#3188](https://github.com/apache/iceberg/pull/3188),
before specification changes were voted on.

No changes recorded.

## Puffin specification

Adopted on 2022-06-23 ([vote](https://lists.apache.org/thread/950rz31y3kr3kz0zzncwokvgzbrmmz4q)).

No changes recorded.

## AES GCM stream specification

Adopted on 2022-11-27 in [#5432](https://github.com/apache/iceberg/pull/5432),
before specification changes were voted on.

No changes recorded.

## SQL UDF specification

Adopted on 2026-02-05 ([vote](https://lists.apache.org/thread/whbgoc325o99vm4b599f0g1owhgww2kx)).

Changes since adoption:

| Date | Change | Pull request | Vote |
| ---------- | ------------------------------------------------------- | ------- | ---- |
| 2026-07-01 | Add optional `specific-name` to the UDF definition model | [#16727](https://github.com/apache/iceberg/pull/16727) | [vote](https://lists.apache.org/thread/75xs4trcgcpog8b2zoxpknf8bq6vfk4c) |

## Expressions specification

Adopted on 2026-06-30 ([vote](https://lists.apache.org/thread/6wfmjhgthlykwbk3f7df4zgcm40xtm2o)).

No changes recorded.

## Mumbling bitmap specification

A draft added on 2026-06-09 ([vote](https://lists.apache.org/thread/59bn4jty8jmtbks8td8zfqodkcpro1hw)).
The specification has not been adopted, so its changes are not recorded here.
