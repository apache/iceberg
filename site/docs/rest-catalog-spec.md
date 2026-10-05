---
title: "REST Catalog Spec"
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

## REST Catalog API Specification

Iceberg defines a REST-based Catalog API for managing table metadata and performing catalog operations. You can find the OpenAPI specification here:
[REST Catalog OpenAPI YAML](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).

You can also explore the API interactively using the [Swagger UI](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml).

## Format Versioning

| Version | Status  | Adopted | Vote | Changes since adoption                            |
|---------|---------|---------|------|---------------------------------------------------|
| 1       | Adopted | -       | -    | [Changes since adoption](#changes-since-adoption) |

## REST Catalog Protocol

As the Iceberg project grew to support more languages and engines, pluggable catalogs started to cause some practical problems. Catalogs needed to be implemented in multiple languages and it proved difficult for commercial offerings to support many different catalogs and clients.

To solve compatibility problems and pave a path for new features, the community created the REST catalog protocol, a common API (using the OpenAPI spec) for interacting with any Iceberg catalog. This is analogous to Hive's thrift protocol for HMS.

The REST protocol is important for several reasons:

- **Language and Engine Compatibility**: New languages and engines can support any catalog with just one client implementation.
- **Improved Reliability**: It uses change-based commits to enable server-side deconfliction and retries — fewer failures!
- **Simplified Metadata Management**: Metadata version upgrades are easier because root metadata is written by the catalog service.
- **Advanced Features**: It enables new features such as lazy snapshot loading, multi-table commits, and caching.
- **Security**: The protocol supports secure table sharing using credential vending or remote signing.

You can use the REST catalog protocol with any built-in catalog using translation in the `CatalogHandlers` class, or using the community maintained [`iceberg-rest-fixture`](https://hub.docker.com/r/apache/iceberg-rest-fixture) docker image.

## Changes since adoption

| Date       | Change                                                       | Pull request                                           | Vote                                                                     |
|------------|--------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------------------------|
| 2026-09-08 | Add catalog-provided `labels` to table and view load results | [#15750](https://github.com/apache/iceberg/pull/15750) | [vote](https://lists.apache.org/thread/do69l2nfm88m024ol34m2pdy0rqomqnz) |
| 2026-09-03 | Add finer grained read restrictions to `loadTable`           | [#13879](https://github.com/apache/iceberg/pull/13879) | [vote](https://lists.apache.org/thread/0zloqhp8wkgyn04yg69j71cwcg5n7g74) |
| 2026-08-02 | Add the variant type                                         | [#17256](https://github.com/apache/iceberg/pull/17256) | [vote](https://lists.apache.org/thread/gkj526shb0y96hdmms73sn3tvsw8mgw9) |
| 2026-07-30 | Formalize remote signing configuration                       | [#16822](https://github.com/apache/iceberg/pull/16822) | [vote](https://lists.apache.org/thread/n4hyqr3k2gnzbxvs3l1o4cz3ggdfwkx2) |
| 2026-07-27 | Align expressions with the expressions spec                  | [#17138](https://github.com/apache/iceberg/pull/17138) | [vote](https://lists.apache.org/thread/njo5bzgf4m126ly74ydz1pfznhz70h38) |
| 2026-06-06 | Add list and load function endpoints                         | [#15180](https://github.com/apache/iceberg/pull/15180) | [vote](https://lists.apache.org/thread/qd7jb3fdx65mcjvcjomrxg7rgxf3yn0m) |
| 2026-05-26 | Add the unregister table endpoint                            | [#16400](https://github.com/apache/iceberg/pull/16400) | [vote](https://lists.apache.org/thread/vy9fqyopp9c149oy40brdhb10grpq32g) |
| 2026-05-18 | Add the `CatalogObjectIdentifier` schema                     | [#16144](https://github.com/apache/iceberg/pull/16144) | [vote](https://lists.apache.org/thread/xyzlndcp4x7zq6xydpcbp192ov9p2skz) |
| 2026-04-22 | Clarify identifier uniqueness across tables and views        | [#15691](https://github.com/apache/iceberg/pull/15691) | [vote](https://lists.apache.org/thread/2647zx7omfyocxl8og7s2tbomgprxvts) |
| 2026-04-15 | Add a 404 response to the config endpoint                    | [#15746](https://github.com/apache/iceberg/pull/15746) | [vote](https://lists.apache.org/thread/bcc62zxhc1rqjmwxxll81rpn8yfnr9fy) |
| 2026-03-31 | Promote the remote signing endpoint to the main spec         | [#15450](https://github.com/apache/iceberg/pull/15450) | [vote](https://lists.apache.org/thread/rspzcx0o2mkm271fb8csghs4x98sfh3h) |
| 2026-03-13 | Add `scan-planning-mode` to the `loadTable` config           | [#14867](https://github.com/apache/iceberg/pull/14867) | [vote](https://lists.apache.org/thread/mt4y8vw80glnqqwsr8czfmwvhqf09h46) |
| 2026-02-12 | Add `referenced-by` to `loadTable`                           | [#13810](https://github.com/apache/iceberg/pull/13810) | [vote](https://lists.apache.org/thread/jnv5f978gc530z0vx9h6l18dopoltm7j) |
| 2026-02-09 | Add access delegation to `registerTable`                     | [#15231](https://github.com/apache/iceberg/pull/15231) | [vote](https://lists.apache.org/thread/fgpwnsbslwkm4qxh10y6xn52pprbk274) |
| 2026-02-09 | Add access delegation to the scan planning endpoints         | [#14781](https://github.com/apache/iceberg/pull/14781) | [vote](https://lists.apache.org/thread/80dm5b8rxcnqwfqwqoc43grjg8hktqgf) |
| 2026-01-17 | Add the register view endpoint                               | [#14869](https://github.com/apache/iceberg/pull/14869) | [vote](https://lists.apache.org/thread/520or6svg2yxcthbjc2wt5c9lbx0ntpz) |
| 2025-12-19 | Add `ETag` to `CommitTableResponse`                          | [#14760](https://github.com/apache/iceberg/pull/14760) | [vote](https://lists.apache.org/thread/fvjrkgm29ohh1f222opps0xnowgo8zz3) |
| 2025-12-08 | Add idempotency keys to the mutating scan planning endpoints | [#14730](https://github.com/apache/iceberg/pull/14730) | [vote](https://lists.apache.org/thread/6xo0v5bnvzq0ktd7tptszgm0csjp2p80) |
| 2025-12-05 | Make the namespace separator configurable by the server      | [#14448](https://github.com/apache/iceberg/pull/14448) | [vote](https://lists.apache.org/thread/8k777v9l2lr7930g8fxrwjqnh5csl48k) |
| 2025-11-21 | Add `min-rows-requested` to scan planning requests           | [#14565](https://github.com/apache/iceberg/pull/14565) | [vote](https://lists.apache.org/thread/t210ccrcz7fglv3vlwvnz827hb65217d) |
| 2025-11-20 | Add storage credentials to scan planning results             | [#14563](https://github.com/apache/iceberg/pull/14563) | [vote](https://lists.apache.org/thread/vq6nzsc0vs81r8dhr94dp222s0gq1ykw) |
| 2025-11-19 | Add the `Idempotency-Key` header                             | [#14196](https://github.com/apache/iceberg/pull/14196) | [vote](https://lists.apache.org/thread/ybo8xst7h2p8vt6nctxf4nxfc0jbc6rq) |
| 2025-11-18 | Add `planId` to the credentials endpoint                     | [#14519](https://github.com/apache/iceberg/pull/14519) | [vote](https://lists.apache.org/thread/002y8ndg0s5l7vswkpr4nlr31ld3myg3) |
| 2025-08-21 | Mark 503 as non-retryable for table updates                  | [#13619](https://github.com/apache/iceberg/pull/13619) | [vote](https://lists.apache.org/thread/xno8c88sm4mlt0y5yw7p3tsob8wmhgkg) |
| 2025-05-27 | Add row lineage fields                                       | [#13010](https://github.com/apache/iceberg/pull/13010) | [vote](https://lists.apache.org/thread/1q3xkvn2373n3g43qjjhnzj7p0v3d9p9) |
| 2025-05-12 | Add encryption key updates                                   | [#12987](https://github.com/apache/iceberg/pull/12987) | [vote](https://lists.apache.org/thread/l055gx3spwy2n24kthy38fhoj2qy1tq7) |
| 2025-03-20 | Clarify dropping non-empty namespaces                        | [#12518](https://github.com/apache/iceberg/pull/12518) | [vote](https://lists.apache.org/thread/lxfbrv32wwn1tkvc37dmor2f8rbkrv2c) |
| 2025-02-17 | Add the overwrite option to table registration               | [#12239](https://github.com/apache/iceberg/pull/12239) | [vote](https://lists.apache.org/thread/bm9x6f9lxh2901j2scwoqs2gf6mlm6yt) |
| 2025-02-14 | Add the `RemoveSchemas` update                               | [#12022](https://github.com/apache/iceberg/pull/12022) | [vote](https://lists.apache.org/thread/n97qo5nb09ggz5591g7cp99767fv5cf7) |
| 2025-01-28 | Add initial and write defaults to schema fields              | [#12094](https://github.com/apache/iceberg/pull/12094) | [vote](https://lists.apache.org/thread/k606d81cv8h9f0oxtv5wzwg7qtb8mgxc) |
| 2025-01-27 | Add freshness-aware table loading                            | [#11946](https://github.com/apache/iceberg/pull/11946) | [vote](https://lists.apache.org/thread/cfxbdzs0rfopjg8qy101qc9vsokn75p5) |
| 2025-01-27 | Deprecate `snapshot-id` in `SetStatisticsUpdate`             | [#12010](https://github.com/apache/iceberg/pull/12010) | [vote](https://lists.apache.org/thread/lcjvzp8o1n6bvvhbr1lo0zx7w9ncs1pm) |
| 2024-11-25 | Deprecate `last-column-id` in `AddSchemaUpdate`              | [#11514](https://github.com/apache/iceberg/pull/11514) | [vote](https://lists.apache.org/thread/of8x50751vy3hv3xk3gcgncp4jry8z6x) |
| 2024-11-02 | Add deletion vector fields to delete files                   | [#11240](https://github.com/apache/iceberg/pull/11240) | [vote](https://lists.apache.org/thread/gxkkpvv2n69q8xq4n3k83ykp11cb1kzn) |
| 2024-10-24 | Add the credentials refresh endpoint                         | [#11281](https://github.com/apache/iceberg/pull/11281) | [vote](https://lists.apache.org/thread/jkpy0zhf5ppjbrxfz5q8gzdsxbg6xrdx) |
| 2024-10-18 | Standardize vended credentials in load results               | [#10722](https://github.com/apache/iceberg/pull/10722) | [vote](https://lists.apache.org/thread/trvnplkbx9kkdxn3m6owwrm8dsc31dhf) |
| 2024-09-10 | Add scan planning endpoints                                  | [#9695](https://github.com/apache/iceberg/pull/9695)   | [vote](https://lists.apache.org/thread/7xtpt6m95qwrk8tc8h40p7c743218ylc) |
| 2024-08-28 | Add the `RemovePartitionSpecs` update                        | [#10846](https://github.com/apache/iceberg/pull/10846) | [vote](https://lists.apache.org/thread/5pt5g4q9zt856cpscob37y30mczlf9rx) |
| 2024-08-23 | Add endpoint discovery to the config response                | [#10928](https://github.com/apache/iceberg/pull/10928) | [vote](https://lists.apache.org/thread/ptg7yoz0tv5c5mb0p1dbc75p9blfos7h) |
| 2024-08-16 | Require commits to fail on unknown updates and requirements  | [#10848](https://github.com/apache/iceberg/pull/10848) | [vote](https://lists.apache.org/thread/99lo7stnprchjzosjcq9k3mns1mq8fwc) |
| 2024-07-15 | Fix property names for statistics and partition statistics   | [#10662](https://github.com/apache/iceberg/pull/10662) | [vote](https://lists.apache.org/thread/tp4dpw6qj5fyhrh17197bf6fg4gj3rk6) |
| 2024-07-11 | Deprecate the `oauth/tokens` endpoint                        | [#10603](https://github.com/apache/iceberg/pull/10603) | [vote](https://lists.apache.org/thread/o4qmrm5jx50mk1mqws0t9f1z2op4gvvm) |
