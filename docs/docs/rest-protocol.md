---
title: "REST Catalog Protocol"
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

# REST Catalog Protocol

## Overview

As the Iceberg ecosystem grew, every engine and language needed its own client
for every catalog implementation. The REST catalog protocol solves this by
putting the catalog logic behind a common HTTP API, defined by the
[Iceberg REST Catalog API specification](../../rest-catalog-spec.md): a single
client implementation works with any compliant server. Because the server owns
commit logic, the protocol also enables capabilities that client-side catalogs
cannot offer, such as server-side conflict resolution, multi-table commits,
and secure table sharing through credential vending or remote signing.

This page explains the concepts behind the protocol features and how the Java
client uses them. For configuring a REST catalog connection and the full
client property reference, see the [REST catalog page](rest-catalog.md).

## Endpoint discovery and server-provided configuration

On initialization, the client calls the server's configuration route
(`GET /v1/config`, passing the configured `warehouse` as a query parameter if
one is set). The response can adjust the client's configuration in three ways:

- **`defaults`**: properties the server suggests; the client's own
  configuration takes precedence over them.
- **`overrides`**: properties the server requires; they take precedence over
  the client's configuration. A common override is `prefix`, which the client
  inserts into all subsequent request paths (`/v1/{prefix}/namespaces/...`)
  so that one server can host multiple catalogs.
- **`endpoints`**: the list of API endpoints the server supports. The client
  only uses features whose endpoints are advertised, so a server that does not
  implement, for example, view or scan-planning endpoints simply causes those
  features to be unavailable rather than producing failed requests.

If the server omits the `endpoints` field entirely, the client assumes a
default set of namespace and table endpoints. For older servers that support
views but predate endpoint discovery, set `view-endpoints-supported=true`
(see [REST catalog properties](rest-catalog.md#configuration)).

## Commit protocol

Unlike client-side catalogs, a REST client never writes a metadata file and
swaps a pointer itself. To commit, it sends the server the list of changes it
wants to make (`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`),
expressed as two parts:

- **Requirements** state what must still be true of the table's current
  metadata for the commit to be valid — for example, that a branch still
  points at the snapshot the change was based on, or that the table's UUID
  matches.
- **Updates** are the metadata changes themselves, such as adding a snapshot,
  setting a branch reference, or updating the schema.

The server validates every requirement against the table's current metadata
and, if all of them hold, applies the updates and writes the new metadata. If
a requirement fails, the commit is rejected and the client refreshes the
table and reapplies its changes before retrying. Because the server owns this
validation, all engines get the same conflict detection, and the server can
resolve conflicts or reject invalid changes centrally.

The protocol also defines an optional endpoint for committing changes to
multiple tables in one atomic operation
(`POST /v1/{prefix}/transactions/commit`), exposed in Java as
`RESTCatalog.commitTransaction`. Engines commit tables individually today, so
this is primarily a Java API feature, and servers are not required to support
it.

## Storage access delegation

The REST protocol lets the catalog server control access to table data, so
that clients do not need long-lived storage credentials of their own. The spec
defines the `X-Iceberg-Access-Delegation` header with two mechanisms, and a
server may supply access through either or both:

### Credential vending

The server returns short-lived, table-scoped storage credentials
(`storage-credentials`) in the load-table response. When they are present, the
client applies them when it creates the table's `FileIO`, so reads and writes
of data and metadata files use the vended credentials. Because each credential
is scoped to a table's storage prefixes, the catalog becomes the single point
of access control.

Whether credentials are vended is up to the server. Some servers only vend
credentials when the request carries the `X-Iceberg-Access-Delegation` header.

Vended credentials expire; the spec's dedicated credentials endpoint
(`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`) lets
clients fetch fresh credentials for long-running jobs. The S3 `FileIO`
refreshes credentials this way when `client.refresh-credentials-endpoint` is
set, which servers typically supply in the returned table configuration.

### Remote signing

With remote signing, the server never hands out credentials at all. Instead,
the client sends each storage request to the catalog's signing endpoint, and
the server returns the signed request for the client to execute. The signing
configuration is supplied by the server in the load-table response. For S3,
signing is activated by `s3.remote-signing-enabled=true`, which servers
typically set in the table configuration they return, so no client-side setup
is needed.

## Server-side scan planning

By default, the client plans scans itself: it reads the manifest list and
manifests and produces file scan tasks locally. With server-side scan
planning, the client instead submits the scan (filter, snapshot, selected
columns) to the server, which returns the scan tasks. This lets the server
apply its own optimizations — metadata caches, indexes — and saves the client
from downloading metadata files.

Planning follows an asynchronous lifecycle: the client submits a plan, and the
server either answers immediately with the completed result or returns a plan
ID that the client polls until planning finishes. Large results are returned
in batches of plan tasks that the client fetches separately. The client
cancels a plan it no longer needs, and gives up after
`rest-scan-planning.poll-timeout-ms` (default 5 minutes) or
the initial fetch plus `rest-scan-planning.poll-num-retries` retry attempts
(default 10), whichever limit is reached first.

The planning mode is controlled by the `scan-planning-mode` catalog property
(`client`, the default, or `server`), and the server can override the mode per
table in the load-table response. Server-side planning requires the server to
advertise the scan-planning endpoints. See
[REST catalog properties](rest-catalog.md#configuration) for
the related keys.

## Metadata freshness and caching

Loading a table normally means downloading its full metadata. The REST
protocol has two features that reduce that cost:

**Freshness-aware table loading.** When the server returns an `ETag` header
with a load-table response, the client caches the loaded table and presents
the ETag on subsequent loads via `If-None-Match`. If the table is unchanged,
the server answers `304 Not Modified` and the client reuses the cached table
instead of re-parsing metadata. The cache is bounded and time-limited; see
[table cache properties](rest-catalog.md#table-cache-properties).

**Lazy snapshot loading.** With `snapshot-loading-mode=REFS`, the client asks
the server for only the snapshots referenced by branches and tags rather than
the table's entire snapshot history. The remaining snapshots are fetched
lazily if and when they are actually needed, which speeds up loading tables
with long histories.

## Idempotent commit retries

A commit whose response is lost (for example, to a network timeout) is
dangerous to retry blindly: the first attempt may have succeeded. If the
server advertises support by returning `idempotency-key-lifetime` in its
configuration response, the client attaches a unique `Idempotency-Key` header
to mutating requests. When a request is retried with the same key, the server
recognizes it and returns the original outcome instead of applying the change
twice.

## Pagination

Listing endpoints (namespaces, tables, views) support pagination. Setting
`rest-page-size` makes the client request results in pages of that size;
otherwise the server's default applies.

## Metrics reporting

When the server advertises the metrics endpoint, the client reports scan and
commit metrics to the server (in addition to any configured
`metrics-reporter-impl`), giving the catalog operator visibility into table
usage. This can be disabled with `rest-metrics-reporting-enabled=false`. See
[Metrics reporting](metrics-reporting.md) for the report contents.
