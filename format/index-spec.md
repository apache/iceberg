---
title: "Index Spec"
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
# Iceberg Index Specification

## Overview

An index accelerates retrieval of rows from an Iceberg table without scanning the entire dataset. Indexes are optional.
Engines may create, maintain, consume, or ignore them.

An index is recorded in an index metadata file that contains the index definition and a set of index snapshots. Each
index snapshot corresponds to a snapshot of the source table and references the index data for that state.

Index metadata files and index data files are immutable. Every update writes a new metadata file and a new tracking
file, may reuse existing range files, and is committed by an atomic swap of the index metadata file, as defined in
[Commits and Concurrency](#commits-and-concurrency).

The index data of a snapshot is organized as a [tracking file](#tracking-file) that lists a set of
[range files](#range-files):

```text
Index Metadata
    |
    +-- Index Snapshot(s)
            |
            +-- Tracking File
                    |
                    +-- Range Files
```

## Definitions

### Index Type

The index type defines the logical category of an index and the class of queries it accelerates.

| Type   | Status                            | Description                                                                                                           |
|--------|-----------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| SCALAR | Defined by this specification     | Accelerates point lookups on clustered fields, and range filters when the clustering expressions are order preserving |
| VECTOR | Reserved for future specification | Accelerates similarity search over vector embeddings                                                                  |

### Index Fields

An index is defined by materialized and non-materialized fields. Each index field is produced by evaluating an
[Iceberg value expression](expressions-spec.md#value-expressions) for an indexed row of the source table.

An index field has the following fields:

| Requirement | Field     | Type            | Description                                                  |
|-------------|-----------|-----------------|--------------------------------------------------------------|
| required    | field-id  | int             | ID that uniquely identifies the index field                  |
| required    | type      | string          | Index field representation; must be `expr-value`             |
| required    | data-type | Iceberg type    | Type produced by the expression                              |
| required    | expr      | JSON expression | Value expression that produces the field, serialized as JSON |

Each index field must satisfy the following requirements:

- `expr` must contain only ID references to fields in the source table schema. Named references must not be used.
- `expr` must be deterministic and must produce the declared `data-type`.
- `field-id` must be unique across both `materialized-fields` and `non-materialized-fields`.

An expression that consists of a single field reference, or of an `identity` function applied to a single field
reference, is an **identity expression** of the referenced source field. The `field-id` of an identity expression of a
source table field must be the field ID of that field.

The `field-id` of any other expression should not be the field ID of a source table field, so fields that carry a source
table field ID can be recognized.

Expressions are serialized using the [JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by
the expressions specification.

### Materialized Fields

`materialized-fields` is a list of index fields whose values are stored in the [range files](#range-files). The list
must not be empty. Evaluating the list for one indexed row produces one range file row. Materialized fields in
`cluster-spec` must also be represented in the [content statistics](#content-statistics).

The materialized fields must allow a reader to identify matching range file rows. For each field in the
[cluster spec](#cluster-spec), the list must contain an identity expression for every source table field that the
cluster field expression references, or, when that expression produces a distinct result for every distinct input, the
cluster field itself may be materialized. For example, clustering on `bucket(256, user_id)` requires materializing
`user_id`, because rows with different `user_id` values can share a bucket, while clustering on `user_id` may
materialize the cluster field itself.

### Non-Materialized Fields

`non-materialized-fields` is a list of index fields whose row values are not stored in range files. Their values are
stored only as field statistics in tracking file entries. A non-materialized field is useful for a clustering
expression, such as `bucket(256, user_id)` or a Hilbert curve over several source fields, when its row-level values do
not need to be duplicated in range files.

### Cluster Spec

`cluster-spec` is a list of field IDs from `materialized-fields` and `non-materialized-fields`. The values of the
referenced fields, in list order, form the clustering key of an indexed row and determine the row's position in the
index, as defined in [Clustering and Ordering](#clustering-and-ordering). The list must not be empty. Every referenced
field must have a primitive `data-type`.

For example, assume that `user_id` is field `1`, `bucket(2147483647, user_id)` is field `103`,
`bucket(256, user_id)` is field `104`, and `hilbert(x, y)` is field `105`:

| Cluster spec | Referenced field expressions            | Clustering                                         |
|--------------|-----------------------------------------|----------------------------------------------------|
| `[ 1 ]`      | `[ user_id ]`                           | By the source column values                        |
| `[ 103 ]`    | `[ bucket(2147483647, user_id) ]`       | By the hash bucket of the source column            |
| `[ 104, 1 ]` | `[ bucket(256, user_id), user_id ]`     | By hash bucket, then by the source column values   |
| `[ 105 ]`    | `[ hilbert(x, y) ]`                     | By position on a Hilbert curve over two dimensions |

### Index Definition

An index is defined by a source table, an index type, materialized fields, non-materialized fields, a cluster spec, and
optional index properties. The definition is fixed when the index is created and must not change for the lifetime of
the index, so range files remain readable through every index snapshot that references them. A different definition
requires a new index.

A table may have multiple indexes of the same index type.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field                   | Type                 | Description                                                                                        |
|-------------|-------------------------|----------------------|----------------------------------------------------------------------------------------------------|
| required    | format-version          | int                  | Index format version; must be `1`                                                                  |
| required    | uuid                    | string               | Stable UUID assigned at creation                                                                   |
| required    | table-uuid              | string               | UUID of the indexed table                                                                          |
| required    | location                | string               | Index root location                                                                                |
| required    | type                    | string               | Logical index type                                                                                 |
| required    | materialized-fields     | list<index-field>    | Fields stored in range files, see [Materialized Fields](#materialized-fields)                      |
| required    | non-materialized-fields | list<index-field>    | Fields stored only in tracking statistics, see [Non-Materialized Fields](#non-materialized-fields) |
| required    | cluster-spec            | list<int>            | Field IDs that define clustering, see [Cluster Spec](#cluster-spec)                                |
| optional    | properties              | map<string,string>   | Index properties applicable for every snapshot                                                     |
| required    | snapshots               | list<index-snapshot> | Index snapshots                                                                                    |
| optional    | encryption-keys         | list<encryption-key> | Encryption keys used by the index, see [Encryption Keys](#encryption-keys)                         |

### Index Snapshot

An index snapshot is an immutable version of the index data generated from a specific source table snapshot. It
references a complete set of index files and contains all data from the referenced table snapshot through the location
of a single [tracking file](#tracking-file).

| Requirement | Field                    | Type               | Description                                                        |
|-------------|--------------------------|--------------------|--------------------------------------------------------------------|
| required    | snapshot-id              | long               | Index snapshot identifier                                          |
| required    | source-table-snapshot-id | long               | Source table snapshot                                              |
| required    | timestamp-ms             | long               | Snapshot creation timestamp                                        |
| required    | index-data               | string             | Location of the tracking file                                      |
| optional    | properties               | map<string,string> | Snapshot properties specific to this snapshot                      |
| optional    | key-id                   | string             | ID of the encryption key that holds the tracking file key metadata |

Each `snapshot-id` and each `source-table-snapshot-id` must be unique within the `snapshots` list, so a source table
snapshot has at most one index snapshot and engines locate index data by matching `source-table-snapshot-id`.

### Encryption Keys

Index metadata is not encrypted, so keys are never stored in plain form. Keys used for index encryption are tracked in
index metadata as a list named `encryption-keys`, using the same structure as the table specification (see
[Encryption Keys](spec.md#encryption-keys)). The schema of each key is a struct with the following fields:

| Requirement | Field                  | Type               | Description                                                   |
|-------------|------------------------|--------------------|---------------------------------------------------------------|
| required    | key-id                 | string             | ID of the encryption key                                      |
| required    | encrypted-key-metadata | string             | Encrypted key and metadata, base64 encoded [1]                |
| optional    | encrypted-by-id        | string             | Optional ID of the key used to encrypt or wrap `key-metadata` |
| optional    | properties             | map<string,string> | Additional metadata used by the index's encryption scheme     |

Notes:

1. The format of encrypted key metadata is determined by the index's encryption scheme and can be a wrapped format
specific to the KMS provider.

The `key-id` of an index snapshot must reference a `key-id` in the index metadata `encryption-keys` list. The
`encrypted-key-metadata` of the referenced entry is the key metadata of the snapshot's tracking file, which in turn
holds the key metadata of the range files.

## Commits and Concurrency

Index metadata is immutable. Every update, whether adding a snapshot, dropping a snapshot, or changing index properties,
must produce a new index metadata file with a unique name.

A commit replaces the current index metadata file with the new one. The swap must be atomic and must succeed only if the
current metadata file is still the file the writer started from, identified by name. If a newer metadata file has been
committed since the writer read the metadata, the commit must be rejected.

A writer whose commit is rejected must not overwrite the newer metadata. It may re-read the latest committed metadata
and retry the update on top of it, or discard the attempted update.

Index maintenance may be performed synchronously with the table commit that produces a new source-table snapshot, or
asynchronously by a separate maintenance process. A catalog may enforce transactional commits that atomically update
both the table and the index, guaranteeing that every committed table snapshot has a corresponding index snapshot. When
an index is updated asynchronously, the index may lag behind the table and engines must reconcile the index snapshot
against the source-table snapshot they intend to read.

## Index Data

### Clustering and Ordering

The cluster spec, together with the tie-break below, defines a total ordering over all index entries of an index
snapshot. Entries must be clustered into non-overlapping ranges according to that ordering, and each range must be
stored in a separate range file.

Index entries are ordered by the [clustering key](#cluster-spec) produced for each indexed row. The key is compared by
the fields in `cluster-spec` order: entries are compared by the value of the first field, and the next field is used
only when the preceding values compare as equal. Each field is ordered ascending.

Primitive values are compared using the rules defined in the
[expressions specification](expressions-spec.md#comparisons), extended so that null and NaN values have a defined
position in the total order:

- `null` values are ordered before all other values (nulls-first)
- `NaN` values are ordered after all other `float` and `double` values

Entries with equal clustering keys must be ordered by the location of the source row: by the `_file` metadata column,
ascending, and then by the `_pos` metadata column, ascending. The source row location is always appended to the range
schema.

### Tracking File

The tracking file contains metadata of all range files belonging to the index snapshot. It may be stored using any
supported metadata file format.

#### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single range file
tracked by an index snapshot. The fields are the subset of the V4 [data file fields](spec.md#data-file-fields) that are
relevant to planning queries against the index.

Tracking file entries must be stored in the [clustering order](#clustering-and-ordering) of the range files they
describe, which is the ascending order of the `group_max_value` statistics recorded for the cluster fields in the
[content statistics](#content-statistics).

| Field ID | Name               | Type   | Requirement | Description                                                                                          |
|----------|--------------------|--------|-------------|------------------------------------------------------------------------------------------------------|
| 100      | file_path          | string | required    | Full URI of the referenced range file.                                                               |
| 101      | file_format        | string | required    | File format name, such as parquet, avro, or orc.                                                     |
| 103      | record_count       | long   | required    | Number of records contained in the referenced range file.                                            |
| 104      | file_size_in_bytes | long   | required    | Total file size in bytes.                                                                            |
| 146      | content_stats      | struct | required    | Field statistics and clustering bounds for the referenced range file, used for planning and pruning. |
| 131      | key_metadata       | binary | optional    | Implementation-specific key metadata, used for range file encryption.                                |

#### Content Statistics

The `content_stats` structure stores field statistics following the [content stats](spec.md#content-stats) rules of the
table specification. Each stored struct derives its ID and metric types from the index field's `field-id` and
`data-type` and contains the metrics supported for that type.

Statistics are required for non-materialized fields and every materialized field in `cluster-spec`, optional for other
materialized fields.

Statistics must not be stored for the appended `file_path` (`2147483546`) and `pos` (`2147483545`) fields.

##### Group Max Value

The field statistics struct for each field in `cluster-spec` must contain a `group_max_value` metric at offset `8` from
the field's stats `base-id`. It has the index field's `data-type` and is optional so that it can represent a null
clustering value.

The `group_max_value` metrics, read in `cluster-spec` order, must be the exact clustering key of the last entry in the
range file according to the [clustering order](#clustering-and-ordering). They must not be truncated or rounded.
Readers use these keys as range file upper bounds. The lower bound is the clustering key of the preceding tracking file
entry, so tracking file entries must be read in order.

### Range Files

Range files contain the materialized fields and represent the lowest level of the index hierarchy.

Range files must be standard Iceberg data files and may be stored using any Iceberg-supported data file format: Parquet,
Avro, or ORC.

Each range file row contains the result of evaluating the [materialized fields](#materialized-fields) for one indexed
row, followed by the location of that source row. Entries within a range file must be stored in the
[clustering order](#clustering-and-ordering).

#### Range Schema

The range schema is constructed from `materialized-fields`. The result is a struct containing one field for each entry,
with fields appearing in the same order as the list. Each field takes its ID from `field-id` and its type from
`data-type`. The following required source row location fields are then appended:

| Field ID   | Name      | Type   | Source metadata column |
|------------|-----------|--------|------------------------|
| 2147483546 | file_path | string | `_file` (`2147483646`) |
| 2147483545 | pos       | long   | `_pos` (`2147483645`)  |

Names of materialized fields in the range schema are generated by the writer and are not defined by this specification.
Users of the index must not rely on them; readers must match range file columns by field ID.

## Future Extensions

Future specifications may define additional index types, for example VECTOR indexes for similarity search or text/term
indexes. Additional clustering strategies do not require changes to this specification and can be added as functions
in the `iceberg_functions` catalog of the [expressions specification](expressions-spec.md), for example a function that
maps multi-column values to their Hilbert curve position. The result can be declared as an index field and referenced
by `cluster-spec`.

## Appendix A: Goals and Rationale

### Goals

This specification defines a portable metadata format for indexes and a common storage architecture for index data, so
that an index written by one engine can be read by another. Indexes are operated independently of source table
metadata, which allows them to be built and maintained without rewriting the table. The index type and the index
expressions together form a framework in which new kinds of indexes can be defined without changing this specification.

Iceberg standardizes the index lifecycle, snapshot relationship, and the minimum metadata needed for safe cross-engine
use. Beyond that minimum, engines remain free to ignore unsupported indexes, use exact snapshot matches only, or
implement more advanced stale-index and incremental-query logic. The index type exists to support this choice: it tells
an engine what class of queries an index can accelerate, so the engine can decide whether the index applies to a query
without understanding how the index was built.

### Expression-based Definitions

An index is defined by expressions rather than by a fixed list of columns and a closed set of transforms. This keeps
the index definition open ended, but expressions must be deterministic for the same reason clustering must be stable:
an expression that depends on `random` or on the evaluation time would place entries at positions that cannot be
reproduced.

Each index field contains the expression that produces its value. Materialized field values are stored in range files,
while non-materialized field values are represented only by tracking statistics. The cluster spec lists field IDs in
comparison order without repeating their expressions. Engines match query expressions to index fields to determine
whether the index applies and which materialized field contains a result. Because expressions reference only source
table fields, each index field can be evaluated directly from a source row.

### Total Clustering Order and Pruning

Each field in the cluster spec determines part of the position of an entry, so its result type is limited to values
that Iceberg can order. Primitives are ordered by the rules the expressions specification already defines.
Multi-component clustering keys are compared field by field, which is an extension of the sort orders in the Iceberg
table specification. Structs, lists, and maps are excluded because Iceberg does not define ordering for lists and maps,
and a struct is represented as separate index fields instead.

Clustering keys alone do not have to be unique. Ordering entries with equal keys by the location of the source row makes
the ordering total, because a source row is uniquely identified by its file path and position.

The clustering order makes the index usable at two levels: range files can be pruned without being opened, and the
entries of a range file that is opened can be located without reading all of it.

When available, lower and upper bounds support pruning on partial clustering keys and fields outside the cluster spec.

Range files hold non-overlapping clustering ranges, so the `group_max_value` statistics in the tracking file are enough
to eliminate a range file. Only the upper bound of a range is stored, because clustered tracking file entries and
non-overlapping ranges make the lower bound redundant: it is the upper bound of the preceding entry.

Each component of the bound is stored in the field statistics of the clustered field. The cluster spec supplies the
component order, including when a component is a non-materialized expression such as a bucket or Hilbert curve. The
`group_max_value` is distinct from `upper_bound`: it is the field value from the last row in clustering order, not
necessarily the greatest value of that field in the range file.

The bound has to be exact because readers derive the lower bound of a range file's range from the upper bound of the
preceding entry. A bound rounded up would place that lower bound above entries the next range file actually contains, so
a lookup would prune to the wrong file and miss rows.

Within a range file, the entries that match a lookup are contiguous, so a reader can locate them with the structures the
file format provides for stored columns, such as Parquet page indexes, instead of examining every entry. Those
structures work on a materialized field that clustering keeps sorted, or a value from which the clustering expression
is order preserving: a file clustered on `day(ts)` is also ordered by a materialized `ts` field. Clustering on
`bucket(256, user_id)` leaves a materialized `user_id` field unsorted unless the bucket field is also materialized, so a
reader may need to evaluate the clustering expression over range file rows.

### Range Schema Derivation

The range schema is derived from the materialized fields, so the index definition and the physical layout of the index
cannot drift apart and the schema does not have to be maintained as a second, redundant copy of the definition.

Requiring materialized fields to identify matching rows is what keeps a range file useful on its own. A transformation
that maps several values to the same result cannot distinguish the entries that share a clustering value, so its source
values have to be materialized. Materializing the transformed result as well is a performance choice, because a reader
can search it directly.

The declared `data-type` fixes the physical and statistics types for the lifetime of the index. It also allows a reader
to construct those schemas without binding expressions against a possibly evolved source schema. A source schema
change that makes an expression incompatible with its declared type requires a new index definition.

Fields produced by identity expressions keep the field ID of the source column, which preserves column identity through
renames and makes the relationship between source and materialized fields explicit. Expressions still reference the
source field IDs, so they are not rewritten when a column is renamed.

Fields in `cluster-spec` must be primitive because clustering requires a defined total order. Other fields may use any
Iceberg type. A nested `data-type` includes IDs for the fields in its subtree, allowing a covering index to materialize
lists, maps, or structs.

The source row location is always appended to the range schema under the field IDs `file_path` (`2147483546`) and `pos`
(`2147483545`), rather than under `_file` and `_pos`. The `_file` and `_pos` metadata columns describe where the row
being read is physically stored, which for a range file row is the range file itself. The position delete columns are
defined to record the location of a row in another file, which is exactly what a range file row carries.

### Atomic Commits

Index metadata is immutable and committed by an atomic swap, mirroring how Iceberg commits table metadata. Requiring
the current metadata file to be unchanged is what prevents concurrent maintenance processes from silently overwriting
each other and losing snapshots.

## Appendix B: Example - Key Lookup Index

Imagine an `events` table that already has a single snapshot (source table snapshot `3055729675574597004`). To speed up
point lookups on the `user_id` column, a key lookup index is created.

```sql
CREATE INDEX bucket_index
    ON events (user_id)
    CLUSTERED BY (bucket(256, user_id), user_id);
```

This creates a `SCALAR` index on the `user_id` column that clusters entries by the hash bucket of `user_id` and then by
`user_id` itself. When the index is created, the engine (or a later index maintenance job) reads the current table
snapshot, writes the range files and a tracking file, and produces the first index metadata file containing a single
index snapshot. Range file boundaries follow the clustering, so a range file holds a contiguous range of buckets or a
range of `user_id` values within a single bucket. The tracking file stores summary information and pruning statistics.

The only materialized field is `user_id`, which keeps the field ID of the source column because it is an identity
expression. The bucket is field `104`; it is evaluated for clustering and tracking statistics but is not materialized
in range files. The source row location fields are appended after `user_id`, producing this range schema:

| Field ID   | Column    | Type   | Description                                                  |
|------------|-----------|--------|--------------------------------------------------------------|
| 1          | user_id   | long   | The indexed source column, keeping its source table field ID |
| 2147483546 | file_path | string | The source data file that contains the row, from `_file`     |
| 2147483545 | pos       | long   | The row position within the source data file, from `_pos`    |

The JSON metadata file is shown below.

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00001-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/bucket_index",
  "type" : "SCALAR",
  "materialized-fields" : [ {
    "field-id" : 1,
    "type" : "expr-value",
    "data-type" : "long",
    "expr" : { "type" : "reference", "id" : 1 }
  } ],
  "non-materialized-fields" : [ {
    "field-id" : 104,
    "type" : "expr-value",
    "data-type" : "int",
    "expr" : {
      "type" : "apply",
      "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
      "arguments" : [ 256, { "type" : "reference", "id" : 1 } ]
    }
  } ],
  "cluster-spec" : [ 104, 1 ],
  "snapshots" : [ {
    "snapshot-id" : 1,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet"
  } ]
}
```

The tracking file referenced by `index-data` lists the range files of this snapshot. It is stored in a metadata file
format rather than JSON, so its entries are shown here as a table. In this example the index snapshot has two range
files:

| file_path               | file_format | record_count | file_size_in_bytes |
|-------------------------|-------------|--------------|--------------------|
| .../range-00001.parquet | parquet     | 3            | 1160               |
| .../range-00002.parquet | parquet     | 2            | 1024               |

Each entry also carries a `content_stats` struct. `file_path` and `pos` have no statistics, so it holds field statistics
for the materialized `user_id` and the non-materialized bucket. Both fields participate in `cluster-spec`, so both stats
structs include `group_max_value`:

```
146: required struct content_stats {
  10_200: optional struct user_id {
    10_201: optional long lower_bound;
    10_202: optional long upper_bound;
    10_208: optional long group_max_value;
  }
  30_800: optional struct bucket {
    30_801: optional int lower_bound;
    30_802: optional int upper_bound;
    30_808: optional int group_max_value;
  }
}
```

| Statistic                   | `range-00001.parquet` | `range-00002.parquet` |
|-----------------------------|-----------------------|-----------------------|
| `user_id` bounds            | `12094` .. `84721`    | `3277` .. `99182`     |
| bucket bounds               | `3` .. `88`           | `120` .. `209`        |
| bucket `group_max_value`    | `88`                  | `209`                 |
| `user_id` `group_max_value` | `55310`               | `3277`                |

The `group_max_value` metrics form the clustering upper bounds `{ bucket: 88, user_id: 55310 }` and
`{ bucket: 209, user_id: 3277 }` when read in `cluster-spec` order. The `user_id` bounds of the two files overlap, so
ordinary field statistics alone cannot eliminate either file. The clustering ranges do not overlap:
`range-00002.parquet` is the second tracking file entry, so its range starts after the first entry's upper bound.

A lookup for `user_id = 55310` evaluates the clustering expressions for that value, producing
`{ bucket: 88, user_id: 55310 }`. That key is not greater than the upper bound of `range-00001.parquet`, the first
tracking file entry, so only the first range file is read.

The rows of `range-00001.parquet` follow the range schema constructed from the materialized fields. They are stored in
clustering order. The non-materialized bucket is shown here to make the complete clustering key visible:

| user_id | file_path                       | pos | (clustering key) |
|---------|---------------------------------|-----|------------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`   |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }`  |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }`  |

Reading the matched row gives the source data file and row position of the indexed row, which the engine uses to read
`user_id = 55310` from the `events` table without scanning it.

Later, new data is added to the `events` table, producing a new table snapshot (`5459876531255530170`). Index
maintenance runs again and writes new range files for the added data, plus a new tracking file that references both the
still-valid old range files and the new range files.

This produces a new index metadata file that completely replaces the previous one. The old index snapshot
(`snapshot-id 1`) is kept alongside the new one (`snapshot-id 2`), so engines can still use the index against the older
table snapshot. The index definition is unchanged, so it is elided from the new metadata file below:

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00002-(uuid).metadata.json
```
```
{
  ...
  "snapshots" : [ {
    "snapshot-id" : 1,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet"
  }, {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```

The new rows fall into buckets that lie inside the range already covered by `range-00001.parquet`. Because range files
must hold non-overlapping clustering ranges, maintenance rewrites that range file as `range-00003.parquet`
with the merged entries. `range-00002.parquet` covers a disjoint range and is reused unchanged, so the tracking file of
`snapshot-id 2` references it as well:

| file_path               | file_format | record_count | file_size_in_bytes |
|-------------------------|-------------|--------------|--------------------|
| .../range-00003.parquet | parquet     | 5            | 1480               |
| .../range-00002.parquet | parquet     | 2            | 1024               |

The merged entries fall inside the range that `range-00001.parquet` already covered, so `range-00003.parquet` keeps the
same field bounds and the same `group_max_value` metrics, which produce the clustering upper bound
`{ bucket: 88, user_id: 55310 }`. The entry for `range-00002.parquet` is copied from the previous tracking file.

The rows of `range-00003.parquet` interleave the entries of the rewritten range file with the entries added for the new
data file, keeping the clustering order:

| user_id | file_path                       | pos | (clustering key) |
|---------|---------------------------------|-----|------------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`   |
| 71004   | .../data/00002-0-(uuid).parquet | 5   | `{ 17, 71004 }`  |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }`  |
| 40318   | .../data/00002-0-(uuid).parquet | 22  | `{ 62, 40318 }`  |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }`  |

`range-00001.parquet` is no longer referenced by `snapshot-id 2`, but it is still referenced by `snapshot-id 1` and must
be retained while that snapshot exists.

Eventually the older table snapshot is no longer needed, so maintenance drops the corresponding index snapshot
(`snapshot-id 1`). It writes a new index metadata file that removes the snapshot from the `snapshots` list and replaces
the previous metadata file. Maintenance then deletes the files referenced only by the removed snapshot: its tracking
file, `tracking-00001-(uuid).parquet`, and `range-00001.parquet`. `range-00002.parquet` and `range-00003.parquet` are
still referenced by `snapshot-id 2` and are retained. The index definition is again elided:

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00003-(uuid).metadata.json
```
```
{
  ...
  "snapshots" : [ {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```
