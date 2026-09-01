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

| Type   | Status                            | Description                                                                                                        |
|--------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------|
| SCALAR | Defined by this specification     | Accelerates point lookups on the index keys, and range filters when the index key expressions are order preserving |
| VECTOR | Reserved for future specification | Accelerates similarity search over vector embeddings                                                               |

### Index Expressions

An index is defined by the [index values](#index-values) and the [index keys](#index-keys), which are built from
[Iceberg expressions](expressions-spec.md). Every expression in both is evaluated for each indexed row of the source
table.

Each expression must be a value expression and must satisfy the following requirements:

- Field references must be ID references. Named references must not be used.
- The expression must be deterministic.

An expression that consists of a single field reference, or of an `identity` function applied to a single field
reference, is an **identity expression** of the referenced source field.

Expressions are serialized using the [JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by
the expressions specification.

### Index Values

The index values define the content of the index [range files](#range-files). `index-values` is a list of index value
entries and each entry produces one field of the [range schema](#range-schema). Evaluating the list for one indexed row
produces one range file row.

| Requirement | Field      | Type            | Description                                                               |
|-------------|------------|-----------------|---------------------------------------------------------------------------|
| required    | field-id   | int             | Field ID of the [range schema](#range-schema) field produced by the entry |
| required    | expression | JSON expression | Expression producing the value of the field                               |

In addition to the requirements in [Index Expressions](#index-expressions), the entries must satisfy:

- The result type of the expression must be a primitive type. List, map, and struct result types must not be used.
- Each `field-id` must be unique within the list.
- The `field-id` of an identity expression of a source table field must be the field ID of that field.
- The `field-id` of an identity expression of the `_file` (`2147483646`) metadata column must be `2147483546`
  (`file_path`).
- The `field-id` of an identity expression of the `_pos` (`2147483645`) metadata column must be `2147483545` (`pos`).
- The `field-id` of any other expression should not be the field ID of a source table field or of a metadata column, so
  that range schema fields that carry a source table field ID can be recognized.

The index values must allow a reader to identify the range file rows that match a lookup. For each
[index keys](#index-keys) expression, the list must contain an identity expression for every source table field that the
expression references, or, when the expression produces a distinct result for every distinct input, an entry whose
expression is the index key expression. For example, an index keyed on `bucket(256, user_id)` must store `user_id`,
because rows with different `user_id` values can share a bucket, while an index keyed on `user_id` may store the key itself.

### Index Keys

`index-keys` is a list of expressions. The results of the expressions, in list order, form the index key of an indexed
row. Index keys determine the position of an entry in the index, as defined in [Ordering](#ordering).

In addition to the requirements in [Index Expressions](#index-expressions), each index key expression must satisfy:

- The result type must be a primitive type. List, map, and struct result types must not be used.

Examples of index keys, shown as SQL for readability:

| Index keys                          | Ordering                                         |
|-------------------------------------|--------------------------------------------------|
| `[ user_id ]`                       | By the source column values                      |
| `[ bucket(2147483647, user_id) ]`   | By the hash bucket of the source column          |
| `[ bucket(256, user_id), user_id ]` | By hash bucket, then by the source column values |

### Index Definition

An index is defined by a source table, an index type, a list of index values, a list of index key expressions, and
optional index properties. The definition is fixed when the index is created and must not change for the lifetime of
the index, so range files remain readable through every index snapshot that references them. A different definition
requires a new index.

A table may have multiple indexes of the same index type.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field           | Type                  | Description                                                                |
|-------------|-----------------|-----------------------|----------------------------------------------------------------------------|
| required    | format-version  | int                   | Index format version; must be `1`                                          |
| required    | uuid            | string                | Stable UUID assigned at creation                                           |
| required    | table-uuid      | string                | UUID of the indexed table                                                  |
| required    | location        | string                | Index root location                                                        |
| required    | type            | string                | Logical index type                                                         |
| required    | index-values    | list<index-value>     | Index values, see [Index Values](#index-values)                            |
| required    | index-keys      | list<JSON expression> | Expressions producing the index keys, see [Index Keys](#index-keys)        |
| optional    | properties      | map<string,string>    | Index properties applicable for every snapshot                             |
| required    | snapshots       | list<index-snapshot>  | Index snapshots                                                            |
| optional    | encryption-keys | list<encryption-key>  | Encryption keys used by the index, see [Encryption Keys](#encryption-keys) |

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

### Ordering

The index keys, together with the tie-break below, define a total ordering over all index entries of an index snapshot.
Entries must be organized into non-overlapping ranges according to that ordering, and each range must be stored in a
separate range file, so range files inherit the ordering from the ranges they contain.

Index entries are ordered by the [index key](#index-keys) produced for each indexed row. The key is compared by the
index key expressions in list order: entries are compared by the result of the first expression, and the result of the
next expression is used only when the preceding results compare as equal. Each result is ordered ascending.

Primitive values are compared using the rules defined in the
[expressions specification](expressions-spec.md#comparisons), extended so that null and NaN values have a defined
position in the total order:

- `null` values are ordered before all other values (nulls-first)
- `NaN` values are ordered after all other `float` and `double` values

Entries with equal index keys must be ordered by the location of the source row: by the `_file` metadata column,
ascending, and then by the `_pos` metadata column, ascending. The tie-break applies whether or not the source row
location is stored as an index value.

### Tracking File

The tracking file contains metadata of all range files belonging to the index snapshot. It may be stored using any
supported metadata file format.

#### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single range file
tracked by an index snapshot. The fields are the subset of the V4 [data file fields](spec.md#data-file-fields) that are
relevant to planning queries against the index.

Tracking file entries must be stored in the [index ordering](#ordering) of the range files they describe, which is the
ascending order of the index key upper bounds recorded in the [content statistics](#content-statistics).

| Field ID | Name               | Type   | Requirement | Description                                                                                                                      |
|----------|--------------------|--------|-------------|----------------------------------------------------------------------------------------------------------------------------------|
| 100      | file_path          | string | required    | Full URI of the referenced range file.                                                                                           |
| 101      | file_format        | string | required    | File format name, such as parquet, avro, or orc.                                                                                 |
| 103      | record_count       | long   | required    | Number of records contained in the referenced range file.                                                                        |
| 104      | file_size_in_bytes | long   | required    | Total file size in bytes.                                                                                                        |
| 146      | content_stats      | struct | required    | Column statistics on the index values and the index key upper bound of the referenced range file, used for planning and pruning. |
| 131      | key_metadata       | binary | optional    | Implementation-specific key metadata, used for range file encryption.                                                            |

#### Content Statistics

The content statistics structure stored for each range file contains two complementary kinds of statistics:

- **Column statistics** for the index values: the lower and upper bound of each range schema field in the range file,
  following the [content stats](spec.md#content-stats) rules of the table specification. These are required and let
  engines prune on the index values even when searching for partial keys. Statistics must not be stored for the
  `file_path` (`2147483546`) and `pos` (`2147483545`) fields, which record the location of the source row and are not
  used to prune range files.
- **Index key upper bound** for the range file: the index key of the last entry in the range file according to the index
  ordering. Engines that evaluate the index key expressions for a lookup value use these bounds to prune range files
  (see [Ordering](#ordering)).

##### Index Key Upper Bound

The index key upper bound is stored in `content_stats` as a struct with the reserved ID `9800`, and must be present for
every tracking file entry. It contains one field per `index-keys` expression, in list order: the expression at position
`i`, starting at `0`, is stored at ID `9801 + i` using the result type of that expression. All fields are optional, and
a null value means the key component is null. Field names are informational; readers must match fields by ID.

IDs `9800` through `9999` are reserved for this struct.

The stored value must be the exact index key of the last entry in the range file. It must not be truncated or rounded.

### Range Files

Range files contain the index values and represent the lowest level of the index hierarchy.

Range files must be standard Iceberg data files and may be stored using any Iceberg-supported data file format: Parquet,
Avro, or ORC.

Each range file row is the result of evaluating the [index values](#index-values) for one indexed row.
Entries within a range file must be stored in the [index ordering](#ordering).

#### Range Schema

The range schema is constructed from the [index values](#index-values). The result is a struct containing one field for
each index value entry, with fields appearing in the same order as the entries. Each field takes its ID from the
`field-id` of the corresponding entry and its type from the result type of that entry's expression.

Field names in the range schema are generated by the writer and are not defined by this specification. Users of the
index must not rely on them; readers must match range file columns by field ID.

## Future Extensions

Future specifications may define additional index types, for example VECTOR indexes for similarity search or text/term
indexes. Additional ordering strategies do not require changes to this specification and can be added as functions in
the `iceberg_functions` catalog of the [expressions specification](expressions-spec.md), for example a function that
maps multi-column keys to their Hilbert curve position.

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
the index definition open ended, but it also means the definition must survive schema changes and expressions must be
deterministic for the same reason a sort key must be stable: an expression that depends on `random` or on the evaluation
time would place entries at positions that cannot be reproduced, invalidating the index as soon as it is written.

Index values and index keys are lists that generate the individual key and value components, and their order, part of
the index definition, which is what engines need to match an index to a query and to compare index keys component by
component. An engine matches the expressions of a query against the index expressions to decide whether the index
applies and which range schema field holds a value.

### Total Ordering and Pruning

Each index key expression determines part of the position of an entry, so its result type is limited to values that
Iceberg can order. Primitives are ordered by the rules the expressions specification already defines. Multi-component
keys are compared expression by expression, which is an extension of the sort orders in the Iceberg table
specification. Structs, lists, and maps are excluded: lists and maps have no defined ordering in Iceberg, and a struct
key is expressed as separate index key expressions instead.

Index keys alone do not have to be unique. Ordering entries with equal index keys by the location of the source row
makes the ordering total, because a source row is uniquely identified by its file path and position.

The ordering makes the index usable at two levels: range files can be pruned without being opened, and the entries of a
range file that is opened can be located without reading all of it.

Range files hold non-overlapping ranges of the ordering, so the index key statistics in the tracking file are enough to
eliminate a range file. Only the upper bound of a range is stored, because sorted entries in the tracking file and
non-overlapping ranges make the lower bound redundant: it is the upper bound of the preceding entry. Storing one bound
halves the size of the index key statistics.

That bound is a key rather than a column value, so it is stored in a reserved struct of its own instead of through the
column statistics of a range schema field. A key component may be produced by an expression that is not stored as an
index value, and the components have to be compared in list order. The reserved IDs are outside the range the table
specification reserves for [column stats structs](spec.md#field-statistics), so both kinds of statistics fit in one
`content_stats` struct.

The bound has to be exact because readers derive the lower bound of a range file's range from the upper bound of the
preceding entry. A bound rounded up would place that lower bound above entries the next range file actually contains, so
a lookup would prune to the wrong file and miss rows.

Within a range file, the entries that match a lookup are contiguous, so a reader can locate them with the structures the
file format provides for stored columns, such as Parquet page indexes, instead of examining every entry. Those
structures work on a stored column that the ordering keeps sorted, which is a key stored as an index value, or a value
the key is derived from by an order-preserving transformation: a file keyed on `day(ts)` is also sorted by a stored
`ts` column. A key like `bucket(256, user_id)` leaves the stored `user_id` column unsorted, so unless the bucket is
also stored as an index value, a reader has to evaluate the index key expressions over the entries of the file.

### Range Schema Derivation

The range schema is derived from the index values, so the index definition and the physical layout of the index cannot
drift apart and the schema does not have to be maintained as a second, redundant copy of the definition.

Requiring the index values to identify the matching rows is what keeps a range file useful on its own. A transformation
that maps several values to the same result cannot distinguish the entries that share a key, so the values it is
computed from have to be stored. Storing the key results as well is a performance choice, because those are what make a
range file efficient to search, and an engine may decide not to use an index that does not store them.

Field types are not stored in the definition. A field takes the result type of its expression, which is determined when
the expression is bound to the source table schema, so the field follows the source column through type promotion. A
stored type would instead be fixed at index creation and go stale as soon as a source column is promoted.

Fields produced by identity expressions keep the field ID of the source column, which keeps schema evolution, column
renames, and type compatibility semantics consistent between the table and the index. It also allows the index key
expressions, which reference source field IDs, to be evaluated against range file rows without remapping references,
whenever the fields they reference are stored as index values.

Index values are restricted to primitive result types, so one field ID per entry fully describes the range schema. A
nested result type would also require IDs for every field of its subtree.

The source row location is stored under the field IDs, `file_path` (`2147483546`) and `pos` (`2147483545`), rather than
under `_file` and `_pos`. The `_file` and `_pos` metadata columns describe where the row being read is physically
stored, which for a range file row is the range file itself. The position delete columns are defined to record the
location of a row in another file, which is exactly what a range file row carries.

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
    USING (bucket(256, user_id), user_id);
```

This creates a `SCALAR` index on the `user_id` column that orders entries by the hash bucket of `user_id` and then
by `user_id` itself. When the index is created, the engine (or a later index maintenance job) reads the current table
snapshot, writes the range files and a tracking file, and produces the first index metadata file containing a single
index snapshot. Range file boundaries are created based on the index keys, so a range file holds a contiguous range
of buckets or a range of `user_id` values within a single bucket. The tracking file stores summary information and
pruning statistics.

The index values produce each range file row from `user_id` and the reserved metadata columns that identify the source
row. All three are identity expressions, so their field IDs are the field ID of the source column and the two reserved
IDs, and the range schema is:

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
  "index-values" : [
    { "field-id" : 1, "expression" : { "type" : "reference", "id" : 1 } },
    { "field-id" : 2147483546, "expression" : { "type" : "reference", "id" : 2147483646 } },
    { "field-id" : 2147483545, "expression" : { "type" : "reference", "id" : 2147483645 } }
  ],
  "index-keys" : [ {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
    "arguments" : [ 256, { "type" : "reference", "id" : 1 } ]
  }, {
    "type" : "reference", "id" : 1
  } ],
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

Each entry also carries a `content_stats` struct. `file_path` and `pos` have no statistics, so it holds the `user_id`
bounds and the index key upper bound of each range file. The index keys are `bucket(256, user_id)` and `user_id`, so the
upper bound struct is:

```
9800: required struct index_key_upper_bound {
  9801: optional int bucket;
  9802: optional long user_id;
}
```

| Statistic             | `range-00001.parquet`             | `range-00002.parquet`             |
|-----------------------|-----------------------------------|-----------------------------------|
| `user_id` bounds      | `12094` .. `84721`                | `3277` .. `99182`                 |
| index key upper bound | `{ bucket: 88, user_id: 55310 }`  | `{ bucket: 209, user_id: 3277 }`  |

These statistics show why the index key upper bound is needed. The `user_id` bounds of the two files overlap, so the
column statistics alone cannot eliminate either file. The index key ranges do not overlap: `range-00002.parquet` is the
second entry of the tracking file, so its range starts after the upper bound of `range-00001.parquet`.

A lookup for `user_id = 55310` evaluates the index key expressions for that value, producing
`{ bucket: 88, user_id: 55310 }`. That key is not greater than the upper bound of `range-00001.parquet`, the first entry
of the tracking file, so only the first range file is read.

The rows of `range-00001.parquet` follow the range schema constructed from the index values, stored in the index
ordering. The index key of each row is not stored; it is shown here to make the ordering visible:

| user_id | file_path                       | pos | (index key)     |
|---------|---------------------------------|-----|-----------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`  |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }` |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }` |

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
must hold non-overlapping ranges of the index ordering, maintenance rewrites that range file as `range-00003.parquet`
with the merged entries. `range-00002.parquet` covers a disjoint range and is reused unchanged, so the tracking file of
`snapshot-id 2` references it as well:

| file_path               | file_format | record_count | file_size_in_bytes |
|-------------------------|-------------|--------------|--------------------|
| .../range-00003.parquet | parquet     | 5            | 1480               |
| .../range-00002.parquet | parquet     | 2            | 1024               |

The merged entries fall inside the range that `range-00001.parquet` already covered, so `range-00003.parquet` keeps the
same `user_id` bounds, `12094` .. `84721`, and the same index key upper bound, `{ bucket: 88, user_id: 55310 }`. The
entry for `range-00002.parquet` is copied from the previous tracking file.

The rows of `range-00003.parquet` interleave the entries of the rewritten range file with the entries added for the new
data file, keeping the index ordering:

| user_id | file_path                       | pos | (index key)     |
|---------|---------------------------------|-----|-----------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`  |
| 71004   | .../data/00002-0-(uuid).parquet | 5   | `{ 17, 71004 }` |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }` |
| 40318   | .../data/00002-0-(uuid).parquet | 22  | `{ 62, 40318 }` |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }` |

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
