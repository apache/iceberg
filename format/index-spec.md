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

Index metadata files and index data files are immutable, updates create new metadata files, and catalogs perform atomic
metadata swaps.

The index data of a snapshot is organized as a [tracking file](#tracking-file) that lists a set of
[leaf files](#leaf-files):

```text
Index Metadata
    |
    +-- Index Snapshot(s)
            |
            +-- Tracking File
                    |
                    +-- Leaf Data Files
```

## Definitions

### Index Type

The index type defines the logical category of an index and the class of queries it accelerates.

The following index type is defined in this specification:

| Type   | Description                                                          |
|--------|----------------------------------------------------------------------|
| SCALAR | Accelerates point lookups and possibly range filters over index keys |

The following index type is reserved for future specifications:

| Type   | Description                                                        |
|--------|--------------------------------------------------------------------|
| VECTOR | Reserved for accelerating similarity search over vector embeddings |

### Index Expressions

An index is defined by two lists of [Iceberg expressions](expressions-spec.md), the [index values](#index-values) and
the [index keys](#index-keys). Every expression in both lists is evaluated for each indexed row of the source table.

Each expression must be a value expression and must satisfy the following requirements:

- Field references must be ID references. Named references must not be used.
- The expression must be deterministic.

An expression that consists of a single field reference, or of an `identity` function applied to a single field
reference, is an **identity expression** of the referenced source field.

Expressions are serialized using the [JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by
the expressions specification.

### Index Values

The index values define the content of the index [leaf files](#leaf-files). `index-values` is a list of expressions and
each expression produces one field of the [leaf schema](#leaf-schema). Evaluating the list for one indexed row produces
one leaf file row.

The list may contain an identity expression of the `_file` (`2147483646`) metadata column and an identity expression
of the `_pos` (`2147483645`) metadata column, which produce the `file_path` (`2147483546`) and `pos` (`2147483545`)
fields of the [leaf schema](#leaf-schema).

### Index Keys

`index-keys` is a list of expressions and the results of the expressions, in list order, form the index key of the
entry. Index keys determine the position of an entry in the index, as defined in [Ordering](#ordering).

In addition to the requirements in [Index Expressions](#index-expressions), each index keys expression must satisfy:

- The result type must be a primitive type. List, map, and struct result types must not be used.

Examples of index keys, shown as SQL for readability:

| Index keys                          | Ordering                                         |
|-------------------------------------|--------------------------------------------------|
| `[ user_id ]`                       | By the source column values                      |
| `[ bucket(2147483647, user_id) ]`   | By the hash bucket of the source column          |
| `[ bucket(256, user_id), user_id ]` | By hash bucket, then by the source column values |

### Index Instance

An index instance is a concrete realization of an index type applied to a specific table. It is defined by a source
table, an index type, a list of index values expressions, a list of index keys expressions, and optional index
properties.

Multiple instances of the same index type may exist for a table.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field           | Type                  | Description                                                                |
|-------------|-----------------|-----------------------|----------------------------------------------------------------------------|
| required    | format-version  | int                   | Index specification version                                                |
| required    | uuid            | string                | Stable UUID assigned at creation                                           |
| required    | table-uuid      | string                | UUID of the indexed table                                                  |
| required    | location        | string                | Index root location                                                        |
| required    | type            | string                | Logical index type                                                         |
| required    | index-values    | list<JSON expression> | Expressions producing the index values, see [Index Values](#index-values)  |
| required    | index-keys      | list<JSON expression> | Expressions producing the index keys, see [Index Keys](#index-keys)        |
| optional    | properties      | map<string,string>    | Index properties applicable for every snapshot                             |
| required    | snapshots       | list<index-snapshot>  | Index snapshots                                                            |
| optional    | encryption-keys | list<encryption-key>  | Encryption keys used by the index, see [Encryption Keys](#encryption-keys) |

### Index Snapshot

An index snapshot is an immutable version of the index data generated from a specific source table snapshot. It
references a complete set of index files and contains all data from the referenced table snapshot through the location
of a single [tracking file](#tracking-file).

| Requirement | Field                    | Type               | Description                                                           |
|-------------|--------------------------|--------------------|-----------------------------------------------------------------------|
| required    | snapshot-id              | long               | Index snapshot identifier                                             |
| required    | source-table-snapshot-id | long               | Source table snapshot                                                 |
| required    | timestamp-ms             | long               | Snapshot creation timestamp                                           |
| required    | index-data               | string             | Location of the tracking file                                         |
| optional    | properties               | map<string,string> | Snapshot properties specific to this snapshot                         |
| optional    | key-id                   | string             | ID of the encryption key that encrypts the tracking file key metadata |

### Encryption Keys

Index metadata is not encrypted, so keys are never stored in plain form. Keys used for index encryption are tracked in
index metadata as a list named `encryption-keys`, using the same structure as the table specification (see
[Encryption Keys](spec.md#encryption-keys)). The schema of each key is a struct with the following fields:

| Requirement | Field                   | Type                | Description                                                   |
|-------------|-------------------------|---------------------|---------------------------------------------------------------|
| required    | key-id                  | string              | ID of the encryption key                                      |
| required    | encrypted-key-metadata  | string              | Encrypted key and metadata, base64 encoded [1]                |
| optional    | encrypted-by-id         | string              | Optional ID of the key used to encrypt or wrap `key-metadata` |
| optional    | properties              | map<string,string>  | Additional metadata used by the index's encryption scheme     |

Notes:

1. The format of encrypted key metadata is determined by the index's encryption scheme and can be a wrapped format
specific to the KMS provider.

The `key-id` of an index snapshot must reference a `key-id` in the index metadata `encryption-keys` list. The referenced
key encrypts the key metadata of the tracking file, which in turn holds the key metadata of the leaf files.

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

Each index snapshot references exactly one tracking file through the `index-data` field. The tracking file lists the
leaf files that hold the index entries.

### Ordering

The index keys, together with the tie-break below, define a total **ordering** over all index entries of an index
snapshot. Entries must be organized into non-overlapping ranges according to that ordering, and each range must be
stored in a separate leaf file, so leaf files inherit the ordering from the ranges they contain.

Index entries are ordered by the [index key](#index-keys) produced for each indexed row. The key is compared by the
index keys expressions in list order: entries are compared by the result of the first expression, and the result of the
next expression is used only when the preceding results compare as equal. Each result is ordered ascending.

Primitive values are compared using the rules defined in the
[expressions specification](expressions-spec.md#comparisons), extended so that null and NaN values have a defined
position in the total order:

- `null` values are ordered before all other values (nulls-first)
- `NaN` values are ordered after all other `float` and `double` values

Entries with equal index keys must be ordered by `_file`, ascending, and then by `_pos` metadata columns,
ascending.

### Tracking File

The tracking file contains metadata of all leaf files belonging to the index snapshot. It may be stored using any
supported metadata file format.

#### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single leaf file
tracked by an index snapshot. The fields are the subset of the V4 [data file fields](spec.md#data-file-fields) that are
relevant to planning queries against the index.

Tracking file entries must be stored in the [index ordering](#ordering) of the leaf files they describe, which is the
ascending order of the index key upper bounds recorded in the [content statistics](#content-statistics).

| Field ID | Name               | Type    | Requirement  | Description                                                                                                                     |
|----------|--------------------|---------|--------------|---------------------------------------------------------------------------------------------------------------------------------|
| 100      | location           | string  | required     | Location of the referenced file.                                                                                                |
| 101      | file_format        | string  | required     | File format name, such as parquet, avro, or orc.                                                                                |
| 103      | record_count       | long    | required     | Number of records contained in the referenced leaf file.                                                                        |
| 104      | file_size_in_bytes | long    | required     | Total file size in bytes.                                                                                                       |
| 146      | content_stats      | struct  | required     | Column statistics on the index values and the index key upper bound of the referenced leaf file, used for planning and pruning. |
| 131      | key_metadata       | binary  | optional     | Implementation-specific key metadata, used for leaf file encryption.                                                            |

#### Content Statistics

The content statistics structure stored for each leaf file contains two complementary kinds of statistics:

- **Column statistics** for the index values: the minimum and maximum value of each leaf schema field in the leaf file,
  using the field's natural ordering. These are always present and let engines prune on the index values even when
  searching for partial keys.
- **Index key upper bound** for the leaf file: the index key of the max entry in the leaf file according to the index
  ordering. Engines that evaluate the index keys expressions for a lookup value use these bounds to prune leaf files
  (see [Ordering](#ordering)).

### Leaf Files

Leaf files contain the index values and represent the lowest level of the index hierarchy.

Leaf files must be standard Iceberg data files and may be stored using any Iceberg-supported file format:
- Parquet
- Avro
- ORC - May be removed if ORC support is deprecated in Iceberg.

Each leaf file row is the result of evaluating the [index values](#index-values) expressions for one indexed row.
Entries within a leaf file must be stored in the [index ordering](#ordering), which allows an effective search for
an index key within the leaf file.

#### Leaf Schema

The leaf schema is derived from the [index values](#index-values) expressions. It is a struct with one field per index
values expression, in the order the expressions are listed. The type of a leaf schema field is the result type of the
expression that produces it.

The field ID of a leaf schema field is assigned as follows:

- An identity expression of the `_file` (`2147483646`) metadata column produces the `file_path` field (`2147483546`).
- An identity expression of the `_pos` (`2147483645`) metadata column produces the `pos` (`2147483545`) field.
- Any other identity expression preserves the field ID of the referenced source table field, including the IDs of its
  nested fields.
- Any other expression is assigned a generated field ID which should not clash with an existing field ID, or metadata
  field ID, of the source table. Users of the index must not rely on the generated field IDs, which may change when the
  index is rebuilt.

The field name of a leaf schema field that preserves a source table field ID is the name of that field in the current
source table schema. Names of generated fields are not defined by this specification and must not be used to resolve
leaf schema fields; readers must match leaf file columns by field ID.

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

### Index expressions

An index is defined by expressions rather than by a fixed list of columns and a closed set of transforms. This keeps
the index definition open ended, but it also means the definition must survive schema changes and expressions must be
deterministic for the same reason a sort key must be stable: an expression that depends on `random` or on the evaluation
time would place entries at positions that cannot be reproduced, invalidating the index as soon as it is written.

Index values and index keys are lists of expressions. A list generates the individual key and value components, and
their order, part of the index definition, which is what engines need to match an index to a query and to compare index
keys component by component. If an engine does not support a specific expression, it cannot use the index for queries
that require that expression.

### Ordering

Each index keys expression determines part of the position of an entry, so its result type is limited to values that
Iceberg can order. Primitives are ordered by the rules the expressions specification already defines. Multi-component
keys are compared expression by expression, which is an extension of the sort orders in the Iceberg table
specification. Structs, lists, and maps are excluded: lists and maps have no defined ordering in Iceberg, and a struct
key is expressed as separate index keys expressions instead.

Index keys alone do not have to be unique. Ordering entries with equal index keys by the location of the source row
makes the ordering total, because a source row is uniquely identified by its file path and position.

The ordering is what makes the index usable at two levels. Leaf files hold non-overlapping ranges of the ordering, so
the index key statistics in the tracking file are enough to eliminate a leaf file without opening it. Within a leaf
file, entries are stored in the same ordering, so a reader can search for an index key efficiently once the leaf file
has been selected.

Only the upper bound of a leaf file's index key range is stored. Sorted entries in the tracking file and non-overlapping
ranges make the lower bound redundant: it is the upper bound of the preceding entry. Storing one bound halves the size
of the index key statistics.

### Leaf schema

The leaf schema is derived from the index values expressions, so the index definition and the physical layout of the
index cannot drift apart and the schema does not have to be maintained as a second, redundant copy of the definition.

Fields produced by identity expressions keep the field ID of the source column, which keeps schema evolution, column
renames, and type compatibility semantics consistent between the table and the index. Fields produced by any other
expression have no counterpart in the table, so they are assigned IDs.

The source row location is stored under its field IDs reusing delete file `file_name` and `pos` columns, rather than
using `_file` and `_pos`. Those metadata columns point to a physical place and describe the file a row is physically
stored in.

### Commits

Index metadata is immutable and committed by an atomic swap, mirroring how Iceberg commits table metadata. Requiring
the current metadata file to be unchanged is what prevents concurrent maintenance processes from silently overwriting
each other and losing snapshots; a writer that loses the race can retry on top of the newer metadata or discard its
update, but it must not overwrite it.

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
snapshot, writes the leaf files and a tracking file, and produces the first index metadata file containing a single
index snapshot. Leaf file boundaries are created based on the index keys, so a leaf file holds a contiguous range
of buckets or a range of `user_id` values within a single bucket. The tracking file stores summary information and
pruning statistics.

The index values expressions produce each leaf file row from `user_id` and the reserved metadata columns that identify
the source row. All three are identity expressions, so the leaf schema is derived as:

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
    { "type" : "reference", "id" : 1 },
    { "type" : "reference", "id" : 2147483646 },
    { "type" : "reference", "id" : 2147483645 }
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

The tracking file referenced by `index-data` lists the leaf files of this snapshot. It is stored in a metadata file
format rather than JSON, so its entries are shown here as a table. In this example the index snapshot has two leaf
files:

| location               | file_format | record_count | file_size_in_bytes |
|------------------------|-------------|--------------|--------------------|
| .../leaf-00001.parquet | parquet     | 3            | 1160               |
| .../leaf-00002.parquet | parquet     | 2            | 1024               |

Each entry also carries a `content_stats` struct. For `leaf-00001.parquet` the column statistics cover the three leaf
schema fields, and the index key upper bound records the last index key in the file:

| Statistic             | Value                                                                  |
|-----------------------|------------------------------------------------------------------------|
| `user_id` bounds      | `12094` .. `84721`                                                     |
| `file_path` bounds    | `.../data/00000-0-(uuid).parquet` .. `.../data/00001-0-(uuid).parquet` |
| `pos` bounds          | `3` .. `92`                                                            |
| index key upper bound | `{ bucket: 88, user_id: 55310 }`                                       |

The equivalent statistics for `leaf-00002.parquet` show that the two files hold non-overlapping ranges of the index
ordering. `leaf-00002.parquet` is the second entry of the tracking file, so its range starts after the upper bound of
`leaf-00001.parquet`:

| Statistic             | Value                                                                  |
|-----------------------|------------------------------------------------------------------------|
| `user_id` bounds      | `3277` .. `99182`                                                      |
| `file_path` bounds    | `.../data/00000-0-(uuid).parquet` .. `.../data/00001-0-(uuid).parquet` |
| `pos` bounds          | `7` .. `41`                                                            |
| index key upper bound | `{ bucket: 209, user_id: 3277 }`                                       |

A lookup for `user_id = 55310` evaluates the index keys expressions for that value, producing
`{ bucket: 88, user_id: 55310 }`. That key is not greater than the upper bound of `leaf-00001.parquet`, the first entry
of the tracking file, so only the first leaf file is read.

The rows of `leaf-00001.parquet` follow the leaf schema derived from the index values expressions, stored in the index
ordering. The index key of each row is not stored; it is shown here to make the ordering visible:

| user_id | source_file_path                 | source_file_pos | (index key)     |
|---------|----------------------------------|-----------------|-----------------|
| 84721   | .../data/00000-0-(uuid).parquet  | 14              | `{ 3, 84721 }`  |
| 12094   | .../data/00001-0-(uuid).parquet  | 3               | `{ 41, 12094 }` |
| 55310   | .../data/00000-0-(uuid).parquet  | 92              | `{ 88, 55310 }` |

Reading the matched row gives the source data file and row position of the indexed row, which the engine uses to read
`user_id = 55310` from the `events` table without scanning it.

Later, new data is added to the `events` table, producing a new table snapshot (`5459876531255530170`). Index
maintenance runs again and writes new leaf files for the added data, plus a new tracking file that references both the
still-valid old leaf files and the new leaf files.

This produces a new index metadata file that completely replaces the previous one. The old index snapshot
(`snapshot-id 1`) is kept alongside the new one (`snapshot-id 2`), so engines can still use the index against the older
table snapshot.

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00002-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/bucket_index",
  "type" : "SCALAR",
  "index-values" : [
    { "type" : "reference", "id" : 1 },
    { "type" : "reference", "id" : 2147483646 },
    { "type" : "reference", "id" : 2147483645 }
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
  }, {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```

The new rows fall into buckets that lie inside the range already covered by `leaf-00001.parquet`. Because leaf files
must hold non-overlapping ranges of the index ordering, maintenance rewrites that leaf file as `leaf-00003.parquet`
with the merged entries. `leaf-00002.parquet` covers a disjoint range and is reused unchanged, so the tracking file of
`snapshot-id 2` references it as well:

| location               | file_format | record_count | file_size_in_bytes |
|------------------------|-------------|--------------|--------------------|
| .../leaf-00003.parquet | parquet     | 5            | 1480               |
| .../leaf-00002.parquet | parquet     | 2            | 1024               |

The statistics of `leaf-00003.parquet` cover the merged entries, while the entry for `leaf-00002.parquet` is copied
from the previous tracking file:

| Statistic             | Value                                                                  |
|-----------------------|------------------------------------------------------------------------|
| `user_id` bounds      | `12094` .. `84721`                                                     |
| `file_path` bounds    | `.../data/00000-0-(uuid).parquet` .. `.../data/00002-0-(uuid).parquet` |
| `pos` bounds          | `3` .. `92`                                                            |
| index key upper bound | `{ bucket: 88, user_id: 55310 }`                                       |

The rows of `leaf-00003.parquet` interleave the entries of the rewritten leaf file with the entries added for the new
data file, keeping the index ordering:

| user_id | source_file_path                 | source_file_pos | (index key)     |
|---------|----------------------------------|-----------------|-----------------|
| 84721   | .../data/00000-0-(uuid).parquet  | 14              | `{ 3, 84721 }`  |
| 71004   | .../data/00002-0-(uuid).parquet  | 5               | `{ 17, 71004 }` |
| 12094   | .../data/00001-0-(uuid).parquet  | 3               | `{ 41, 12094 }` |
| 40318   | .../data/00002-0-(uuid).parquet  | 22              | `{ 62, 40318 }` |
| 55310   | .../data/00000-0-(uuid).parquet  | 92              | `{ 88, 55310 }` |

`leaf-00001.parquet` is no longer referenced by `snapshot-id 2`, but it is still referenced by `snapshot-id 1` and must
be retained while that snapshot exists.

Eventually the older table snapshot is no longer needed, so maintenance drops the corresponding index snapshot
(`snapshot-id 1`). It writes a new index metadata file that removes the snapshot from the `snapshots` list and replaces
the previous metadata file. Maintenance then deletes the files referenced only by the removed snapshot: its tracking
file, `tracking-00001-(uuid).parquet`, and `leaf-00001.parquet`. `leaf-00002.parquet` and `leaf-00003.parquet` are
still referenced by `snapshot-id 2` and are retained.

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00003-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/bucket_index",
  "type" : "SCALAR",
  "index-values" : [
    { "type" : "reference", "id" : 1 },
    { "type" : "reference", "id" : 2147483646 },
    { "type" : "reference", "id" : 2147483645 }
  ],
  "index-keys" : [ {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
    "arguments" : [ 256, { "type" : "reference", "id" : 1 } ]
  }, {
    "type" : "reference", "id" : 1
  } ],
  "snapshots" : [ {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```
