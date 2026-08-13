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

## Background and Motivation

Indexes enable query engines to locate relevant rows without scanning entire datasets.
They can accelerate point lookups, range predicates, and other retrieval patterns
while preserving Iceberg's table format, snapshot isolation, and interoperability.

Indexes are optional. Engines may choose to create, maintain, consume, or ignore them.

## Goals

- Define a portable metadata format for indexes
- Provide a common storage architecture for index data
- Allow indexes to be operated independently of source table metadata
- Enable index sharing across engines
- Provide a framework for defining new index types

## Overview

An index is recorded in an Index `metadata.json` file which contains the index definition and a set of index snapshots.
Each index snapshot maps to the complete state of an Iceberg table at a given Iceberg table snapshot and references the
index data for that state.

Iceberg standardizes the index lifecycle, discovery model, snapshot relationship, and the minimum metadata needed for
safe cross-engine use. Engines remain free to ignore unsupported indexes, use exact snapshot matches only, or implement
more advanced stale-index and incremental-query logic.

Like Iceberg tables, views, and functions:

- Index metadata files and index data files are immutable
- Updates create new metadata files
- Catalogs perform atomic metadata swaps

The index data of a snapshot is organized as a [tracking file](#tracking-file) (similar to a root manifest file) that
lists a set of [leaf files](#leaf-files) (similar to data files):

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

The index type defines the logical category of an index and the class of queries it is designed to accelerate. It
communicates the capabilities of an index to query engines and helps determine whether an index is applicable to a
particular query.

The following index type is defined in this specification:

| Type   | Description                                                           |
|--------|-----------------------------------------------------------------------|
| SCALAR | Accelerates point lookups and possibly range filters over index keys |

The following index type is reserved for future specifications.

| Type   | Description                                                        |
|--------|--------------------------------------------------------------------|
| VECTOR | Reserved for accelerating similarity search over vector embeddings |

### Index Values

The index values are produced by an [Iceberg expression](expressions-spec.md) that is evaluated for each indexed row.
The index values expression defines the content of the index [leaf files](#leaf-files): each evaluation produces one
leaf file row.

The index values expression must be a value expression and must satisfy the following requirements:

- The result type must be a struct and must be declared on the expression. The declared struct type is the
  [leaf schema](#leaf-schema).
- Field references must be ID references. Named references are not allowed because the expression is stored and must
  remain valid when columns are renamed.
- Functions must be resolved in the `iceberg_functions` or `sql_functions` reserved catalogs. User-defined functions
  and engine-specific function catalogs are not allowed because index values must be portable across engines.
- The expression must be deterministic. Non-deterministic functions, such as `random` or functions that depend on the
  evaluation time, would invalidate the index as soon as it is written.
- The result must locate the indexed row in the source table. It must include the `source_file_path` and
  `source_file_pos` fields defined by the [leaf schema](#leaf-schema), populated from the `_file` and `_pos` metadata
  columns of the source table.

The index values expression is serialized using the
[JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by the expressions specification.

### Index Keys

The index keys are produced by an [Iceberg expression](expressions-spec.md) that is evaluated for each indexed row. The
resulting index key determines the position of the entry in the index, as described in [Ordering](#ordering).

The index keys expression must be a value expression and must satisfy the following requirements:

- Field references must be ID references. Named references are not allowed because the expression is stored and must
  remain valid when columns are renamed.
- Functions must be resolved in the `iceberg_functions` or `sql_functions` reserved catalogs. User-defined functions
  and engine-specific function catalogs are not allowed because index keys must be portable across engines.
- The expression must be deterministic. Non-deterministic functions, such as `random` or functions that depend on the
  evaluation time, would invalidate the index as soon as it is written.
- The result type must be a primitive type or a struct whose fields are, recursively, primitives or structs. List and
  map result types are not allowed because no ordering is defined for them.

Examples of valid index keys expressions, shown as SQL for readability:

| Index keys                              | Description                                                    |
|-----------------------------------------|----------------------------------------------------------------|
| `identity(user_id)`                     | Orders entries by the original key values                      |
| `bucket(256, user_id)`                  | Orders entries by the hash bucket of the source column         |
| `struct(bucket(256, user_id), user_id)` | Orders entries by hash bucket, then by the original key values |

The index keys expression is serialized using the
[JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by the expressions specification.

### Index Instance

An index instance is a concrete realization of an index type applied to a specific table.

Users create index instances by specifying:

- Source table
- Index type
- Index values
- Index keys
- Index properties (optional)

Multiple instances of the same index type may exist for a table.

### Index Snapshot

An index snapshot is an immutable version of the index data generated from a specific table snapshot.

Each index snapshot references a complete set of index files and contains all data from the referenced
table snapshot.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field          | Type                 | Description                                                                |
|-------------|----------------|----------------------|----------------------------------------------------------------------------|
| required    | format-version | int                  | Index specification version                                                |
| required    | uuid           | string               | Stable UUID assigned at creation                                           |
| required    | table-uuid     | string               | UUID of the indexed table                                                  |
| required    | location       | string               | Index root location                                                        |
| required    | type           | string               | Logical index type                                                         |
| required    | index-values   | JSON expression      | Expression producing the index values, see [Index Values](#index-values)   |
| required    | index-keys     | JSON expression      | Expression producing the index keys, see [Index Keys](#index-keys)         |
| optional    | properties     | map<string,string>   | Index properties applicable for every snapshot                             |
| required    | snapshots      | list<index-snapshot> | Index snapshots                                                            |
| optional    | encryption-keys| list<encryption-key> | Encryption keys used by the index, see [Encryption Keys](#encryption-keys) |

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

## Index Snapshot

Every snapshot shares the common fields below and references its index data through the location of a single
[tracking file](#tracking-file).

| Requirement | Field                    | Type               | Description                                                           |
|-------------|--------------------------|--------------------|-----------------------------------------------------------------------|
| required    | snapshot-id              | long               | Index snapshot identifier                                             |
| required    | source-table-snapshot-id | long               | Source table snapshot                                                 |
| required    | timestamp-ms             | long               | Snapshot creation timestamp                                           |
| required    | index-data               | string             | Location of the tracking file                                         |
| optional    | properties               | map<string,string> | Snapshot properties specific to this snapshot                         |
| optional    | key-id                   | string             | ID of the encryption key that encrypts the tracking file key metadata |

## Commits and Concurrency

Index metadata is immutable. Every update, whether adding a new snapshot, dropping an old one, or changing index
properties, produces a new index metadata file rather than modifying the existing one. Each new metadata file is written
with a unique name.

A commit replaces the current index metadata file with the new one. This swap is atomic: it succeeds only if the current
metadata file is still the one the writer started from. The check is based on the current metadata file name. If,
between the time a writer reads the metadata and the time it attempts to commit, another process has already committed
a newer metadata file, the expected current file no longer matches and the commit is rejected.

When a commit is rejected because of such a conflict, the writer does not overwrite the newer metadata. Instead, the index
maintenance process decides how to proceed. Depending on the situation it may:
- Re-read the latest committed metadata and retry the update on top of it, or
- Discard the attempted update, for example when the conflicting commit already achieved the intended result.

This prevents concurrent index maintenance commits from silently overwriting each other and losing snapshots.

Whether index maintenance is performed synchronously with the table commit that produces a new source-table snapshot, or
asynchronously by a separate maintenance process, depends on the catalog. A catalog may enforce transactional commits
that atomically update both the table and index versions together, guaranteeing that every committed table snapshot has a
corresponding index snapshot. For other tables or indexes the catalog may allow the index to be updated asynchronously,
in which case the index can temporarily lag behind the table and engines must reconcile the index snapshot against the
source-table snapshot they intend to read.

## Index Data

Each index snapshot references exactly one tracking file through the `index-data` field. The tracking file lists the
leaf files that hold the index entries. Leaf files store an ordered set of rows, each containing at least a key, the
path of the Iceberg table data file, and the position within that file where the row for that key is stored.

### Ordering

The index keys define a total **ordering** over the index entries. Entries are organized into non-overlapping ranges
according to that ordering, and each range is stored in a separate leaf file.

Index entries are ordered by the [index key](#index-keys) produced for each indexed row:

- If the index key is a **primitive** value, entries are ordered by that value, ascending.
- If the index key is a **struct** value, entries are ordered by the struct fields in field order: entries are
  compared by the first field, and the next field is used only when the preceding fields compare as equal. Nested
  structs are compared by applying the same rule recursively.

Primitive values are compared using the rules defined in the
[expressions specification](expressions-spec.md#comparisons), extended so that null and NaN values have a defined
position in the total order:

- `null` values are ordered before all other values (nulls-first)
- `NaN` values are ordered after all other `float` and `double` values

When two entries have equal index keys, they are ordered by the source data file path, ascending, and then by
the source row position within that file, ascending.

### Tracking File

The tracking file contains metadata of all leaf files belonging to the index snapshot and enables efficient planning
without scanning every leaf file.

The tracking file may be stored using any supported metadata file format.

#### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single leaf file
tracked by an index snapshot. The fields are the subset of the V4 manifest entry fields that are relevant to planning
queries against the index.
Entries contain aggregated statistics for all referenced leaf files, enabling engines to perform pruning and planning
without opening every leaf file.

| Field ID | Name               | Type    | Requirement  | Description                                                                                                                 |
|----------|--------------------|---------|--------------|-----------------------------------------------------------------------------------------------------------------------------|
| 100      | location           | string  | required     | Location of the referenced file.                                                                                            |
| 101      | file_format        | string  | required     | File format name, such as parquet, avro, or orc.                                                                            |
| 103      | record_count       | long    | required     | Number of records contained in the referenced leaf file.                                                                    |
| 104      | file_size_in_bytes | long    | required     | Total file size in bytes.                                                                                                   |
| 146      | content_stats      | struct  | required     | Column statistics on the index values and index key statistics for the referenced leaf file, used for planning and pruning. |
| 131      | key_metadata       | binary  | optional     | Implementation-specific key metadata, used for leaf file encryption.                                                        |

#### Content Statistics

The content statistics structure stored for each leaf file contains two complementary kinds of statistics:

- **Column statistics** for the index values: the minimum and maximum value of each leaf schema field in the leaf file,
  using the field's natural ordering. These are always present and let engines prune on the index values even when
  searching for partial keys.
- **Index key statistics** for the leaf file: the index keys of the first and last entries in the leaf file according to
  the index ordering. Because leaf files hold non-overlapping ranges of the index ordering, engines can evaluate the
  index keys expression for a lookup value and use these statistics to prune leaf files (see [Ordering](#ordering)).

### Leaf Files

Leaf files contain the actual index entries and represent the lowest level of the index hierarchy.

Leaf files must be standard Iceberg data files and may be stored using any Iceberg-supported file format:
- Parquet
- Avro
- ORC - May be removed if ORC support is deprecated in Iceberg.

The schema of a leaf file is the struct type declared by the [index values](#index-values) expression. Each leaf file
row is the result of evaluating that expression for one indexed row.

Entries within a leaf file are stored in the [index ordering](#ordering). This lets a reader binary-search for an index
key within the leaf after selecting it from the tracking file.

#### Leaf Schema

The leaf schema is declared as the result type of the index values expression and must satisfy the requirements for
struct types in the [table specification](spec.md#schemas-and-data-types).

Fields that hold a value read directly from the source table should preserve the original Iceberg field identifier.
Reusing the original field IDs ensures that schema evolution, column renames, and type compatibility semantics remain
consistent between the table and the index.

The leaf schema must include fields that locate the indexed row in the source table:

| Field Id   | Column           | Type   | Description                                               |
|------------|------------------|--------|-----------------------------------------------------------|
| 2147483538 | source_file_path | string | The path of the source data file the entry references     |
| 2147483537 | source_file_pos  | long   | The row position of the entry within the source data file |

These fields are populated from the `_file` (`2147483646`) and `_pos` (`2147483645`) metadata columns of the source
table, defined in [Reserved Field IDs](spec.md#reserved-field-ids). They are assigned separate index-specific field IDs
from the same reserved range because they describe the source row rather than the leaf file the entry is stored in.

### Example: Key Lookup Index

Imagine an `events` table that already has a single snapshot (source table snapshot `3055729675574597004`). To speed up
point lookups on the `user_id` column, a key lookup index is created.

```sql
CREATE INDEX bucket_index
    ON events (user_id)
    USING struct(bucket(2147483647, user_id), user_id);
```

This creates a `SCALAR` index on the `user_id` column that orders entries by the hash bucket of `user_id` and then
by `user_id` itself. When the index is created, the engine (or a later index maintenance job) reads the current table
snapshot, writes the leaf files and a tracking file, and produces the first index metadata file containing a single
index snapshot. Leaf file boundaries are created based on the index keys, so a leaf file holds a contiguous range
of buckets or a range of `user_id` values within a single bucket. The tracking file stores summary information and
pruning statistics.

The index values expression produces each leaf file row from `user_id` and the reserved metadata columns that identify
the source row:

| Column           | Description                                  |
|------------------|----------------------------------------------|
| user_id          | The indexed source column                    |
| source_file_path | The source data file that contains the row   |
| source_file_pos  | The row position within the source data file |

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
  "index-values" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [
      { "type" : "reference", "id" : 1 },
      { "type" : "reference", "id" : 2147483646 },
      { "type" : "reference", "id" : 2147483645 }
    ],
    "result-type" : {
      "type" : "struct",
      "fields" : [
        { "id" : 1, "name" : "user_id", "required" : true, "type" : "long" },
        { "id" : 2147483538, "name" : "source_file_path", "required" : true, "type" : "string" },
        { "id" : 2147483537, "name" : "source_file_pos", "required" : true, "type" : "long" }
      ]
    }
  },
  "index-keys" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [ {
      "type" : "apply",
      "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
      "arguments" : [ 2147483647, { "type" : "reference", "id" : 1 } ]
    }, {
      "type" : "reference", "id" : 1
    } ]
  },
  "snapshots" : [ {
    "snapshot-id" : 1,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet"
  } ]
}
```

Later, new data is added to the `events` table, producing a new table snapshot (`5459876531255530170`). Index
maintenance runs again and writes new leaf files for the added data, plus a new tracking file that references both the
still-valid old leaf files and the new leaf files.

This produces a new index metadata file that completely replaces the previous one. The old index snapshot (`snapshot-id`
1) is kept alongside the new one (`snapshot-id` 2), so engines can still use the index against the older table snapshot.

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
  "index-values" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [
      { "type" : "reference", "id" : 1 },
      { "type" : "reference", "id" : 2147483646 },
      { "type" : "reference", "id" : 2147483645 }
    ],
    "result-type" : {
      "type" : "struct",
      "fields" : [
        { "id" : 1, "name" : "user_id", "required" : true, "type" : "long" },
        { "id" : 2147483538, "name" : "source_file_path", "required" : true, "type" : "string" },
        { "id" : 2147483537, "name" : "source_file_pos", "required" : true, "type" : "long" }
      ]
    }
  },
  "index-keys" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [ {
      "type" : "apply",
      "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
      "arguments" : [ 2147483647, { "type" : "reference", "id" : 1 } ]
    }, {
      "type" : "reference", "id" : 1
    } ]
  },
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

Eventually the older table snapshot is no longer needed, so maintenance drops the corresponding index snapshot
(`snapshot-id` 1). It writes a new index metadata file that removes the snapshot from the `snapshots` list and replaces
the previous metadata file. Maintenance then deletes the files referenced only by the removed snapshot: its tracking file
and any leaf files not referenced by a remaining snapshot. Leaf files still referenced by the remaining snapshot are
retained.

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
  "index-values" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [
      { "type" : "reference", "id" : 1 },
      { "type" : "reference", "id" : 2147483646 },
      { "type" : "reference", "id" : 2147483645 }
    ],
    "result-type" : {
      "type" : "struct",
      "fields" : [
        { "id" : 1, "name" : "user_id", "required" : true, "type" : "long" },
        { "id" : 2147483538, "name" : "source_file_path", "required" : true, "type" : "string" },
        { "id" : 2147483537, "name" : "source_file_pos", "required" : true, "type" : "long" }
      ]
    }
  },
  "index-keys" : {
    "type" : "apply",
    "function" : { "catalog" : "iceberg_functions", "identifier" : [ "struct" ] },
    "arguments" : [ {
      "type" : "apply",
      "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
      "arguments" : [ 2147483647, { "type" : "reference", "id" : 1 } ]
    }, {
      "type" : "reference", "id" : 1
    } ]
  },
  "snapshots" : [ {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```

## Future Extensions

Future specifications may define additional index types, for example VECTOR indexes for similarity search or text/term
indexes. Additional ordering strategies do not require changes to this specification and can be added as functions in
the `iceberg_functions` catalog of the [expressions specification](expressions-spec.md), for example a function that
maps multi-column keys to their Hilbert curve position.
