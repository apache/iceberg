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
- Provide a framework for defining new index types and layouts

## Overview

An index is recorded in an Index `metadata.json` file which contains the index definition and a set of index snapshots.
Each index snapshot maps to the complete state of an Iceberg table at a given Iceberg table snapshot and references the
index data for that state. The physical organization of the index data is defined by the index layout and varies based
on the type of index. See [Scalar Indexes](#scalar-indexes) for the per-type data organization.

Iceberg standardizes the index lifecycle, discovery model, snapshot relationship, and the minimum metadata needed for
safe cross-engine use. Engines remain free to ignore unsupported indexes, use exact snapshot matches only, or implement
more advanced stale-index and incremental-query logic where the index type permits it.

Like Iceberg tables, views, and functions:

- Index metadata files and index data files are immutable
- Updates create new metadata files
- Catalogs perform atomic metadata swaps

Each index snapshot references the layout-specific index data belonging to the snapshot.

```text
Index Metadata
    |
    +-- Index Snapshot(s)
            |
            +-- Layout-specific index data
```

This structure enables a common lifecycle and discovery model while keeping the physical data layout flexible for
different index implementations.

## Definitions

### Index Type

The index type defines the logical category of an index and the class of queries it is designed to accelerate.

The metadata, snapshot, and lifecycle structures defined in this specification form a generic framework shared by all
index types. Each index type builds on this framework by defining its type-specific details: the query patterns it
supports, the applicable layouts, and the physical organization of the index data.

The following index type is defined in this specification:

| Type   | Description                                                                  |
|--------|------------------------------------------------------------------------------|
| SCALAR | Accelerates point lookups and possibly range filters over scalar key columns |

The following index type is reserved for future specifications.

| Type   | Description                                                        |
|--------|--------------------------------------------------------------------|
| VECTOR | Reserved for accelerating similarity search over vector embeddings |

The index type communicates the capabilities of an index to query engines and helps determine whether an index is
applicable to a particular query.

### Index Layout

The index layout defines the required index data, the meaning of the index properties, and the physical layout or
references needed to read the index data. The available layouts are defined by each index type; the same index type can
be realized with different layouts, and an index instance fixes exactly one layout.

The following layouts are defined in this specification:

| Layout   | Index Type | Description                                                                     |
|----------|------------|---------------------------------------------------------------------------------|
| IDENTITY | SCALAR     | Stores the original key value and sorts entries in ascending, nulls-first order |
| HASH     | SCALAR     | Hashes the key columns into hash buckets                                        |
| HILBERT  | SCALAR     | Clusters multi-column keys by their Hilbert curve position                      |

The layout definitions for each index type are described in [Scalar Indexes](#scalar-indexes).

### Key columns
The source-table columns the layout is applied to and optimized for retrieval.

### Included columns
Optional source-table columns copied into the index for read convenience. They do not affect how the index is organized.

### Index Instance

An index instance is a concrete realization of an index type and layout applied to a specific table.

Users create index instances by specifying:

- Source table
- Index type
- Layout
- Key columns
- Included columns (optional)
- Index properties (optional)

Multiple instances of the same index type may exist for a table.

### Index Snapshot

An index snapshot is an immutable version of the index data generated from a specific table snapshot.

Each index snapshot references a complete set of layout-specific index files and contains all data from the referenced
table snapshot.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field               | Type                     | Description                                                                           |
|-------------|---------------------|--------------------------|---------------------------------------------------------------------------------------|
| required    | format-version      | int                      | Index specification version                                                           |
| required    | uuid                | string                   | Stable UUID assigned at creation                                                      |
| required    | table-uuid          | string                   | UUID of the indexed table                                                             |
| required    | location            | string                   | Index root location                                                                   |
| required    | type                | string                   | Logical index type                                                                    |
| required    | layout              | string                   | Physical organization layout                                                          |
| required    | key-column-ids      | list<int>                | Source-table column IDs the layout is applied to (key columns)                        |
| optional    | included-column-ids | list<int>                | Source-table column IDs copied into the index for read convenience (included columns) |
| optional    | properties          | map<string,string>       | Index properties applicable for every snapshot                                        |
| required    | snapshots           | list<index-snapshot>     | Index snapshots                                                                       |

## Index Snapshot

Every snapshot shares the common fields below and references its layout-specific index data through a single file
location. The interpretation of that file is defined by the layout: scalar layouts reference a
[tracking file](#tracking-file).

| Requirement | Field                    | Type               | Description                                                          |
|-------------|--------------------------|--------------------|----------------------------------------------------------------------|
| required    | snapshot-id              | long               | Index snapshot identifier                                            |
| required    | source-table-snapshot-id | long               | Source table snapshot                                                |
| required    | timestamp-ms             | long               | Snapshot creation timestamp                                          |
| required    | index-data               | string             | Location of the layout-specific root index file                      |
| optional    | properties               | map<string,string> | Snapshot properties specific to this snapshot                        |
| optional    | key-metadata             | binary             | Implementation-specific key metadata, used to encrypt the index data |

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

## Scalar Indexes

A `SCALAR` index accelerates point lookups and, depending on the layout, range filters over one or more scalar key
columns. Its index data is organized as a [tracking file](#tracking-file) (similar to a root manifest file) that lists a
set of [leaf files](#leaf-files) (similar to data files). Leaf files store an ordered set of rows, each containing at
least a key, the path of the Iceberg table data file, and the position within that file where the row for that key is
stored. Each index snapshot references exactly one tracking file through the `index-data` field.

```text
Index Metadata
    |
    +-- Index Snapshot(s)
            |
            +-- Tracking File
                    |
                    +-- Leaf Data Files
```

### Scalar Layouts

Each scalar layout defines a total **ordering** over the key-column values and organizes the entries into
non-overlapping ranges according to that ordering, storing each range in a separate leaf file.

The following layouts are defined for the `SCALAR` index type:

| Layout   | Ordering                                                                                                  |
|----------|-----------------------------------------------------------------------------------------------------------|
| IDENTITY | By the original key values, ascending, nulls-first                                                        |
| HASH     | By the hash bucket of the key columns, then by the original key values, ascending, nulls-first            |
| HILBERT  | By the Hilbert curve position of the key columns, then by the original key values, ascending, nulls-first |

**HASH** follows the 32-bit hash requirements defined in the table specification (see
[Appendix B: 32-bit Hash Requirements](spec.md#appendix-b-32-bit-hash-requirements)). ***TBD***: Specify the hash
function for structs.

**HILBERT** ***TBD***: Specify the Hilbert function exactly.

### Tracking File

Each scalar index snapshot references exactly one tracking file.

It contains metadata of all leaf files belonging to the index snapshot and enables efficient planning
without scanning every leaf file.

The tracking file may be stored using any supported metadata file format.

#### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single leaf file
tracked by an index snapshot. The fields are the subset of the V4 manifest entry fields that are relevant to planning
queries against the index.
Entries contain aggregated statistics for all referenced leaf files, enabling engines to perform pruning and planning
without opening every leaf file.

| Field ID | Name               | Type    | Requirement  | Description                                                                                                           |
|----------|--------------------|---------|--------------|-----------------------------------------------------------------------------------------------------------------------|
| 100      | location           | string  | required     | Location of the referenced file.                                                                                      |
| 101      | file_format        | string  | required     | File format name, such as parquet, avro, or orc.                                                                      |
| 103      | record_count       | long    | required     | Number of records contained in the referenced leaf file.                                                              |
| 104      | file_size_in_bytes | long    | required     | Total file size in bytes.                                                                                             |
| 146      | content_stats      | struct  | required     | Column statistics on the key columns and ordering bounds for the referenced leaf file, used for planning and pruning. |
| 131      | key_metadata       | binary  | optional     | Implementation-specific key metadata, used for leaf file encryption.                                                  |

#### Content Statistics

The content statistics structure stored for each leaf file contains two complementary kinds of statistics:

- **Column statistics** for the key columns: the minimum and maximum original key values in the leaf file, using each
column's natural ordering. These are always present and let engines that do not implement the layout's ordering prune
leaf files using predicates on the original key values.
- **Ordering bounds** for the key columns: the original key values of the first and last entries in the leaf file
according to the layout's ordering. Because leaf files partition the ordered data into non-overlapping ranges, engines
that implement the layout's ordering can use these bounds to prune leaf files (see [Scalar Layouts](#scalar-layouts)).

### Leaf Files

Leaf files contain the actual index entries and represent the lowest level of the index hierarchy.

Leaf files must be standard Iceberg data files and may be stored using any Iceberg-supported file format:
- Parquet
- Avro
- ORC - May be removed if ORC support is deprecated in Iceberg.

The schema of a leaf file is determined by the index definition and contains:
- All key columns defined by the index
- Any included columns defined by the index
- The source file path
- The source row position

Entries within a leaf file are organized by an ascending, nulls-first sort of the key columns, source file path and
source row position. This lets a reader binary-search for a key within the leaf after selecting it from the tracking
file even if the layout's ordering is not known.

#### Leaf Schema

Columns originating from the source table must preserve their original Iceberg field identifiers.
Reusing the original field IDs ensures that schema evolution, column renames, and type compatibility semantics remain
consistent between the table and the index.

The index-specific columns are:

| Field Id  | Column          | Type   | Description                                               |
|-----------|-----------------|--------|-----------------------------------------------------------|
| TBD       | file_path       | string | The path of the source data file the entry references     |
| TBD       | position        | long   | The row position of the entry within the source data file |

### Example: Key Lookup Index

Imagine an `events` table that already has a single snapshot (source table snapshot `3055729675574597004`). To speed up
point lookups on the `user_id` column, a key lookup index is created.

```sql
CREATE INDEX hash_index
    ON events (user_id)
    USING HASH;
```

This creates a `SCALAR` index that applies the `HASH` layout to the `user_id` key column. When the index is created,
the engine (or a later index maintenance job) reads the current table snapshot, writes the leaf files and a tracking
file, and produces the first index metadata file containing a single index snapshot. Leaf file boundaries are created
based on the hash value of the `user_id`, and data inside the leaf files are sorted by `user_id` itself. The tracking file
stores summary information and pruning statistics.

Each leaf file row contains the key column and the location of the source row:

| Column          | Description                                  |
|-----------------|----------------------------------------------|
| user_id         | The indexed key column                       |
| file_path       | The source data file that contains the row   |
| position        | The row position within the source data file |

The JSON metadata file is shown below.

```
s3://bucket/warehouse/default.db/events/index/hash_index/metadata/00001-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/hash_index",
  "type" : "SCALAR",
  "layout" : "HASH",
  "key-column-ids" : [ 1 ],
  "snapshots" : [ {
    "snapshot-id" : 1,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/hash_index/metadata/tracking-00001-(uuid).parquet"
  } ]
}
```

Later, new data is added to the `events` table, producing a new table snapshot (`5459876531255530170`). Index
maintenance runs again and writes new leaf files for the added data, plus a new tracking file that references both the
still-valid old leaf files and the new leaf files.

This produces a new index metadata file that completely replaces the previous one. The old index snapshot (`snapshot-id`
1) is kept alongside the new one (`snapshot-id` 2), so engines can still use the index against the older table snapshot.

```
s3://bucket/warehouse/default.db/events/index/hash_index/metadata/00002-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/hash_index",
  "type" : "SCALAR",
  "layout" : "HASH",
  "key-column-ids" : [ 1 ],
  "snapshots" : [ {
    "snapshot-id" : 1,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/hash_index/metadata/tracking-00001-(uuid).parquet"
  }, {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/hash_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```

Eventually the older table snapshot is no longer needed, so maintenance drops the corresponding index snapshot
(`snapshot-id` 1). It writes a new index metadata file that removes the snapshot from the `snapshots` list and replaces
the previous metadata file. Maintenance then deletes the files referenced only by the removed snapshot: its tracking file
and any leaf files not referenced by a remaining snapshot. Leaf files still referenced by the remaining snapshot are
retained.

```
s3://bucket/warehouse/default.db/events/index/hash_index/metadata/00003-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/hash_index",
  "type" : "SCALAR",
  "layout" : "HASH",
  "key-column-ids" : [ 1 ],
  "snapshots" : [ {
    "snapshot-id" : 2,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "index-data" : "s3://bucket/warehouse/default.db/events/index/hash_index/metadata/tracking-00002-(uuid).parquet"
  } ]
}
```

## Future Extensions

Future specifications may define additional index types and layouts, for example VECTOR indexes for similarity search or
text/term indexes.
