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
- Expose indexes as catalog-managed objects
- Allow indexes to be operated independently from source table metadata
- Enable index sharing across engines
- Provide a framework for defining new index types and transform functions

## Overview

Indexes are stored as a collection of files with some Iceberg table like semantics. At a high level they consist of a tracking file (similar to a root manifest file) which contains listings for a defined set of leaf files (similar to data files.) Leaf files store an ordered set of rows containing at least a key and the path of a Iceberg Table data file and the position within that file where the row where that key is stored. The organization of leaf files is defined by an Index Transform Function which varies based on the type of index. This structure is recorded in an Index metadata.json file which contains a set of snapshots, each of which points to a single tracking file mapping to the complete state of an Iceberg table at a given Iceberg table snapshot.

Like Iceberg tables, views, and functions:

- Metadata files (index metadata and tracking files) and data files (leaf files) are immutable
- Updates create new metadata files
- Catalogs perform atomic metadata swaps

Each index snapshot references a tracking file which describes the leaf files belonging to the snapshot.

```text
Index Metadata
    |
    +-- Index Snapshot
            |
            +-- Tracking File
                    |
                    +-- Leaf Data Files
```

Transform functions derive a transform value from the key columns and determine how index entries are organized within
the leaf files.
- The transform value space is divided into non-overlapping ranges.
- Each leaf file stores entries for a single range.
- The tracking file stores range bounds for each leaf file.

This structure enables efficient planning while keeping the data layout flexible for different index implementations.

## Definitions

### Index Type

The index type defines the logical category of an index and the class of queries it is designed to accelerate.

The metadata, snapshot, tracking-file, and leaf-file structures defined in this specification form a generic framework shared by all index types. Each index type builds on this framework by defining its type-specific details, such as the leaf schema and the applicable transform functions.

The following index type is fully defined in this specification:

| Type   | Description                                                      |
|--------|------------------------------------------------------------------|
| SCALAR | Maps scalar key values to their locations for equality lookups.  |

The following index types are reserved for future specifications. Their identifiers are claimed so that engines and catalog implementations recognize them as valid type names and handle them gracefully, but this specification defines no type-specific requirements (leaf schema, transforms, or query semantics) for them:

| Type   | Description                                              |
|--------|----------------------------------------------------------|
| VECTOR | Reserved for similarity search over vector embeddings.   |
| TERM   | Reserved for text/term search.                           |

The index type communicates the capabilities of an index to query engines and helps determine whether an index is
applicable to a particular query.

### Index Transform Function

The index transform function defines how the transform value is derived from the key columns when rows are stored in the
index. The following terms are used throughout this specification:

- **Key columns**: the source-table columns the transform function is applied to.
- **Transform value**: the value produced by applying the transform function to a row's key columns. Index entries are organized by transform value.
- **Included columns**: optional source-table columns copied into the index for read convenience. They do not affect how the index is organized.

The transform function determines the physical organization of the indexed data and therefore influences which query
patterns can efficiently leverage the index.

The following transform functions are defined in this specification. The bound interpretation describes what the
transform-value bounds stored in the tracking file represent for each transform:

| Transform | Bound Interpretation |
|-----------|----------------------|
| IDENTITY  | Original value range |
| HASH      | Hash bucket range    |
| HILBERT   | Hilbert key range    |

The following transform function is reserved for future specifications:

| Transform | Bound Interpretation      |
|-----------|---------------------------|
| IVF       | Centroid identifier range |

An index type does not fix a single transform function; the same index type can be realized with different transform functions.

### Index Instance

An index instance is a concrete realization of an index type and function applied to a specific table.

Users create index instances by specifying:

- The source table
- The index type
- The transform function
- The key columns
- The included columns (optional)
- Index properties (optional)

Multiple instances of the same index type may exist for a table.

### Index Snapshot

An index snapshot is an immutable version of the index data generated from a specific table snapshot.

Each index snapshot references a complete set of index files and contains all data from the referenced table snapshot.

## Index Metadata

The index metadata file stores the index definition and snapshot history.

### Index Metadata File

| Requirement | Field               | Type                     | Description                                     |
|-------------|---------------------|--------------------------|-------------------------------------------------|
| required    | format-version      | int                      | Index specification version                     |
| required    | uuid                | string                   | Stable UUID assigned at creation                |
| required    | table-uuid          | string                   | UUID of the indexed table                       |
| required    | location            | string                   | Index root location                             |
| required    | type                | string                   | Logical index type                              |
| required    | transform-function  | string                   | Physical organization transform                 |
| required    | key-column-ids      | list<int>                | Source-table column IDs the transform is applied to (key columns) |
| optional    | included-column-ids | list<int>                | Source-table column IDs copied into the index for read convenience (included columns) |
| optional    | properties          | map<string,string>       | Index properties applicable for every snapshot  |
| optional    | current-snapshot-id | long                     | ID of the current index snapshot                |
| required    | snapshots           | list<index-snapshot>     | Index snapshots                                 |

## Index Snapshot

Each index snapshot corresponds to one version of the index data.

| Requirement | Field                    | Type               | Description                                                         |
|-------------|--------------------------|--------------------|---------------------------------------------------------------------|
| required    | snapshot-id              | long               | Index snapshot identifier                                           |
| required    | source-table-snapshot-id | long               | Source table snapshot                                               |
| required    | timestamp-ms             | long               | Snapshot creation timestamp                                         |
| required    | tracking-file            | string             | Tracking file location                                              |
| optional    | properties               | map<string,string> | Snapshot properties specific to this snapshot                       |
| optional    | key-metadata             | binary             | Implementation-specific key metadata, for tracking file encryption. |

## Tracking File

Each index snapshot references exactly one tracking file.

It contains summary metadata about all leaf files belonging to the index snapshot and enables efficient planning
without scanning every leaf file.

The tracking file may be stored using any supported metadata file format.

### Leaf File Entry

Each tracking file contains a collection of leaf file entries. A leaf file entry describes a single leaf file
tracked by an index snapshot. The fields are the subset of the V4 manifest entry fields that are relevant to planning
queries against the index.
Entries contain aggregated statistics for all referenced leaf files, enabling engines to perform pruning and planning
without opening every leaf file.

| Field ID | Name               | Type    | Requirement  | Description                                                                                                  |
|----------|--------------------|---------|--------------|--------------------------------------------------------------------------------------------------------------|
| 100      | location           | string  | required     | Location of the referenced file.                                                                             |
| 101      | file_format        | string  | required     | File format name, such as parquet, avro, or orc.                                                             |
| 103      | record_count       | long    | required     | Number of records contained in the referenced leaf file.                                                     |
| 104      | file_size_in_bytes | long    | required     | Total file size in bytes.                                                                                    |
| 146      | content_stats      | struct  | optional     | Statistics used for planning and pruning, including transform-key statistics and optional column statistics. |
| 131      | key_metadata       | binary  | optional     | Implementation-specific key metadata, used for leaf file encryption.                                         |

### Content Statistics

The content statistics structure contains transform-key statistics and optional column statistics for the referenced
file. The transform-key statistics are always present, while column statistics are optional and may be omitted for
performance reasons.

## Leaf Files

Leaf files contain the actual index entries and represent the lowest level of the index hierarchy.

Leaf files must be standard Iceberg data files and may be stored using any Iceberg-supported file format:
- Parquet
- Avro
- ORC - May be removed if ORC support is deprecated in Iceberg.

The schema of a leaf file is determined by the index definition and contains:
- All key columns defined by the index
- All included columns defined by the index
- The transform value produced by the transform function
- The source file path
- The source row position

Entries within a leaf file are sorted by transform value, then by the key columns. This lets a reader binary-search for a
key within the leaf after selecting it from the tracking file, and groups all entries that share a key.

### Transform value ranges

This section describes how transform values organize leaf files. For the list of transform functions and their bound
interpretations, see [Index Transform Function](#index-transform-function).

The transform function produces a transform value for each indexed row. To enable efficient planning, the transform
value space is divided into non-overlapping ranges. Each leaf file contains entries for a single range, while the
tracking file stores the corresponding bounds for every leaf file. If, and only if, a single transform value produces
more rows than fit in one leaf file, multiple leaf files may be created for that value, and engines must read all of
them.

When a query predicate can be mapped to transform value ranges, engines can use these bounds to prune leaf files that
cannot contain matching entries, avoiding unnecessary reads.

The effectiveness of this pruning depends on the transform and the query pattern. A transform that preserves locality
between the source columns and the resulting transform value enables additional pruning using column statistics stored
in the tracking file. For example, a Hilbert transform can cluster similar multi-column keys together, which can reduce
the number of leaf files read for range scans and partial-key lookups.

### Leaf Schema

Columns originating from the source table must preserve their original Iceberg field identifiers.
Reusing the original field IDs ensures that schema evolution, column renames, and type compatibility semantics remain
consistent between the table and the index.

The index-specific columns are:

| Field Id  | Column          | Type   | Description                                                            |
|-----------|-----------------|--------|------------------------------------------------------------------------|
| TBD       | transform_value | long   | The result of applying the index transform function to the key columns |
| TBD       | file_path       | string | The path of the source data file the entry references                  |
| TBD       | position        | long   | The row position of the entry within the source data file              |

## Example: Key Lookup Index

Index Type:

```text
SCALAR
```

Transform Function:

```text
HASH(primary_key)
```

Leaf Schema:

| Column           |
|------------------|
| primary_key      |
| transform_value  |
| file_path        |
| position         |

The leaf files are organized by hash key, while the tracking file stores summary information and pruning statistics.

## Snapshot Evolution

Index snapshots are immutable.

Updating an index creates:

1. New leaf files
2. A new tracking file pointing to new leaf files, and potentially some old leaf files that are still valid for the new snapshot.
3. A new index metadata file

The catalog commits the update by atomically replacing the metadata location.

## Maintenance

An index maintains a mapping between source table snapshots and index snapshots.

Engines may use this mapping to determine whether a compatible index snapshot exists for a given table snapshot.

## Future Extensions

Future specifications may define:

- VECTOR indexes
- TERM indexes