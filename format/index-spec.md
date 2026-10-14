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

An index is a secondary store of data from a table that is structured to accelerate specific access patterns.
An index is derived from the rows of a source table and is stored separately from table data, so it can be built,
refreshed, or dropped without rewriting the table.

An index is most valuable when it is a property of the table rather than of the engine that built it. This
specification defines a common format for index metadata and a common storage architecture for index data, so that any
engine can build an index, maintain it, and use it to plan queries against the table.

## Goals

* **Independence** -- An index is committed as a separate object, without modifying its source table.
* **Consistency** -- An index reflects exactly the live rows of a single state of its source table.
* **Read-optimized** -- An index is structured for fast reads, at the cost of extra work when writing.
* **Incrementality** -- An index is refreshed by writing only the index data affected by the table's changes.
* **Scalability** -- An index supports any table size that the table spec supports.
* **Portability** -- An index is readable and maintainable by any engine, not only the one that wrote it.
* **Extensibility** -- An index type or ordering strategy can be added without disrupting existing indexes or
  engines that do not implement it.

## Overview

Index state is maintained in index metadata files. All changes to index state create a new metadata file and replace
the old metadata with an atomic swap, as defined in [Commits and Concurrency](#commits-and-concurrency). An index
metadata file tracks the index definition, index properties, and extracts of a single index. A table may have
any number of indexes, including multiple indexes of the same type.

An extract represents the state of an index for one snapshot of its source table and is used to access the
complete set of index data files for that state. The index data of an extract is organized as a
[tracking file](#tracking-file) that lists a set of [region files](#region-files). Index data files are immutable and
may be referenced by more than one extract.

## Specification

### Terms

* **Index** -- A structure that accelerates retrieval of rows from a source table.
* **Extract** -- The state of an index for a single snapshot of the source table.
* **Index entry** -- The values produced by the index fields for one indexed row of the source table.
* **Ordering key** -- The tuple of values that determines the position of an index entry within an extract.
* **Tracking file** -- A file that lists the region files of an extract; one per extract.
* **Region file** -- A file that stores the index entries for a range of ordering keys; a subset of an extract.

### Locations in Metadata

Location strings stored in index metadata are classified and resolved as defined by
[paths in metadata](spec.md#paths-in-metadata) in the table specification. Relative locations are resolved against the
index `location`, which must be an absolute location.

### Index Definition

An index is defined by a source table, an index type, an [extract schema](#extract-schema), and an
[ordering key](#ordering-key). The definition is fixed when the index is created so that region files remain readable
through every extract that references them. Only extract schema field types and optionality may change, as defined in
[Extract Schema Evolution](#extract-schema-evolution). Any other change requires a new index.

Index properties configure how an index is written and maintained, such as the target size of region files. Any commit
may change properties, and readers must not depend on them.

#### Index Type

The index type defines the logical category of an index and the class of queries it accelerates.

| Type     | Description                                                                                                            |
|----------|------------------------------------------------------------------------------------------------------------------------|
| `scalar` | Accelerates point lookups on ordering key fields, and range filters when the ordering expressions are order preserving |

This specification defines a single index type, `scalar`. Future specifications may define additional types.

Writers must write `index-type` in lower case. Readers must match it case-insensitively.

#### Extract Schema

The extract schema is an Iceberg [schema](spec.md#schemas-and-data-types) and follows the same rules as a table schema.
It defines the fields of an index entry and is used to read region files. It contains:

* Identity columns, which store a value of the indexed row as is. [`identity-fields`](#identity-fields) maps each
  source field to its identity column. A source field is a source table field or a
  [metadata column](spec.md#reserved-field-ids), except `_deleted`, `_partition`, the changelog columns
  (`_change_type`, `_change_ordinal`, `_commit_snapshot_id`), and the delete file columns (`file_path`, `pos`, `row`).
* [Derived columns](https://docs.google.com/document/d/1u93kyIrTc8bt9VSTB4O0GM9S7zuXekqixJR6Id2FZC4), which are
  computed only from identity columns by a deterministic expression. `generated-always` columns are stored in region
  files. `virtual` columns are not stored; only their statistics are kept in
  [tracking file entries](#tracking-file-entry).

Extract schemas are stored in `schemas`, and each extract references one by `extract-schema-id`. Each identity column
must have the type of its source field: for a table field, as defined by the schema of the extract's source table
snapshot, which must have a `schema-id`; for a metadata column, as defined by the table specification. An identity
column must be optional if its source field is optional. If no existing extract schema matches, a new one is added.
If the source field of an identity column is dropped, no new extracts can be created.

##### Extract Schema Evolution

Extract schemas of an index may differ only in field types and optionality. Field IDs, names, order, nesting, and
derived column definitions must be the same in every extract schema. Compared to the most recently added extract
schema, each field in a new extract schema must have the same type or a valid
[type promotion](spec.md#schema-evolution), and may change from required to optional but not from optional to required.

Each extract schema only widens all earlier ones, so a region file can be read with the schema it was written with
or any schema added later. An extract must not reuse a region file written with an extract schema added after its own.
For `ordering-key` fields, a promoted type must also order all values of the original type identically.

#### Ordering Key

`ordering-key` is a non-empty list of extract schema field IDs, each of a primitive type. The values of the referenced
fields, in list order, form the ordering key of an indexed row and determine the row's position in the index, as
defined in [Ordering](#ordering).

### Index Metadata

The index metadata file stores the index definition and extract history. It is encoded as JSON.

#### Index Metadata File

The index metadata file has the following fields:

| Requirement | Field name                | Type                       | Description                                                                     |
|-------------|---------------------------|----------------------------|---------------------------------------------------------------------------------|
| _required_  | `format-version`          | `int`                      | Index format version; must be `1`                                               |
| _required_  | `index-uuid`              | `string`                   | Stable UUID assigned at creation                                                |
| _required_  | `table-uuid`              | `string`                   | UUID of the indexed table                                                       |
| _required_  | `location`                | `string`                   | Index root location                                                             |
| _required_  | `last-updated-ms`         | `long`                     | Timestamp when the index was last updated (ms from epoch) [1]                   |
| _required_  | `index-type`              | `string`                   | Logical index type                                                              |
| _required_  | `identity-fields`         | `list<identity-field>`     | Identity columns of the extract schema, see [Identity Fields](#identity-fields) |
| _required_  | `schemas`                 | `list<schema>`             | Extract schemas, see [Extract Schema](#extract-schema)                          |
| _required_  | `ordering-key`            | `list<int>`                | Field IDs that form the ordering key, see [Ordering Key](#ordering-key)         |
| _optional_  | `properties`              | `map<string, string>`      | Index properties applicable for every extract                                   |
| _optional_  | `extracts`                | `list<extract>`            | Extracts [2]                                                                    |
| _optional_  | `metadata-log`            | `list<metadata-log-entry>` | Previous index metadata files, see [Metadata Log](#metadata-log)                |
| _optional_  | `encryption-keys`         | `list<encryption-key>`     | Encryption keys used by the index, see [Encryption Keys](#encryption-keys)      |

A missing optional list must be read as an empty list.

Notes:

1. Each index metadata file should update `last-updated-ms` just before writing.
2. An index that has not been built yet has no extracts.
3. Index names are not stored in index metadata. It is the catalog's responsibility to map index names to metadata file
   locations.
4. How the indexes of a table are discovered is out of scope for this specification and is defined by the catalog
   specification.

#### Identity Fields

Each entry in `identity-fields` maps a source field to its identity column in the [extract schema](#extract-schema),
like a partition field maps a source column to a partition field.

| Requirement | Field name  | Type  | Description                                     |
|-------------|-------------|-------|-------------------------------------------------|
| _required_  | `source-id` | `int` | ID of the source table field or metadata column |
| _required_  | `field-id`  | `int` | Extract schema field ID of the identity column  |

Each `source-id` and each `field-id` must be unique within `identity-fields`.

#### Extract

An extract is an immutable version of the index data generated from a specific source table snapshot. It
references a complete set of index files through the location of a single [tracking file](#tracking-file).

An extract must index exactly the live rows of the referenced table snapshot.

| Requirement | Field name                    | Type                  | Description                                                        |
|-------------|-------------------------------|-----------------------|--------------------------------------------------------------------|
| _required_  | `extract-id`                  | `long`                | Extract identifier                                                 |
| _required_  | `source-table-snapshot-id`    | `long`                | Source table snapshot                                              |
| _required_  | `extract-schema-id`           | `int`                 | ID of the [extract schema](#extract-schema) in `schemas`           |
| _required_  | `timestamp-ms`                | `long`                | Timestamp when the extract was created (ms from epoch)             |
| _required_  | `tracking-file`               | `string`              | Location of the tracking file                                      |
| _required_  | `tracking-file-size-in-bytes` | `long`                | Total size of the tracking file in bytes                           |
| _optional_  | `properties`                  | `map<string, string>` | Extract properties specific to this extract                        |
| _optional_  | `key-id`                      | `string`              | ID of the encryption key that holds the tracking file key metadata |

Each `extract-id` must be unique within the `extracts` list. Engines locate index data by matching
`source-table-snapshot-id`. More than one extract may reference the same source table snapshot, and an engine may
use any of the matching extracts.

#### Metadata Log

`metadata-log` records the index metadata files that preceded the current one. A commit should append an entry for the
metadata file it replaces. The number of entries to retain is controlled by the index property
`write.metadata.previous-versions-max`, and a commit drops the oldest entries beyond that limit. When
`write.metadata.delete-after-commit.enabled` is true, a commit also deletes the dropped metadata files.

| Requirement | Field name      | Type     | Description                                                     |
|-------------|-----------------|----------|-----------------------------------------------------------------|
| _required_  | `metadata-file` | `string` | Location of the index metadata file                             |
| _required_  | `timestamp-ms`  | `long`   | `last-updated-ms` of the index metadata file at `metadata-file` |

#### Encryption Keys

An index must not store indexed values with weaker protection than its source table. If the source table snapshot that
an extract indexes is encrypted, indicated by the snapshot's `key-id` as defined by the table specification, the
tracking file and the region files of that extract must be encrypted.

Index metadata is not encrypted, so keys are never stored in plain form. Keys used for index encryption are tracked in
index metadata as a list named `encryption-keys`, using the [encryption keys](spec.md#encryption-keys) structure defined
by the table specification. The format of encrypted key metadata is determined by the index's encryption scheme and can
be a wrapped format specific to the KMS provider.

The `key-id` of an extract must reference a `key-id` in the index metadata `encryption-keys` list. The
`encrypted-key-metadata` of the referenced entry is the key metadata of the extract's tracking file, which in turn
holds the key metadata of the region files.

### Commits and Concurrency

Index metadata is immutable. Every update, whether adding an extract, dropping an extract, or changing index properties,
must produce a new index metadata file with a unique name.

A commit replaces the current index metadata file with the new one. The swap must be atomic and must succeed only if the
current metadata file is still the file the writer started from, identified by name. If a newer metadata file has been
committed since the writer read the metadata, the commit must be rejected.

A writer whose commit is rejected must not overwrite the newer metadata. It may re-read the latest committed metadata
and retry the update on top of it, or discard the attempted update.

Index maintenance may be performed synchronously with the table commit that produces a new source-table snapshot, or
asynchronously by a separate maintenance process. A catalog may enforce transactional commits that atomically update
both the table and the index, guaranteeing that every committed table snapshot has a corresponding extract. When
an index is updated asynchronously, the index may lag behind the table and engines must reconcile the extract
against the source-table snapshot they intend to read.

### Index Data

#### Ordering

The ordering key defines an ordering over all index entries of an extract. Index entries must be partitioned
into ranges of ordering key values that do not overlap, and each range must be stored in a separate region file. A
region boundary must fall at a change in ordering key, so all index entries that share an ordering key are stored in
the same region file.

Index entries are ordered by the [ordering key](#ordering-key) produced for each indexed row. The key is compared by
the fields in `ordering-key` order: index entries are compared by the value of the first field, and the next field is
used only when the preceding values compare as equal. Each field is ordered ascending.

Primitive values are compared using the rules defined in the
[expressions specification](expressions-spec.md#comparisons), extended so that null and NaN values have a defined
position in the ordering:

- `null` values are ordered before all other values (nulls-first)
- `float` and `double` values are ordered `-NaN` < `-Infinity` < `-value` < `-0.0` < `0.0` < `value` < `Infinity` <
  `NaN`, as defined by [sorting](spec.md#sorting) in the table specification

#### Tracking File

The tracking file contains metadata of all region files belonging to the extract. It may be stored using any
supported metadata file format.

##### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single region file
tracked by an extract. The fields are the subset of the V4 [data file fields](spec.md#data-file-fields) that are
relevant to planning queries against the index, plus the index-specific `max_ordering_key`, which reuses the ID of
the data file `partition` tuple.

Tracking file entries must be stored in the [index order](#ordering) of the region files they describe, which is the
ascending order of their [`max_ordering_key`](#max-ordering-key).

| Requirement | Field id, name                | Type     | Description                                                                                        |
|-------------|-------------------------------|----------|----------------------------------------------------------------------------------------------------|
| _required_  | **`100  file_path`**          | `string` | Full URI of the referenced region file                                                             |
| _required_  | **`101  file_format`**        | `string` | File format name, such as `parquet`, `avro`, or `orc`                                              |
| _required_  | **`102  max_ordering_key`**   | `struct` | Ordering key of the last index entry in the region file, see [Max Ordering Key](#max-ordering-key) |
| _required_  | **`103  record_count`**       | `long`   | Number of records contained in the referenced region file                                          |
| _required_  | **`104  file_size_in_bytes`** | `long`   | Total file size in bytes                                                                           |
| _optional_  | **`146  content_stats`**      | `struct` | Field statistics for the referenced region file, see [content stats](spec.md#content-stats)        |
| _optional_  | **`131  key_metadata`**       | `binary` | Implementation-specific key metadata, used for region file encryption                              |

##### Max Ordering Key

`max_ordering_key` is a struct with one field for each `ordering-key` field, in `ordering-key` order. The field at
position `i`, starting at `0`, has ID `1000 + i` and the name and type of the referenced field in the extract's schema.
It is optional so that it can represent a null ordering key value.

`max_ordering_key` must be the exact ordering key of the last index entry in the region file according to the
[index order](#ordering). It must not be truncated or rounded. Readers use it as an inclusive region file upper bound.
The ordering keys of a region file are strictly greater than the `max_ordering_key` of the preceding tracking file
entry, so tracking file entries must be read in order.

#### Region Files

Region files must be valid Iceberg data files stored in Parquet, Avro, or ORC, following the
[format-specific requirements](spec.md#appendix-a-format-specific-requirements) of the table specification. Those
requirements define how each type is encoded and where a column's field ID is recorded in the file.

Each region file row is one index entry, written with the extract schema. Index entries within a region file must be
stored in the [index order](#ordering). Index entries that share an ordering key may be stored in any order.

## Appendix A: Rationale

Iceberg standardizes the index lifecycle, snapshot relationship, and the minimum metadata needed for safe cross-engine
use. Beyond that minimum, engines remain free to ignore unsupported indexes, use exact snapshot matches only, or
implement more advanced stale-index and incremental-query logic. The index type is a first filter: it identifies the
class of index, so an engine can skip a type it does not implement without inspecting the definition.

### Identity and Derived Columns

Identity columns store values of the indexed row, and derived columns compute everything else from them. Derived
expressions must be deterministic, otherwise entries could not be placed at reproducible positions. Computing them only
from identity columns keeps a region file self-contained: a reader can recompute `virtual` columns, and entries that
share an ordering key can be told apart by its inputs. Ordering by `bucket(256, user_id)`, for example, needs `user_id`
stored, because different `user_id` values can share a bucket. A `generated-always` column trades storage for faster
search.

Metadata columns that do not describe a live row of one snapshot are excluded: `_deleted` is the same for every indexed
row, the changelog columns describe a change between snapshots, and the delete file columns describe delete records.
`_partition` is excluded because its type changes when the partition spec evolves.

`identity-fields` gives the extract schema its own field ID space, so IDs can be assigned freely without clashing with
IDs the source table assigns as it evolves, or with reserved metadata column IDs. For example, a reader synthesizes
`_pos` from the row position in the region file, so storing the source `_pos` under its own ID would collide.

### Ordering and Pruning

Each field in the ordering key determines part of the position of an entry, so its result type is limited to values
that Iceberg can order. Primitives are ordered by the rules the expressions specification already defines.
Multi-component ordering keys are compared field by field, which is an extension of the sort orders in the Iceberg
table specification. Structs, lists, and maps are excluded because Iceberg does not define ordering for lists and maps,
and a struct is represented as separate index fields instead.

Ordering keys do not have to be unique. Region boundaries fall only where the ordering key changes, so all entries
that share a key are in one region file and a lookup resolves to a single region file. Because a region file holds every
entry with a given ordering key, the order of those entries within the file has no effect on planning or on region
file bounds, and the specification leaves it to the writer.

The index order makes the index usable at two levels: region files can be pruned without being opened, and the
entries of a region file that is opened can be located without reading all of it.

Region files hold non-overlapping ordering key ranges, so `max_ordering_key` in the tracking file is enough to
eliminate a region file. Only the upper bound of a range is stored: ordered tracking file entries make the lower bound
redundant, because it is exclusive and equal to the upper bound of the preceding entry. The bound is a single key
compared field by field, not a per-field statistic, so it is stored as its own struct rather than in `content_stats`.
It places a region file within the index as a partition tuple places a data file within a table, so it reuses the
`partition` field ID and numbers its fields from `1000` like partition fields. The bound has to be exact, because a
bound rounded up would place the next region file's lower bound above entries that file actually contains, so a
lookup would prune to the wrong file and miss rows.

`max_ordering_key` also supports pruning on a prefix of the ordering key, so `content_stats` is optional. Its bounds
can still eliminate more region files: for a lookup key between the upper bound of the preceding entry and the first
entry of a region file, or for a filter on fields that do not form a prefix of the ordering key.

Within a region file, the entries that match a lookup are contiguous, so a reader can locate them with the structures
the file format provides for stored columns, such as Parquet page indexes, instead of examining every entry. Those
structures work on a stored field that the ordering keeps sorted, or a value from which the ordering expression is
order preserving: a file ordered by `day(ts)` is also ordered by a stored `ts` field. Ordering by
`bucket(256, user_id)` leaves a stored `user_id` field unsorted unless the bucket field is also stored, so a reader may
need to evaluate the ordering expression over region file rows.

### Source Table Schema Evolution

Source table schema evolution only limits which extracts can be created next: a dropped identity source field prevents
new extracts, and a type promotion or a field made optional requires a new extract schema.

Extract schemas may differ only in changes that widen the values a field accepts, so a reused region file is read like
a data file written before a promotion. Any other change would prevent reusing region files and their `content_stats`
across extracts. Making a field required is unsafe, as region files may contain nulls, and never needed, as an
optional identity column can hold the values of a required source field.

A reused region file keeps the order it was written in, so an ordering key promotion must not reorder values. This is
checked on reuse because a source table evolves without knowledge of its indexes, so only the index writer can detect
the case and rewrite the region file.

### Atomic Commits

Index metadata is immutable and committed by an atomic swap, mirroring how Iceberg commits table metadata. Requiring
the current metadata file to be unchanged is what prevents concurrent maintenance processes from silently overwriting
each other and losing extracts.

### Reclaiming Index Files

The `extracts` list of the current index metadata file is the only root for reachability. A tracking file or region
file is live because a listed extract references it. A commit that removes an extract should delete the
files that only that extract referenced.

## Appendix B: Recommendations

### Recording the Source Row Location

This specification does not define how an index entry points back to the row it was built from. The pointer is one or
more identity columns for metadata columns, and indexes could identify a row in different ways. The options below
cover the common cases.

To identify an individual row, add identity columns for the `_file` (`2147483646`) and `_pos` (`2147483645`) metadata
columns, as shown in [Appendix C](#appendix-c-example---key-lookup-index). Every entry then carries the data file and
the row position of its source row, so a reader can fetch matching rows without scanning the table.

For a table with row lineage, an identity column for `_row_id` (`2147483540`) identifies a row with a single column
and keeps the pointer valid when a data file is rewritten, at the cost of resolving the row ID to a location when
reading.

An index that only has to eliminate data files can store `_file` alone. Recording statistics for the `_file` identity
column also lets index maintenance find the entries produced by a data file that has since been rewritten.

An index that stores none of these can still prune region files by ordering key, but it cannot return source rows.

### Choosing Virtual Columns

A `virtual` derived column suits an ordering expression whose result is as large as its input, such as `lower(name)`
over an identity column `name`. Storing the result would nearly double the stored bytes, and a reader can recompute it
from `name` while searching.

### Choosing an Ordering Key

A region file cannot hold fewer entries than a single ordering key produces, because a region boundary falls only
where the ordering key changes. Ordering by a low-cardinality expression alone, such as `bucket(256, user_id)` on a
large table, therefore forces very large region files. Ending `ordering-key` with a high-cardinality field, as in
`[ bucket(256, user_id), user_id ]`, keeps region files bounded.

## Appendix C: Example - Key Lookup Index

Imagine an `events` table that already has a single snapshot (source table snapshot `3055729675574597004`). To speed up
point lookups on the `user_id` column, a key lookup index is created.

```sql
CREATE INDEX bucket_index
    ON events (user_id)
    ORDERED BY (bucket(256, user_id), user_id);
```

This creates a `scalar` index on the `user_id` column that orders entries by the hash bucket of `user_id` and then by
`user_id` itself. When the index is created, the engine (or a later index maintenance job) reads the current table
snapshot, writes the region files and a tracking file, and produces the first index metadata file containing a single
extract. Region file boundaries follow the ordering, so a region file holds a contiguous range of buckets or a
range of `user_id` values within a single bucket. The tracking file describes each region file with its location,
format, record count, size, and the ordering key upper bound used for pruning.

The extract schema has identity columns for `user_id` (source field `1`) and for the `_file` and `_pos` metadata
columns, which record the source row location. `identity-fields` maps them to extract schema field IDs `1`, `2`, and
`3`, so the metadata columns take ordinary IDs instead of their reserved ones. The bucket is a `generated-always`
derived column computed from the `user_id` identity column and stored in region files. The extract schema is:

| Field id, name   | Type     | Column                                     | Description                                |
|------------------|----------|--------------------------------------------|--------------------------------------------|
| **`1  user_id`** | `long`   | Identity of source field `1`               | The indexed source column                  |
| **`2  file`**    | `string` | Identity of `_file`                        | The source data file that contains the row |
| **`3  pos`**     | `long`   | Identity of `_pos`                         | The row position within that data file     |
| **`4  bucket`**  | `int`    | `generated-always`, `bucket(256, user_id)` | The hash bucket of `user_id`               |

The location columns are not part of the ordering key, so entries that fall in the same bucket with the same `user_id`
are ordered by source location only because the writer chose to store them that way.

The JSON metadata file is shown below.

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00001-(uuid).metadata.json
```
```json
{
  "format-version" : 1,
  "index-uuid" : "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "table-uuid" : "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
  "location" : "s3://bucket/warehouse/default.db/events/index/bucket_index",
  "last-updated-ms" : 1573518431292,
  "index-type" : "scalar",
  "identity-fields" : [ {
    "source-id" : 1,
    "field-id" : 1
  }, {
    "source-id" : 2147483646,
    "field-id" : 2
  }, {
    "source-id" : 2147483645,
    "field-id" : 3
  } ],
  "schemas" : [ {
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [ {
      "id" : 1,
      "name" : "user_id",
      "required" : true,
      "type" : "long"
    }, {
      "id" : 2,
      "name" : "file",
      "required" : true,
      "type" : "string"
    }, {
      "id" : 3,
      "name" : "pos",
      "required" : true,
      "type" : "long"
    }, {
      "id" : 4,
      "name" : "bucket",
      "required" : true,
      "type" : "int",
      "derived" : {
        "kind" : "generated-always",
        "expr" : {
          "type" : "apply",
          "function" : { "catalog" : "iceberg_functions", "identifier" : [ "bucket" ] },
          "arguments" : [ 256, { "type" : "reference", "id" : 1 } ]
        }
      }
    } ]
  } ],
  "ordering-key" : [ 4, 1 ],
  "extracts" : [ {
    "extract-id" : 8744736658442914487,
    "source-table-snapshot-id" : 3055729675574597004,
    "extract-schema-id" : 0,
    "timestamp-ms" : 1573518431292,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet",
    "tracking-file-size-in-bytes" : 4382
  } ]
}
```

The tracking file at `tracking-file` lists the region files of this extract. It is stored in a metadata file
format rather than JSON, so its tracking file entries are shown here as a table. `max_ordering_key` has one field per
`ordering-key` field:

```
102: required struct max_ordering_key {
  1000: optional int bucket;
  1001: optional long user_id;
}
```

In this example the extract has two region files, and the writer does not store `content_stats`:

| file_path                | file_format | record_count | file_size_in_bytes | max_ordering_key                 |
|--------------------------|-------------|--------------|--------------------|----------------------------------|
| .../region-00001.parquet | parquet     | 3            | 1160               | `{ bucket: 88, user_id: 55310 }` |
| .../region-00002.parquet | parquet     | 2            | 1024               | `{ bucket: 209, user_id: 3277 }` |

The ordering key ranges of the two files do not overlap: `region-00002.parquet` is the second tracking file entry, so
its range starts after `{ bucket: 88, user_id: 55310 }`, the upper bound of the first entry.

A lookup for `user_id = 55310` evaluates the ordering expressions for that value, producing
`{ bucket: 88, user_id: 55310 }`. That key is not greater than the upper bound of `region-00001.parquet`, the first
tracking file entry, so only the first region file is read.

The rows of `region-00001.parquet` follow the extract schema and are stored in index order:

| user_id | file                            | pos | bucket |
|---------|---------------------------------|-----|--------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | 3      |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | 41     |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | 88     |

Reading the matched row gives the source data file and row position of the indexed row, which the engine uses to read
`user_id = 55310` from the `events` table without scanning it.

Later, new data is added to the `events` table, producing a new table snapshot (`5459876531255530170`). Index
maintenance runs again and writes new region files for the added data, plus a new tracking file that references both the
still-valid old region files and the new region files.

This produces a new index metadata file that completely replaces the previous one. The first extract is kept
alongside the new one, so engines can still use the index against the older table snapshot. The index definition is
unchanged, so it is elided from the new metadata file below:

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00002-(uuid).metadata.json
```
```json
{
  ...
  "last-updated-ms" : 1573518981593,
  "extracts" : [ {
    "extract-id" : 8744736658442914487,
    "source-table-snapshot-id" : 3055729675574597004,
    "extract-schema-id" : 0,
    "timestamp-ms" : 1573518431292,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet",
    "tracking-file-size-in-bytes" : 4382
  }, {
    "extract-id" : 6574117201097113750,
    "source-table-snapshot-id" : 5459876531255530170,
    "extract-schema-id" : 0,
    "timestamp-ms" : 1573518981593,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet",
    "tracking-file-size-in-bytes" : 4417
  } ],
  "metadata-log" : [ {
    "metadata-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00001-(uuid).metadata.json",
    "timestamp-ms" : 1573518431292
  } ]
}
```

The new rows fall into buckets that lie inside the range already covered by `region-00001.parquet`. Because region files
must hold non-overlapping ordering key ranges, maintenance rewrites that region file as `region-00003.parquet`
with the merged entries. `region-00002.parquet` covers a disjoint range and is reused unchanged, so the tracking file of
the second extract references it as well:

| file_path                | file_format | record_count | file_size_in_bytes |
|--------------------------|-------------|--------------|--------------------|
| .../region-00003.parquet | parquet     | 5            | 1480               |
| .../region-00002.parquet | parquet     | 2            | 1024               |

The merged entries fall inside the range that `region-00001.parquet` already covered, so `region-00003.parquet` keeps
the same `max_ordering_key`, `{ bucket: 88, user_id: 55310 }`. The entry for `region-00002.parquet` is copied from
the previous tracking file.

The rows of `region-00003.parquet` interleave the entries of the rewritten region file with the entries added for the
new data file, keeping the index order:

| user_id | file                            | pos | bucket |
|---------|---------------------------------|-----|--------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | 3      |
| 71004   | .../data/00002-0-(uuid).parquet | 5   | 17     |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | 41     |
| 40318   | .../data/00002-0-(uuid).parquet | 22  | 62     |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | 88     |

`region-00001.parquet` is no longer referenced by the second extract, but it is still referenced by the first and
must be retained while that extract exists.

Eventually the older table snapshot is no longer needed, so maintenance drops the first extract. It writes a new
index metadata file that removes the extract from the `extracts` list and replaces the previous metadata file.
Maintenance then deletes the files referenced only by the removed extract: its tracking file,
`tracking-00001-(uuid).parquet`, and `region-00001.parquet`. `region-00002.parquet` and `region-00003.parquet` are
still referenced by the second extract and are retained. The index definition is again elided:

```
s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00003-(uuid).metadata.json
```
```json
{
  ...
  "last-updated-ms" : 1573519505104,
  "extracts" : [ {
    "extract-id" : 6574117201097113750,
    "source-table-snapshot-id" : 5459876531255530170,
    "extract-schema-id" : 0,
    "timestamp-ms" : 1573518981593,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet",
    "tracking-file-size-in-bytes" : 4417
  } ],
  "metadata-log" : [ {
    "metadata-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00001-(uuid).metadata.json",
    "timestamp-ms" : 1573518431292
  }, {
    "metadata-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/00002-(uuid).metadata.json",
    "timestamp-ms" : 1573518981593
  } ]
}
```
