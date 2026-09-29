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

An index is defined by a source table, an index type, identity fields, materialized fields, non-materialized fields, and
an ordering key. The definition is fixed when the index is created and must not change for the lifetime of the index,
so region files remain readable through every extract that references them. A different definition requires a
new index.

Index properties configure how an index is written and maintained, such as the target size of region files. Any commit
may change properties, and readers must not depend on them.

#### Index Type

The index type defines the logical category of an index and the class of queries it accelerates.

| Type     | Description                                                                                                            |
|----------|------------------------------------------------------------------------------------------------------------------------|
| `scalar` | Accelerates point lookups on ordering key fields, and range filters when the ordering expressions are order preserving |

This specification defines a single index type, `scalar`. Future specifications may define additional types.

Writers must write `index-type` in lower case. Readers must match it case-insensitively.

#### Index Fields

An index field defines one value of an index entry, produced for an indexed row of the source table. An index declares
three lists of index fields:

* [Identity fields](#identity-fields) store a source table field in region files as is, so a reader can return the
  indexed values and distinguish entries that share an ordering key.
* [Materialized fields](#materialized-fields) store a value computed from an indexed row in region files. A value is
  materialized when a reader cannot recompute it from the stored fields, such as the file and position, or when
  recomputing it would cost more than storing it, such as a bucket or Hilbert value.
* [Non-materialized fields](#non-materialized-fields) keep only statistics in
  [tracking file entries](#tracking-file-entry), for a value a reader can recompute from the stored fields, so it can
  take part in ordering and pruning without being stored for every entry.

Every index field has a field ID that must be unique across the three lists.

Every source table field that an index field references must be present in the source schema of an [extract](#extract).
When a referenced field has been dropped, no new extract can be created, but existing extracts remain readable using
their own source schema.

##### Identity Fields

`identity-fields` is a non-empty list of unique source table field IDs. Each entry must reference a data field.
[Metadata columns](spec.md#reserved-field-ids) are not allowed. Each listed field is stored in the
[region files](#region-files) under its own field ID and takes its type from the source schema of an
[extract](#extract).

Every source table field referenced by an expression field in the [ordering key](#ordering-key) must be an identity
field.

##### Expression Fields

Both [materialized fields](#materialized-fields) and [non-materialized fields](#non-materialized-fields) are expression
fields; they differ only in where their values are kept.

The value of an expression field is produced by evaluating an
[Iceberg value expression](expressions-spec.md#value-expressions) for an indexed row of the source table.
An expression field has the following fields:

| Requirement | Field name    | Type              | Description                                                  |
|-------------|---------------|-------------------|--------------------------------------------------------------|
| _required_  | `field-id`    | `int`             | ID that uniquely identifies the index field                  |
| _required_  | `type`        | `expr-value`      | Expression field representation                              |
| _required_  | `data-type`   | Iceberg type      | Type produced by the expression                              |
| _required_  | `expr`        | JSON expression   | Value expression that produces the field, serialized as JSON |

Each expression field must satisfy the following requirements:

- `expr` must contain only ID references to source table fields or
  [metadata columns](spec.md#reserved-field-ids). Named references must not be used. The `_deleted`, `_change_type`,
  `_change_ordinal`, and `_commit_snapshot_id` metadata columns must not be referenced, and neither must the
  `file_path`, `pos`, and `row` columns of delete files.
- `expr` must be deterministic and must produce the declared `data-type`.
- `field-id` must not be a [reserved field ID](spec.md#reserved-field-ids).
- `data-type` must not change. A source table schema change that makes `expr` incompatible with `data-type` requires a
  new index definition and prevents new extracts from being created.

Expressions are serialized using the [JSON serialization](expressions-spec.md#appendix-b-json-serialization) defined by
the expressions specification. Types are serialized using the [type serialization](spec.md#schemas) defined by the table
specification.

###### Materialized Fields

`materialized-fields` is a list of expression fields whose values are stored in the [region files](#region-files).
Evaluating the identity fields and the materialized fields for one indexed row produces one region file row.

###### Non-Materialized Fields

`non-materialized-fields` is a list of expression fields whose row values are not stored in region files. Only their
field statistics are stored, in [tracking file entries](#tracking-file-entry).

#### Ordering Key

`ordering-key` is a list of field IDs from `identity-fields`, `materialized-fields`, and `non-materialized-fields`.
The values of the referenced fields, in list order, form the ordering key of an indexed row and determine the row's
position in the index, as defined in [Ordering](#ordering). The list must not be empty.
Every referenced field must have a primitive type.

### Index Metadata

The index metadata file stores the index definition and extract history. It is encoded as JSON.

#### Index Metadata File

The index metadata file has the following fields:

| Requirement | Field name                | Type                       | Description                                                                                        |
|-------------|---------------------------|----------------------------|----------------------------------------------------------------------------------------------------|
| _required_  | `format-version`          | `int`                      | Index format version; must be `1`                                                                  |
| _required_  | `index-uuid`              | `string`                   | Stable UUID assigned at creation                                                                   |
| _required_  | `table-uuid`              | `string`                   | UUID of the indexed table                                                                          |
| _required_  | `location`                | `string`                   | Index root location                                                                                |
| _required_  | `last-updated-ms`         | `long`                     | Timestamp when the index was last updated (ms from epoch) [1]                                      |
| _required_  | `index-type`              | `string`                   | Logical index type                                                                                 |
| _required_  | `identity-fields`         | `list<int>`                | Source table fields stored in region files, see [Identity Fields](#identity-fields)                |
| _optional_  | `materialized-fields`     | `list<expression-field>`   | Expression fields stored in region files, see [Materialized Fields](#materialized-fields)          |
| _optional_  | `non-materialized-fields` | `list<expression-field>`   | Fields stored only in tracking statistics, see [Non-Materialized Fields](#non-materialized-fields) |
| _required_  | `ordering-key`            | `list<int>`                | Field IDs that form the ordering key, see [Ordering Key](#ordering-key)                            |
| _optional_  | `properties`              | `map<string, string>`      | Index properties applicable for every extract                                                      |
| _optional_  | `extracts`                | `list<extract>`            | Extracts [2]                                                                                       |
| _optional_  | `metadata-log`            | `list<metadata-log-entry>` | Previous index metadata files, see [Metadata Log](#metadata-log)                                   |
| _optional_  | `encryption-keys`         | `list<encryption-key>`     | Encryption keys used by the index, see [Encryption Keys](#encryption-keys)                         |

A missing optional list must be read as an empty list.

Notes:

1. Each index metadata file should update `last-updated-ms` just before writing.
2. An index that has not been built yet has no extracts.
3. Index names are not stored in index metadata. It is the catalog's responsibility to map index names to metadata file
   locations.
4. How the indexes of a table are discovered is out of scope for this specification and is defined by the catalog
   specification.

#### Extract

An extract is an immutable version of the index data generated from a specific source table snapshot. It
references a complete set of index files through the location of a single [tracking file](#tracking-file).

An extract must index exactly the live rows of the referenced table snapshot.

The referenced snapshot must have a `schema-id`. The schema it identifies is the **source schema** of the extract, the
schema that index fields resolve source table fields against.

| Requirement | Field name                 | Type                  | Description                                                                  |
|-------------|----------------------------|-----------------------|------------------------------------------------------------------------------|
| _required_  | `extract-id`               | `long`                | Extract identifier                                                           |
| _required_  | `source-table-snapshot-id` | `long`                | Source table snapshot                                                        |
| _required_  | `timestamp-ms`             | `long`                | Timestamp when the extract was created (ms from epoch)                       |
| _required_  | `tracking-file`            | `string`              | Location of the tracking file                                                |
| _optional_  | `properties`               | `map<string, string>` | Extract properties specific to this extract                                  |
| _optional_  | `key-id`                   | `string`              | ID of the encryption key that holds the tracking file key metadata           |

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

An extract may reuse a region file written for an earlier extract only if the `ordering-key` field types in its
[source schema](#extract) order all values of the earlier types identically.

#### Tracking File

The tracking file contains metadata of all region files belonging to the extract. It may be stored using any
supported metadata file format.

##### Tracking File Entry

Each tracking file contains a collection of tracking file entries. A tracking file entry describes a single region file
tracked by an extract. The fields are the subset of the V4 [data file fields](spec.md#data-file-fields) that are
relevant to planning queries against the index.

Tracking file entries must be stored in the [index order](#ordering) of the region files they
describe, which is the ascending order of the `group_max_value` statistics recorded for the ordering key fields in the
[content statistics](#content-statistics).

| Requirement | Field id, name                | Type      | Description                                                                                            |
|-------------|-------------------------------|-----------|--------------------------------------------------------------------------------------------------------|
| _required_  | **`100  file_path`**          | `string`  | Full URI of the referenced region file                                                                 |
| _required_  | **`101  file_format`**        | `string`  | File format name, such as `parquet`, `avro`, or `orc`                                                  |
| _required_  | **`103  record_count`**       | `long`    | Number of records contained in the referenced region file                                              |
| _required_  | **`104  file_size_in_bytes`** | `long`    | Total file size in bytes                                                                               |
| _required_  | **`146  content_stats`**      | `struct`  | Field statistics and ordering key bounds for the referenced region file, used for planning and pruning |
| _optional_  | **`131  key_metadata`**       | `binary`  | Implementation-specific key metadata, used for region file encryption                                  |

##### Content Statistics

The `content_stats` structure stores field statistics following the [content stats](spec.md#content-stats) rules of the
table specification. Each stored struct derives its ID and metric types from the index field's ID and type and contains
the metrics supported for that type.

The following metrics are required:

| Index field              | Required metrics                                |
|--------------------------|-------------------------------------------------|
| Field in `ordering-key`  | `lower_bound`, `upper_bound`, `group_max_value` |
| Non-materialized field   | `lower_bound`, `upper_bound`                    |
| Other materialized field | None                                            |

All other metrics are optional. Statistics for a non-materialized field describe the rows that the region file indexes,
not values stored in it.

###### Group Max Value

The field statistics struct for each field in `ordering-key` must contain a `group_max_value` metric at offset `8`
from the field's stats `base-id`. It has the index field's data type and is optional so that it can represent a null
ordering key value. Unlike other metrics, a null `group_max_value` is a null ordering key value, not an unknown
statistic.

The `group_max_value` metrics, read in `ordering-key` order, must be the exact ordering key of the last index entry in
the region file according to the [index order](#ordering). They must not be truncated or rounded. Readers use these
keys as inclusive region file upper bounds. The ordering keys of a region file are strictly greater than the
`group_max_value` key of the preceding tracking file entry, so tracking file entries must be read in order.

#### Region Files

Region files must be valid Iceberg data files stored in Parquet, Avro, or ORC, following the
[format-specific requirements](spec.md#appendix-a-format-specific-requirements) of the table specification. Those
requirements define how each type is encoded and where a column's field ID is recorded in the file.

Each region file row is one index entry and holds the [identity field](#identity-fields) and
[materialized field](#materialized-fields) values of one indexed row. Index entries within a region file must be stored
in the [index order](#ordering). Index entries that share an ordering key may be stored in any order.

##### Region Schema

The region schema is constructed from `identity-fields` followed by `materialized-fields`. The result is a struct
containing one field for each index field in those lists, with fields appearing in that order. An identity field takes
its ID and type from the source table field it names; a materialized field takes its ID from `field-id` and its type
from `data-type`.

Names of region schema fields are generated by the writer and are not defined by this specification. Users of the index
must not rely on them; readers must match region file columns by field ID.

## Appendix A: Rationale

Iceberg standardizes the index lifecycle, snapshot relationship, and the minimum metadata needed for safe cross-engine
use. Beyond that minimum, engines remain free to ignore unsupported indexes, use exact snapshot matches only, or
implement more advanced stale-index and incremental-query logic. The index type is a first filter: it identifies the
class of index, so an engine can skip a type it does not implement without inspecting the definition.

### Expression-based Definitions

Beyond the source columns it indexes directly, an index is defined by expressions, which keeps the definition open
ended. Expressions must be deterministic for the same reason ordering must be stable: an expression that depends on
`random` or on the evaluation time would place entries at positions that cannot be reproduced.

Each expression field contains the expression that produces its value. A field that indexes a source table field as is
carries no expression: it is declared by its ID in `identity-fields`. Materialized field values are stored in region
files, while non-materialized field values are represented only by tracking statistics. The ordering key lists field
IDs in comparison order without repeating their expressions. Engines match query expressions to index fields to
determine whether the index applies and which stored field contains a result. Because expressions reference only fields
and metadata columns of the source table, each index field can be evaluated directly from a source row.

That is also why only some metadata columns can be referenced. An extract indexes the live rows of a single
table snapshot, so a row has one position and one file, and the value of a column such as `_deleted` is fixed for every
indexed row. The changelog columns describe a row's change between two snapshots rather than a value within one, and
the delete file columns describe a delete file record rather than a source row, so neither can be evaluated from the
row an index entry is built from.

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

Region files hold non-overlapping ordering key ranges, so the `group_max_value` statistics in the tracking file are
enough to eliminate a region file. Only the upper bound of a range is stored: ordered tracking file entries make the
lower bound redundant, because it is exclusive and equal to the upper bound of the preceding entry. Each component of
the bound is stored in the field statistics of the ordering key field, and the ordering key supplies the component
order. The bound has to be exact, because a bound rounded up would place the next region file's lower bound above
entries that file actually contains, so a lookup would prune to the wrong file and miss rows.

Ordinary lower and upper bounds are required for ordering key fields and non-materialized fields because they support
pruning on partial ordering keys, which the `group_max_value` keys alone cannot do. Bounds for the other fields stored
in region files are optional and, when present, extend pruning to fields outside the ordering key.

Within a region file, the entries that match a lookup are contiguous, so a reader can locate them with the structures
the file format provides for stored columns, such as Parquet page indexes, instead of examining every entry. Those
structures work on a stored field that the ordering keeps sorted, or a value from which the ordering expression is
order preserving: a file ordered by `day(ts)` is also ordered by a stored `ts` field. Ordering by
`bucket(256, user_id)` leaves a stored `user_id` field unsorted unless the bucket field is also stored, so a reader may
need to evaluate the ordering expression over region file rows.

### Region Schema Derivation

The region schema is derived from the identity fields and the materialized fields, so the index definition and the
physical layout of the index cannot drift apart and the schema does not have to be maintained as a second, redundant
copy of the definition.

Requiring the stored fields to identify matching rows is what keeps a region file useful on its own. An ordering key
value alone cannot distinguish the entries that share it, so the source values behind each ordering key field have to
be indexed as identity fields. Ordering by `bucket(256, user_id)`, for example, requires `user_id` to be an identity
field, because rows with different `user_id` values can share a bucket. Storing the expression result as well is a
performance choice, because a reader can search it directly.

The `data-type` declared by an expression field fixes the physical and statistics types for the lifetime of the index.
It also allows a reader to construct those schemas without binding the expression against a possibly evolved source
table schema. Fields outside `ordering-key` may use any Iceberg type, and a nested `data-type` includes IDs for the
fields in its subtree, allowing a covering index to store lists, maps, or structs.

An identity field declares only a source field ID, and its type is resolved from the source table schema rather than
repeated in index metadata, where the two could disagree. Resolving it needs no new rules: a region file stores the
field under the source field ID, so a reader reads it exactly as it reads the same column of a data file, including the
type promotions the table specification allows. Keeping the source field ID also preserves column identity through
renames and makes the relationship between source and stored fields explicit. Expressions reference source field IDs
for the same reason, so they are not rewritten when a column is renamed.

A metadata column cannot be an identity field. Its name and ID are fixed by the table specification, so there is no
column identity to preserve. The region schema is the schema of an Iceberg data file, and metadata column IDs are
reserved. For example, an Iceberg reader synthesizes `_pos` from the position of a row in the file it is reading, which
is the region file rather than the source data file, so storing a region schema field under that ID would collide. A
metadata column is therefore indexed with a materialized field that takes an ordinary field ID, and a reader recognizes
it from its expression.

### Source Table Schema Evolution

An extract resolves types against the schema of the snapshot it indexes, so it is always read with the types it was
written with, and requiring that snapshot to have a `schema-id` makes the resolution total. Source table schema
evolution therefore only limits which extracts can be created next: a dropped field prevents new extracts, and a type
promotion applies only to extracts created after it.

Ordering key fields are the exception, because a region file may be reused unchanged by a later extract whose source
schema promoted one of them. Its entries keep the order they were written in, so a promotion that reordered values
would leave the reused region file misordered, breaking the non-overlapping region ranges and the `group_max_value`
bounds used to prune them. The restriction is placed on reuse rather than on the promotion, because a source table is
evolved without knowledge of the indexes built over it, so only the index writer can detect the case and rewrite the
region file instead.

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
more ordinary materialized fields, and indexes could identify a row in different ways. The options below cover the
common cases.

To identify an individual row, materialize expressions that reference the `_file` (`2147483646`) and `_pos`
(`2147483645`) metadata columns, as shown in [Appendix C](#appendix-c-example---key-lookup-index). The index fields take
ordinary field IDs, chosen by whoever defines the index. Every entry then carries the data file and the row position of
its source row, so a reader can fetch matching rows without scanning the table.

For a table with row lineage, materializing `_row_id` (`2147483540`) identifies a row with a single field and keeps the
pointer valid when a data file is rewritten, at the cost of resolving the row ID to a location when reading.

An index that only has to eliminate data files can materialize `_file` alone. Recording statistics for a materialized
`_file` field also lets index maintenance find the entries produced by a data file that has since been rewritten.

An index that materializes none of these can still prune region files by ordering key, but it cannot return source
rows.

### Choosing Non-Materialized Fields

Leaving a field non-materialized suits an ordering expression whose result is as large as its source, such as
`lower(name)` over an identity field `name`. Storing the result would nearly double the stored bytes, and a reader can
recompute it from `name` while searching.

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
format, record count, and size, together with the statistics used for pruning.

The index stores `user_id` and the source row location. `user_id` is an identity field, so it keeps the field ID and the
type of the source column, while the location fields are materialized fields that reference metadata columns and so
take ordinary field IDs. The bucket is field `104`; it is evaluated for ordering and tracking statistics but is not
stored in region files. The resulting region schema is:

| Field id, name    | Type     | Description                                              |
|-------------------|----------|----------------------------------------------------------|
| **`1  user_id`**  | `long`   | The identity field on the indexed source column          |
| **`105  file`**   | `string` | The source data file that contains the row, from `_file` |
| **`106  pos`**    | `long`   | The row position within that data file, from `_pos`      |

The location fields are not part of the ordering key, so entries that fall in the same bucket with the same `user_id`
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
  "identity-fields" : [ 1 ],
  "materialized-fields" : [ {
    "field-id" : 105,
    "type" : "expr-value",
    "data-type" : "string",
    "expr" : { "type" : "reference", "id" : 2147483646 }
  }, {
    "field-id" : 106,
    "type" : "expr-value",
    "data-type" : "long",
    "expr" : { "type" : "reference", "id" : 2147483645 }
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
  "ordering-key" : [ 104, 1 ],
  "extracts" : [ {
    "extract-id" : 8744736658442914487,
    "source-table-snapshot-id" : 3055729675574597004,
    "timestamp-ms" : 1573518431292,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet"
  } ]
}
```

The tracking file at `tracking-file` lists the region files of this extract. It is stored in a metadata file
format rather than JSON, so its tracking file entries are shown here as a table. In this example the extract has
two region files:

| file_path                | file_format | record_count | file_size_in_bytes |
|--------------------------|-------------|--------------|--------------------|
| .../region-00001.parquet | parquet     | 3            | 1160               |
| .../region-00002.parquet | parquet     | 2            | 1024               |

Each tracking file entry also carries a `content_stats` struct. The location fields are materialized fields outside
`ordering-key`, so no statistics are required for them and this writer stores none. The struct holds field statistics
for the identity field `user_id` and the non-materialized bucket. Both fields participate in `ordering-key`, so both
stats structs include `group_max_value`:

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

| Region file            | Field     | `lower_bound` | `upper_bound` | `group_max_value` |
|------------------------|-----------|---------------|---------------|-------------------|
| `region-00001.parquet` | bucket    | `3`           | `88`          | `88`              |
| `region-00001.parquet` | `user_id` | `12094`       | `84721`       | `55310`           |
| `region-00002.parquet` | bucket    | `120`         | `209`         | `209`             |
| `region-00002.parquet` | `user_id` | `3277`        | `99182`       | `3277`            |

The `group_max_value` metrics form the ordering key upper bounds `{ bucket: 88, user_id: 55310 }` and
`{ bucket: 209, user_id: 3277 }` when read in `ordering-key` order. The `user_id` bounds of the two files overlap, so
ordinary field statistics alone cannot eliminate either file. The ordering key ranges do not overlap:
`region-00002.parquet` is the second tracking file entry, so its range starts after the first entry's upper bound.

A lookup for `user_id = 55310` evaluates the ordering expressions for that value, producing
`{ bucket: 88, user_id: 55310 }`. That key is not greater than the upper bound of `region-00001.parquet`, the first
tracking file entry, so only the first region file is read.

The rows of `region-00001.parquet` follow the region schema constructed from the identity field and the materialized
fields. They are stored in index order. The non-materialized bucket is shown here to make the complete ordering
key visible:

| user_id | file                            | pos | (ordering key) |
|---------|---------------------------------|-----|------------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`   |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }`  |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }`  |

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
    "timestamp-ms" : 1573518431292,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00001-(uuid).parquet"
  }, {
    "extract-id" : 6574117201097113750,
    "source-table-snapshot-id" : 5459876531255530170,
    "timestamp-ms" : 1573518981593,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
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
the same field bounds and the same `group_max_value` metrics, which produce the ordering key upper bound
`{ bucket: 88, user_id: 55310 }`. The entry for `region-00002.parquet` is copied from the previous tracking file.

The rows of `region-00003.parquet` interleave the entries of the rewritten region file with the entries added for the
new data file, keeping the index order:

| user_id | file                            | pos | (ordering key) |
|---------|---------------------------------|-----|------------------|
| 84721   | .../data/00000-0-(uuid).parquet | 14  | `{ 3, 84721 }`   |
| 71004   | .../data/00002-0-(uuid).parquet | 5   | `{ 17, 71004 }`  |
| 12094   | .../data/00001-0-(uuid).parquet | 3   | `{ 41, 12094 }`  |
| 40318   | .../data/00002-0-(uuid).parquet | 22  | `{ 62, 40318 }`  |
| 55310   | .../data/00000-0-(uuid).parquet | 92  | `{ 88, 55310 }`  |

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
    "timestamp-ms" : 1573518981593,
    "tracking-file" : "s3://bucket/warehouse/default.db/events/index/bucket_index/metadata/tracking-00002-(uuid).parquet"
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
