---
date: 2026-08-12
title: "Semi-Structured Data in Apache Iceberg: Meet the Variant Type"
slug: variant-in-apache-iceberg
authors:
  - nssalian
categories:
  - blog
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

Semi-structured data, such as JSON-like documents whose fields differ from row to row, has always been a poor fit for table formats built around fixed schemas. Iceberg v3 adds the Variant type for exactly this data: a single column can hold values of arbitrary, evolving shape, stored as a compact binary value that engines read and write consistently.

This is the first post in a series on Variant in Apache Iceberg: what Variant is, why it exists, and how it fits into an Iceberg table. Parquet already defines the Variant type and its binary encoding, so this post focuses on what Iceberg adds on top: how Variant fits the table's schema, files, snapshots, and statistics.

Variant is stored in Parquet, Avro, and ORC. Shredding, an optimization that stores commonly queried Variant fields as their own typed columns, is available only in Parquet; a later post covers how it works.

<!-- more -->

## The problem Variant solves

Consider event data whose shape changes over time:

```json
{"event": "view", "page": "/pricing", "session": "s-8f21"}
{"event": "signup", "session": "s-8f21", "plan": "pro", "price": 29.00}
{"event": "view", "page": "/docs", "session": "s-3c07", "referrer": {"source": "search", "term": "iceberg variant"}}
```

Without Variant, an Iceberg table has two ways to store data like this, and both have drawbacks:

- **JSON stored as a string.** This is flexible, but reading a single field means parsing the whole text. JSON's type system is also thin: a timestamp is just a string, and a number's precision is ambiguous.
- **A rigid, flattened schema.** This is fast to query, but every new field is a schema migration, and sparse or one-off fields waste space.

Variant is as flexible as JSON but encodes data as compact, typed binary. Values keep their native types: a timestamp stays a timestamp and a decimal stays an exact decimal, instead of collapsing to JSON's strings and numbers. No schema is declared up front, so documents of different shapes coexist in one column and a new field needs no migration.

## Variant in Apache Iceberg

Variant was added to the Iceberg type system in the v3 spec. The spec places it in its own category: `variant` is "neither a primitive type nor a nested type." It is richer than a primitive, yet has no fixed, declared shape the way a struct or list does.

A Variant value is similar to JSON, but with a wider set of primitives including `date`, `timestamp`, `timestamptz`, `binary`, and `decimal`. It can also nest:

1. A **Variant array** is an ordered collection of Variant values. Unlike an Iceberg list, its elements are not constrained to a single element type.
2. A **Variant object** is a collection of string-keyed fields whose values are themselves Variant values. Unlike a struct column in an Iceberg schema, its fields are not a fixed, named set of typed columns.

In an Iceberg schema, a Variant column is declared with the type name `variant` and stored in table metadata as a plain string, `"variant"`, not a nested object like a struct or list. Because its shape is not fixed, a Variant column cannot be promoted to or from another type, cannot be used for partitioning or as an identifier field, and must default to null.

### How it is stored

Iceberg does not define its own binary encoding for Variant. The type and its encoding come from the [Apache Parquet project](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md), and Iceberg uses that encoding as-is, so a `variant` column maps to a Parquet `group` with two binary fields:

```parquet
optional group payload (VARIANT(1)) {
  required binary metadata;
  required binary value;
}
```

- `metadata` holds the dictionary of field names used in the value, so those names are not repeated inline with the data.
- `value` holds the encoded data: a scalar, an array, or an object. Arrays and objects store a `field_offset` per element (the byte offset where that element's value starts), and objects also store a `field_id` per field (an index into the metadata dictionary).

The Variant column itself is addressed by its Iceberg field ID like any other column, but its `metadata` and `value` subfields are accessed by name, which matters for shredding.

The same Variant maps into every file format Iceberg supports: a Parquet `group`, an Avro `record`, or an ORC `struct`, each holding the `metadata` and `value` pair. In Avro and ORC, a Variant is always the single unshredded pair.

### One column, many layouts

A Variant's structure is not consistent across rows or files, but the column's Iceberg type is always `variant`, whatever shapes flow through it. Adding or removing a field inside the data changes only the bytes in each row.

That one logical column can be laid out differently in each data file. In Parquet, one file may store it unshredded as the `metadata` + `value` pair while another shreds its hot fields into dedicated typed columns. Both files carry the same Variant field ID, and a reader reconciles whichever layout it finds:

```text
payload  (one Variant column, one field ID)
├─ data file A, unshredded:  metadata + value
└─ data file B, shredded:    metadata + value + typed_value.event, typed_value.country
```

Snapshots do not change this. Each snapshot records the schema that was current when it was written, and because the Variant column keeps its field ID across schema changes, time travel reads every file back through the same column.

### Statistics and data skipping

Because Variant is a column in the Iceberg schema, the table's manifests carry statistics for it, and that is what lets Iceberg skip files during planning. Iceberg records value and null counts for a Variant column. When a field is shredded into its own typed column, Iceberg also records lower and upper bounds for it, stored as a Variant object whose keys are normalized JSON paths to each field. An unshredded `value` blob is opaque, so it contributes counts but no bounds.

With those bounds in the manifest, a predicate on a shredded field can prune whole files before any data is read: if a file's recorded range for that field cannot match, Iceberg skips it during planning. Fields that are not shredded have no bounds, so the engine reads and filters them instead.

### Reading and writing across engines

- **Apache Spark 4.0 and 4.1** create tables with `VARIANT` columns and read and write them. Reading landed in Iceberg 1.10.0 on Spark 4.0; Iceberg 1.11.0 added Spark 4.1 and writing shredded Variant on both Spark 4.0 and 4.1.
- **Apache Flink 2.1** added Variant support in Iceberg 1.11.0. This covers unshredded Variant only; shredded write support is merged and expected in a later release.

Because these engines all use the same Iceberg Variant type, a value written by one reads back identically in the others.

Apache Arrow carries that same Variant value in memory through its canonical extension type `arrow.parquet.variant`, letting engines exchange it without special handling.

## Working with Variant

With Spark SQL, you store heterogeneous events in a single `VARIANT` column and read fields back with `variant_get`. A Variant column requires an Iceberg v3 table:

```sql
-- Variant is a v3 type, so the table must be format version 3
CREATE TABLE events (id BIGINT, payload VARIANT)
USING iceberg
TBLPROPERTIES ('format-version' = '3');

-- Insert events of different shapes into the same column
INSERT INTO events VALUES
    (1, parse_json('{"event": "login", "country": "US"}')),
    (2, parse_json('{"event": "purchase", "country": "UK", "amount": 99}')),
    (3, parse_json('{"event": "login"}'));

-- Read fields out of the Variant with variant_get(column, path, type)
SELECT
    id,
    variant_get(payload, '$.event', 'string')   AS event,
    variant_get(payload, '$.country', 'string') AS country,
    variant_get(payload, '$.amount', 'int')     AS amount
FROM events;
```

The query returns one row per event, with `null` wherever a field is absent:

| id | event      | country | amount |
|----|------------|---------|--------|
| 1  | `login`    | `US`    | `null` |
| 2  | `purchase` | `UK`    | `99`   |
| 3  | `login`    | `null`  | `null` |

Row 3 carries only `event`, so `country` and `amount` read back as `null`. Variant does not require every row to share the same structure, so heterogeneous events live in one column.

## When to reach for Variant

Variant is the right choice when you do not control the shape of the data, or it changes faster than you want to evolve a schema:

- **Event and clickstream data**, where each event type carries a different set of fields and new ones appear over time.
- **Application and service logs** with structured but heterogeneous payloads.
- **IoT and sensor telemetry**, where each device model reports its own readings.
- **Third-party API responses and webhooks**, whose schema is owned by someone else and can change without notice.
- **Sparse attributes** that would otherwise become a wide table of mostly-null columns.

Variant is not a replacement for a known schema. When a field is present on every row and you query it constantly, a regular typed column, or a struct for a fixed nested shape, is simpler and more efficient. A common pattern is to keep the stable, frequently queried fields as normal columns and put the variable part in one Variant column.

## What's next: shredding

So far, a Variant column is a single `metadata` + `value` pair. Reading one field still means decoding the `metadata` dictionary and pulling that field from the single `value` blob, so a query for one field reads the entire `value` column. The next post explains how shredding works: how a writer chooses which fields to extract into their own typed columns and infers a type for each, how values that do not fit fall back to the untyped `value`, and how a reader reconstructs the original Variant from both.

## Getting Involved

Variant support in Iceberg is still growing, and contributions are welcome. The [Variant tracking issue (#10392)](https://github.com/apache/iceberg/issues/10392) follows the ongoing work, and you can reach the community through the [mailing lists and Slack](https://iceberg.apache.org/community/).

## Resources

- **Iceberg Table Spec, Variant type:** [Semi-structured Types](https://iceberg.apache.org/spec/#semi-structured-types)
- **Variant in Apache Parquet:** [Variant Type in Apache Parquet for Semi-Structured Data](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/)
- **Variant in Apache Arrow:** [Parquet Variant canonical extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant)