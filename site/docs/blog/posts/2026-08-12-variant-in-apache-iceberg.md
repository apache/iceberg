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

Semi-structured data, such as JSON-like documents whose fields differ from row to row, has always been a poor fit for table formats built around fixed schemas. Iceberg v3 adds the Variant type for exactly this data: a single column can hold values of arbitrary, evolving shape, stored in a compact binary form that engines read and write consistently.

This is the first post in a series on Variant in Apache Iceberg. It covers what Variant is, why it exists, and how it fits into an Iceberg table. The next post covers shredding, the technique that stores frequently accessed Variant fields as typed, columnar data. Variant is stored in Parquet, Avro, and ORC; shredding is currently available only in Parquet.

<!-- more -->

## The problem Variant solves

Consider event data whose shape changes over time:

```json
{"event": "view", "page": "/pricing", "session": "s-8f21"}
{"event": "signup", "session": "s-8f21", "plan": "pro", "price": 29.00}
{"event": "view", "page": "/docs", "session": "s-3c07", "referrer": {"source": "search", "term": "iceberg variant"}}
```

Two traditional approaches handle this, and both have drawbacks:

- **JSON stored as a string.** This is flexible, but reading a single field means parsing the whole text. JSON's type system is also thin: a timestamp is just a string, and a number's precision is ambiguous.
- **A rigid, flattened schema.** This is fast to query, but every new field is a schema migration, and sparse or one-off fields waste space.

Variant is as flexible as JSON but stores data in a compact, typed binary form. Values keep their native types: a timestamp stays a timestamp and a decimal stays an exact decimal, instead of collapsing to JSON's strings and numbers. Within a value, field names are collected into a dictionary and referenced by id, so a name is not written out in full each time it appears. No schema is declared up front, so documents of different shapes coexist in one column and a new field needs no migration.

## Variant in Apache Iceberg

Variant was added to the Iceberg type system in the v3 spec. The spec places it in its own category: `variant` is "neither a primitive type nor a nested type." It is richer than a primitive, yet has no fixed, declared shape the way a struct or list does.

A Variant value is similar to JSON, but with a wider set of primitives including `date`, `timestamp`, `timestamptz`, `binary`, and `decimal`. It can also nest:

1. A **Variant array** is an ordered collection of Variant values. Unlike an Iceberg list, its elements are not constrained to a single element type.
2. A **Variant object** is a collection of string-keyed fields whose values are themselves Variant values. Unlike a struct column in an Iceberg schema, its fields are not a fixed, named set of typed columns.

### How it is stored

Iceberg does not define its own binary encoding for Variant. The type and its encoding come from the [Apache Parquet project](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md), and Iceberg uses that encoding as-is, so a `variant` column maps to a Parquet `group` with two binary fields:

```parquet
optional group payload (VARIANT(1)) {
  required binary metadata;
  required binary value;
}
```

- `metadata` holds a dictionary of the field names used in the value, so the `value` bytes reference each name by an integer id instead of repeating the name string.
- `value` holds the encoded data: a scalar, an array, or an object. Arrays and objects store a `field_offset` per element (the byte offset where that element's value starts), and objects also store a `field_id` per field (an index into the metadata dictionary).

The Variant column itself is addressed by field ID like any other Iceberg column, but its `metadata` and `value` subfields are accessed by name, which matters for shredding.

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

Variant is not a replacement for a known schema. When a field is present on every row and you query it constantly, a regular typed column, or a struct for a fixed nested shape, is simpler and more efficient. A common pattern is to keep the stable, frequently queried fields as normal columns and put the variable part in a single Variant column.

## What's next: shredding

So far, a Variant column is a single `metadata` + `value` pair. Reading one field means decoding the `metadata` dictionary, then deserializing that field out of the `value` blob if it is present. That is flexible, but the flexibility costs performance: because the whole document lives in one `value` column, the engine cannot use per-field min/max statistics to prune data the way it can for a regular typed column. Shredding recovers this: it stores frequently accessed fields as separate, typed Parquet columns, so queries that touch only those fields read only those columns and can use their statistics for data skipping, while anything that does not fit falls back to the untyped `value`. The next post in this series explains how shredding works and what it unlocks.

## Getting Involved

Variant support in Iceberg is still growing, and contributions are welcome. The [Variant tracking issue (#10392)](https://github.com/apache/iceberg/issues/10392) follows the ongoing work, and you can reach the community through the [mailing lists and Slack](https://iceberg.apache.org/community/).

## Resources

- **Iceberg Table Spec, Variant type:** [Semi-structured Types](https://iceberg.apache.org/spec/#semi-structured-types)
- **Variant in Apache Parquet:** [Variant Type in Apache Parquet for Semi-Structured Data](https://parquet.apache.org/blog/2026/02/27/variant-type-in-apache-parquet-for-semi-structured-data/)
- **Variant in Apache Arrow:** [Parquet Variant canonical extension type](https://arrow.apache.org/docs/format/CanonicalExtensions.html#parquet-variant)