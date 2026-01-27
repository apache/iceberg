---
date: 2026-01-21
title: Iceberg Meetup Japan 4th Recap
authors:
  - yuya-ebihara
categories:
  - community
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

**Iceberg Meetup Japan #4 Recap**

Apache Iceberg continues to gain momentum in Japan. In 2025, two Iceberg-related books were
published [[1]](https://gihyo.jp/book/2025/978-4-297-15074-7)[[2]](https://book.impress.co.jp/books/1124101072), and our
community meetups regularly attracted over 200 participants.

To kick off 2026, we hosted the [Apache Iceberg Meetup Japan #4](https://iceberg.connpass.com/event/378396/) in January,
featuring four presentations ranging from beginner introductions to advanced features.

<!-- more -->

### Getting Started with Apache Iceberg

<iframe class="speakerdeck-iframe" frameborder="0" src="https://speakerdeck.com/player/23f7d16789e34adfb5e96d36485a3d2a" title="[Iceberg Meetup #4] ゼロからはじめる: Apache Icebergとはなにか？ / Apache Iceberg for Beginners" allowfullscreen="true" style="border: 0px; background: padding-box padding-box rgba(0, 0, 0, 0.1); margin: 0px; padding: 0px; border-radius: 6px; box-shadow: rgba(0, 0, 0, 0.2) 0px 5px 40px; width: 100%; height: auto; aspect-ratio: 560 / 315;" data-ratio="1.7777777777777777"></iframe>

Saki Kitaoka (Databricks) gave a clear introduction to Open Table Formats and Apache Iceberg. She explained how Iceberg
brings reliability and manageability to data lakes by organizing metadata at the file level, enabling ACID transactions,
time travel, and schema evolution.

Her talk covered Iceberg’s architecture, highlighting its three logical layers and how metadata is managed for efficient
read/write operations.

**Key features:**

- ACID reliability and historical data management
- Metadata design for large-scale analytics
- Open, engine-agnostic architecture

### Apache Iceberg V3: Current Status and Migration

<iframe class="speakerdeck-iframe" frameborder="0" src="https://speakerdeck.com/player/db6c8369596a44c79c9bd6f085bd9ed6" title="Apache Iceberg V3 and migration to V3" allowfullscreen="true" style="border: 0px; background: padding-box padding-box rgba(0, 0, 0, 0.1); margin: 0px; padding: 0px; border-radius: 6px; box-shadow: rgba(0, 0, 0, 0.2) 0px 5px 40px; width: 100%; height: auto; aspect-ratio: 560 / 315;" data-ratio="1.7777777777777777"></iframe>

Tomohiro Tanaka (AWS) discussed new data types and features in Iceberg V3, including unknown, variant, timestamp nanos,
and geometry/geography types. He also covered Deletion Vectors, Row Lineage, Default Values, Multi-Argument Transforms,
and Table Encryption.

The session focused on the variant type and Deletion Vectors. With Variant, you can use `parse_json` at write time and
`variant_get` at read time. V3 also improves efficiency by reducing Puffin file size during DELETE operations.

Merge-on-Read performance gains depend on workload size, so benchmarking is recommended.

### Iceberg Streaming Write at Repro

<iframe class="speakerdeck-iframe" frameborder="0" src="https://speakerdeck.com/player/468c4310bb5c43fb8786dc46fd996dc0" title="ReproでのicebergのStreaming Writeの検証と実運用にむけた取り組み" allowfullscreen="true" style="border: 0px; background: padding-box padding-box rgba(0, 0, 0, 0.1); margin: 0px; padding: 0px; border-radius: 6px; box-shadow: rgba(0, 0, 0, 0.2) 0px 5px 40px; width: 100%; height: auto; aspect-ratio: 560 / 315;" data-ratio="1.7777777777777777"></iframe>

Tomohiro Hashidate (Repro) shared his experience evaluating Iceberg for streaming writes under mixed workloads,
including queries fetching millions of records and single-record lookups.

Repro’s existing architecture uses Cassandra for write scalability and per-record updates, but it’s not ideal for bulk
loads. After evaluating Hudi, they found Trino’s support limited and Spark compaction difficult to control.

To replicate Cassandra queries, they created a Trino view that UNIONs Kafka and Iceberg connectors. Single queries
performed well, but further evaluation is needed for high concurrency scenarios.

### High-Performance Analytics on Iceberg with StarRocks

<iframe class="speakerdeck-iframe" frameborder="0" src="https://speakerdeck.com/player/22380500964947528656234bfbff1a98" title="StarRocks Introduction for Iceberg Meetup Japan #4" allowfullscreen="true" style="border: 0px; background: padding-box padding-box rgba(0, 0, 0, 0.1); margin: 0px; padding: 0px; border-radius: 6px; box-shadow: rgba(0, 0, 0, 0.2) 0px 5px 40px; width: 100%; height: auto; aspect-ratio: 560 / 315;" data-ratio="1.7777777777777777"></iframe>

Marko SUN (CelerData) introduced StarRocks, a high-performance MPP database for real-time analytics. 

He explained how
metadata planning can be a bottleneck with Iceberg, and how StarRocks optimizes planning by switching between
distributed and local strategies based on metadata size.
