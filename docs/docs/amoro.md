---
title: "Apache Amoro"
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

# Apache Amoro With Iceberg

**[Apache Amoro(incubating)](https://amoro.apache.org)** is a Lakehouse management system built on open data lake formats. Working with compute engines including Flink, Spark, and Trino, Amoro brings pluggable and
**[Table Maintenance](https://amoro.apache.org/docs/latest/self-optimizing/)** features for a Lakehouse to provide out-of-the-box data warehouse experience, and helps data platforms or products easily build infra-decoupled, stream-and-batch-fused and lake-native architecture.
**AMS(Amoro Management Service)**  provides Lakehouse management features, like self-optimizing, data expiration, etc. It also provides a unified catalog service for all compute engines, which can also be combined with existing metadata services like HMS(Hive Metastore).

# Auto Self-optimizing

Lakehouse is characterized by its openness and loose coupling, with data and files maintained by users through various engines. While this
architecture appears to be well-suited for T+1 scenarios, as more attention is paid to applying Lakehouse to streaming data warehouses and real-time
analysis scenarios, challenges arise. For example:

- Streaming writes bring a massive amount of fragment files
- CDC ingestion and streaming updates generate excessive redundant data
- Using the new data lake format leads to orphan files and expired snapshots.

These issues can significantly affect the performance and cost of data analysis. Therefore, Amoro has introduced a Self-optimizing mechanism to
create an out-of-the-box Streaming Lakehouse management service that is as user-friendly as a traditional database or data warehouse. Self-optimizing involves various procedures such as file compaction, deduplication, and sorting.

The architecture and working mechanism of Self-optimizing are shown in the figure below:

![Self-optimizing architecture](https://github.com/apache/amoro/blob/master/docs/images/concepts/self-optimizing_arch.png)

The Optimizer is a component responsible for executing Self-optimizing tasks. It is a resident process managed by [AMS](https://amoro.apache.org/docs/latest/#architecture). AMS is responsible for
detecting and planning Self-optimizing tasks for tables, and then scheduling them to Optimizers for distributed execution in real-time. Finally, AMS
is responsible for submitting the optimizing results. Amoro achieves physical isolation of Optimizers through the Optimizer Group.

The core features of [Amoro's Self-optimizing](https://amoro.apache.org/docs/latest/self-optimizing/) are:

- Automated, Asynchronous and Transparent — Continuous background detecting of file changes, asynchronous distributed execution of optimizing tasks,
  transparent and imperceptible to users
- Resource Isolation and Sharing — Allow resources to be isolated and shared at the table level, as well as setting resource quotas
- Flexible and Scalable Deployment — Optimizers support various deployment methods and convenient scaling


# Iceberg Format

Apache Amoro supports all catalog types supported by Iceberg, including common catalog: [REST](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog), Hadoop, Hive, Glue, JDBC, Nessie and other third-party catalog.
Amoro supports all storage types supported by Iceberg, including common store: Hadoop, S3, GCS, ECS, OSS, and so on.

At the same time, we also provide a unique form based on Apache Iceberg, including mixed-Iceberg Format and mixed-Hive Format, so that you can quickly upgrade to the iceberg+hive Mixed table while compatible with the original Hive data

## Mixed-Iceberg Format

[Mixed-Iceberg Format](https://amoro.apache.org/docs/latest/mixed-iceberg-format/) Compared with Iceberg format, Mixed-Iceberg format provides more features:

- Stronger primary key constraints that also apply to Spark
- OLAP performance that is production-ready for real-time data warehouses through the auto-bucket mechanism
- LogStore configuration that can reduce data pipeline latency from minutes to milliseconds/seconds
- Transaction conflict resolution mechanism that enables concurrent writes with the same primary key
- The design intention of Mixed-Iceberg format is to provide a storage layer for stream-batch integration and offline-real-time unified data warehouses for big data platforms based on data lakes. Under this goal-driven approach, Amoro designs Mixed-Iceberg format as a three-tier structure, with each level named after a different TableStore:

![mixed_format](https://github.com/apache/amoro/blob/master/docs/images/formats/mixed_format.png)

- BaseStore — stores the stock data of the table, usually generated by batch computing or optimizing processes, and is more friendly to ReadStore for reading.
- ChangeStore — stores the flow and change data of the table, usually written in real-time by streaming computing, and can also be used for downstream CDC consumption, and is more friendly to WriteStore for writing.
- LogStore — serves as a cache layer for ChangeStore to accelerate stream processing. Amoro manages the consistency between LogStore and ChangeStore.


## Mixed-Hive Format 

[Mixed-Hive](https://amoro.apache.org/docs/latest/mixed-hive-format/) format is a format that has better compatibility with Hive than Mixed-Iceberg format. Mixed-Hive format uses a Hive table as the BaseStore and an Iceberg table as the ChangeStore. Mixed-Hive format supports:

![mixed_format](https://github.com/apache/amoro/blob/master/docs/images/formats/mixed_format.png)

- Schema, partition, and types consistent with Hive format
- Using the Hive connector to read and write Mixed-Hive format tables as Hive tables
- Upgrading a Hive table in-place to a Mixed-Hive format table without data rewriting or migration, with a response time in seconds
- All the functional features of Mixed-Iceberg format
