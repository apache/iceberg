---
title: "Vendors"
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

## Vendors Supporting Iceberg Tables

This page contains some of the vendors who are shipping and supporting Apache Iceberg in their products
### [Bodo](https://bodo.ai)

Bodo is a high performance SQL & Python compute engine that brings HPC and supercomputing techniques to data analytics.
Bodo supports Apache Iceberg tables as a first-class data source, enabling users to read and write Iceberg tables
with Bodo's high-performance data processing engine. Bodo's integration with Iceberg allows users to leverage
Iceberg's table format for efficient data management and analytics. Bodo is available as a cloud service on
AWS and Azure, and as well as an on-premises solution.

### [CelerData](https://celerdata.com)

CelerData provides commercial offerings for [StarRocks](https://www.starrocks.io/), a distributed MPP SQL engine for enterprise analytics on Iceberg. With its fully vectorized technology, local caching, and intelligent materialized view, StarRocks delivers sub-second query latency for both batch and real-time analytics. CelerData offers both an [enterprise deployment](https://celerdata.com/celerdata-enterprise) and a [cloud service](https://celerdata.com/celerdata-cloud) to help customers use StarRocks more smoothly. Learn more about how to query Iceberg with StarRocks [here](https://docs.starrocks.io/en-us/latest/data_source/catalog/iceberg_catalog).

### [ClickHouse](https://clickhouse.com/)
ClickHouse is a column-oriented database that enables its users to generate powerful analytics, using SQL queries, in real-time. ClickHouse integrates well with Iceberg and offers two options to work with it:
1. Via Iceberg [table function](https://clickhouse.com/docs/en/sql-reference/table-functions/iceberg): Provides a read-only table-like interface to Apache Iceberg tables in Amazon S3.
2. Via the Iceberg [table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/iceberg): An engine that provides a read-only integration with existing Apache Iceberg tables in Amazon S3.

### [Cloudera](http://cloudera.com)

[Cloudera's data lakehouse](https://www.cloudera.com/products/open-data-lakehouse.html)
enables customers to store and manage their data in open table
formats like Apache Iceberg for running large scale multi-function analytics and AI.
Organizations rely on Cloudera's Iceberg support because it is easy to use, easy
to integrate into any data ecosystem and easy to run multiple engines - both Cloudera
and non-Cloudera, regardless of where the data resides.
It provides a common standard for all data with unified security, governance, metadata
management, and fine-grained access control across the data.

[Cloudera](https://www.cloudera.com/) provides an integrated end to end open data lakehouse with the ability
to ingest batch and streaming data using NiFi, Flink and Kafka, then process
the same copy of data using Spark and run analytics or AI with our
[Data Visualization](https://www.cloudera.com/products/cloudera-data-platform/data-visualization.html),
[Data warehouse](https://www.cloudera.com/products/data-warehouse.html) and
[Machine Learning](https://www.cloudera.com/products/machine-learning.html) tools on private
or any public cloud.

### [Dremio](https://www.dremio.com/)

With Dremio, an organization can easily build and manage a data lakehouse in which data is stored in open formats like Apache Iceberg and can be processed with Dremio’s interactive SQL query engine and non-Dremio processing engines. [Dremio Cloud](https://www.dremio.com/get-started/) provides these capabilities in a fully managed offering.

* [Dremio Sonar](https://www.dremio.com/platform/sonar/) is a lakehouse query engine that provides interactive performance and DML on Apache Iceberg, as well as other formats and data sources.
* [Dremio Arctic](https://www.dremio.com/platform/arctic/) is a lakehouse catalog and optimization service for Apache Iceberg. Arctic automatically optimizes tables in the background to ensure high-performance access for any engine. Arctic also simplifies experimentation, data engineering, and data governance by providing Git concepts like branches and tags on Apache Iceberg tables.

### [IOMETE](https://iomete.com/)

IOMETE is a fully-managed ready to use, batteries included Data Platform. IOMETE optimizes clustering, compaction, and access control to Apache Iceberg tables. Customer data remains on customer's account to prevent vendor lock-in. The core of IOMETE platform is a serverless Lakehouse that leverages Apache Iceberg as its core table format. IOMETE platform also includes Serverless Spark, an SQL Editor, A Data Catalog, and granular data access control. IOMETE supports Hybrid-multi-cloud setups. 

### [PuppyGraph](https://puppygraph.com)

PuppyGraph is a cloud-native graph analytics engine that enables users to query one or more relational data stores as a unified graph model. This eliminates the overhead of deploying and maintaining a siloed graph database system, with no ETL required. [PuppyGraph’s native Apache Iceberg integration](https://docs.puppygraph.com/user-manual/getting-started/iceberg) adds native graph capabilities to your existing data lake in an easy and performant way.

<!-- markdown-link-check-disable-next-line -->
### [Snowflake](https://snowflake.com/)

<!-- markdown-link-check-disable-next-line -->
[Snowflake](https://www.snowflake.com/) is a single, cross-cloud platform that enables every organization to mobilize their data with Snowflake’s Data Cloud. Snowflake supports Apache Iceberg by offering [Snowflake-managed Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg#use-snowflake-as-the-iceberg-catalog) for full DML as well as [externally managed Iceberg Tables with catalog integrations](https://docs.snowflake.com/en/user-guide/tables-iceberg#use-a-catalog-integration) for read-only access.

### [Starburst](https://starburst.io)

Starburst is a commercial offering for the [Trino query engine](https://trino.io). Trino is a distributed MPP SQL query engine that can query data in Iceberg at interactive speeds. Trino also enables you to join Iceberg tables with an [array of other systems](https://trino.io/docs/current/connector.html). Starburst offers both an [enterprise deployment](https://www.starburst.io/platform/starburst-enterprise/) and a [fully managed service](https://www.starburst.io/platform/starburst-galaxy/) to make managing and scaling Trino a flawless experience. Starburst also provides customer support and houses many of the original contributors to the open-source project that know Trino best. Learn more about [the Starburst Iceberg connector](https://docs.starburst.io/latest/connector/iceberg.html).

### [Tabular](https://tabular.io)

[Tabular](https://tabular.io/product/) is a managed warehouse and automation platform. Tabular offers a central store for analytic data that can be used with any query engine or processing framework that supports Iceberg. Tabular warehouses add role-based access control and automatic optimization, clustering, and compaction to Iceberg tables.

### [Upsolver](https://upsolver.com)

[Upsolver](https://upsolver.com) is a streaming data ingestion and table management solution for Apache Iceberg. With Upsolver, users can easily ingest batch and streaming data from files, streams and databases (CDC) into [Iceberg tables](https://docs.upsolver.com/reference/sql-commands/iceberg-tables/upsolver-managed-tables). In addition, Upsolver connects to your existing REST and Hive catalogs, and [analyzes the health](https://docs.upsolver.com/how-to-guides/apache-iceberg/optimize-your-iceberg-tables) of your tables. Use Upsolver to continuously optimize tables by compacting small files, sorting and compressing, repartitioning, and cleaning up dangling files and expired manifests. Upsolver is available from the [Upsolver Cloud](https://www.upsolver.com/sqlake-signup-wp) or can be deployed in your AWS VPC.
