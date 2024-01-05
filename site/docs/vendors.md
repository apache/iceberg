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

### [CelerData](https://celerdata.com)

CelerData provides commercial offerings for [StarRocks](https://www.starrocks.io/), a distributed MPP SQL engine for enterprise analytics on Iceberg. With its fully vectorized technology, local caching, and intelligent materialized view, StarRocks delivers sub-second query latency for both batch and real-time analytics. CelerData offers both an [enterprise deployment](https://celerdata.com/celerdata-enterprise) and a [cloud service](https://celerdata.com/celerdata-cloud) to help customers use StarRocks more smoothly. Learn more about how to query Iceberg with StarRocks [here](https://docs.starrocks.io/en-us/latest/data_source/catalog/iceberg_catalog).

### [Cloudera](http://cloudera.com)

Cloudera Data Platform integrates Apache Iceberg to the following components:
* Apache Hive, Apache Impala, and Apache Spark to query Apache Iceberg tables
* Cloudera Data Warehouse service providing access to Apache Iceberg tables through Apache Hive and Apache Impala
* Cloudera Data Engineering service providing access to Apache Iceberg tables through Apache Spark
* The CDP Shared Data Experience (SDX) provides compliance and self-service data access for Apache Iceberg tables
* Hive metastore, which plays a lightweight role in providing the Iceberg Catalog
* Data Visualization to visualize data stored in Apache Iceberg

https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html

### [Dremio](https://www.dremio.com/)

With Dremio, an organization can easily build and manage a data lakehouse in which data is stored in open formats like Apache Iceberg and can be processed with Dremio’s interactive SQL query engine and non-Dremio processing engines. [Dremio Cloud](https://www.dremio.com/get-started/) provides these capabilities in a fully managed offering.

* [Dremio Sonar](https://www.dremio.com/platform/sonar/) is a lakehouse query engine that provides interactive performance and DML on Apache Iceberg, as well as other formats and data sources.
* [Dremio Arctic](https://www.dremio.com/platform/arctic/) is a lakehouse catalog and optimization service for Apache Iceberg. Arctic automatically optimizes tables in the background to ensure high-performance access for any engine. Arctic also simplifies experimentation, data engineering, and data governance by providing Git concepts like branches and tags on Apache Iceberg tables.

### [IOMETE](https://iomete.com/)

IOMETE is a fully-managed ready to use, batteries included Data Platform. IOMETE optimizes clustering, compaction, and access control to Apache Iceberg tables. Customer data remains on customer's account to prevent vendor lock-in. The core of IOMETE platform is a serverless Lakehouse that leverages Apache Iceberg as its core table format. IOMETE platform also includes Serverless Spark, an SQL Editor, A Data Catalog, and granular data access control. IOMETE supports Hybrid-multi-cloud setups. 

### [Snowflake](http://snowflake.com/)

[Snowflake](https://www.snowflake.com/data-cloud/) is a single, cross-cloud platform that enables every organization to mobilize their data with Snowflake’s Data Cloud. Snowflake supports Apache Iceberg by offering [native support for Iceberg Tables](https://www.snowflake.com/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/) for full DML as well as connectors to [External Tables](https://www.snowflake.com/blog/expanding-the-data-cloud-with-apache-iceberg/) for read-only access.

### [Starburst](http://starburst.io)

Starburst is a commercial offering for the [Trino query engine](https://trino.io). Trino is a distributed MPP SQL query engine that can query data in Iceberg at interactive speeds. Trino also enables you to join Iceberg tables with an [array of other systems](https://trino.io/docs/current/connector.html). Starburst offers both an [enterprise deployment](https://www.starburst.io/platform/starburst-enterprise/) and a [fully managed service](https://www.starburst.io/platform/starburst-galaxy/) to make managing and scaling Trino a flawless experience. Starburst also provides customer support and houses many of the original contributors to the open-source project that know Trino best. Learn more about [the Starburst Iceberg connector](https://docs.starburst.io/latest/connector/iceberg.html).

### [Tabular](https://tabular.io)

[Tabular](https://tabular.io/product/) is a managed warehouse and automation platform. Tabular offers a central store for analytic data that can be used with any query engine or processing framework that supports Iceberg. Tabular warehouses add role-based access control and automatic optimization, clustering, and compaction to Iceberg tables.
