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

### [Amazon Web Services (AWS)](https://aws.com)

AWS provides a [comprehensive suite of services](https://aws.amazon.com/what-is/apache-iceberg/#seo-faq-pairs#what-aws-services-support-iceberg) to support Apache Iceberg as part of its modern data architecture. [Amazon Athena](https://aws.amazon.com/athena/) offers a serverless, interactive query engine with native support for Iceberg, enabling fast and cost-efficient querying of large-scale datasets. [Amazon EMR](https://aws.amazon.com/emr/) integrates Iceberg with Apache Spark, Apache Flink, Apache Hive, Presto and Trino, making it easy to process and analyze data at scale. [AWS Glue](https://aws.amazon.com/glue/) provides fully managed data integration capabilities, including schema evolution, maintenance, optimizations and partition management for Iceberg tables. With [Amazon S3](https://aws.amazon.com/s3/) as the underlying storage layer, AWS enables a high-performance and cost-effective data lakehouse solution powered by Iceberg.

### [BladePipe](https://bladepipe.com)

BladePipe is a real-time end-to-end data integration tool, offering 40+ out-of-the-box connectors. It provides a one-stop data movement solution, including schema evolution, data migration and sync, verification and correction, monitoring and alerting. With sub-second latency, BladePipe captures change data from MySQL, Oracle, PostgreSQL and other sources and streams it to Iceberg and more, all without writing a single line of code. It offers [on-premise and BYOC deployment options](https://www.bladepipe.com/pricing). Learn more about how to build a pipeline with BladePipe [here](https://doc.bladepipe.com/quick/quick_start_byoc).

### [Bodo](https://bodo.ai)

Bodo is a high performance SQL & Python compute engine that brings HPC and supercomputing techniques to data analytics.
Bodo supports Apache Iceberg tables as a first-class table format and storage, enabling users to read and write Iceberg
tables with Bodo's high-performance data processing engine. Bodo is available as a cloud service on
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

### [Confluent](https://confluent.io)

[Confluent](https://www.confluent.io/) provides [Tableflow](https://www.confluent.io/product/tableflow/), a managed service for streaming data from Apache Kafka topics into Apache Iceberg tables. Tableflow automates schema evolution through Schema Registry integration and handles table maintenance tasks including compaction automatically.

### [Crunchy Data](https://www.crunchydata.com/)

[Crunchy Data Warehouse](https://www.crunchydata.com/products/warehouse) is a modern data warehouse built on PostgreSQL. Crunchy Data Warehouse extends unmodified PostgreSQL to provide support for fully transactional Iceberg tables and high performance analytics. Crunchy Data Warehouse is available as a managed service on AWS via [Crunchy Bridge](https://www.crunchydata.com/products/crunchy-bridge), fully managed PostgreSQL as a service. Crunchy Data Warehouse can create Iceberg tables directly from a PostgreSQL database or external URLs and can read, query, and update Iceberg tables using PostgreSQL syntax. Using a hybrid query engine that combines PostgreSQL and DuckDB, Crunchy Data Warehouse enables high performance analytical queries of Iceberg tables.

### [Databricks](https://www.databricks.com/)

[Databricks](https://www.databricks.com/) uses an open lakehouse architecture to power its Data Intelligence Platform and provide a unified foundation for all data and governance, combined with AI models tuned to an organization’s unique characteristics. Through [Unity Catalog](https://www.databricks.com/product/unity-catalog), users can manage and govern all structured data, unstructured data, business metrics and AI models across open data formats like Delta Lake, Apache Iceberg, Hudi, Parquet and more.

### [dltHub](https://dlthub.com/)

[dlt](https://dlthub.com/docs/intro) is an open-source Python library for building production-grade extract & load pipelines. It automates the tedious parts of ELT, letting you load any data source into Apache Iceberg with minimal code. dlt eliminates boilerplate and makes data ingestion robust against evolving and unpredictable data sources.
 
* [Pythonic pipelines](https://dlthub.com/docs/tutorial/load-data-from-an-api): Define Iceberg ingestion with simple Python functions that are testable and CI/CD-friendly.
* [Automated schema management](https://dlthub.com/docs/general-usage/schema-contracts): Infers and evolves Iceberg table schemas on the fly, adapting automatically to source changes.
* [Catalog support](https://dlthub.com/docs/plus/ecosystem/iceberg#configuration): Works with SQL-based (SQLite, PostgreSQL), REST (Lakekeeper, Polaris), and cloud-native options like AWS Glue, Databricks Unity Catalog and Snowflake Open Catalog.   
* [Flexible deployment](https://dlthub.com/docs/walkthroughs/share-a-dataset): Run Iceberg pipelines anywhere - local, Docker, Airflow, or serverless.    

Go from an API to a versioned Iceberg table in minutes [here](https://dlthub.com/docs/dlt-ecosystem/destinations/iceberg).

### [Dremio](https://www.dremio.com/)

With Dremio, an organization can easily build and manage a data lakehouse in which data is stored in open formats like Apache Iceberg and can be processed with Dremio’s interactive SQL query engine and non-Dremio processing engines. [Dremio Cloud](https://www.dremio.com/get-started/) provides these capabilities in a fully managed offering.

* [Dremio Sonar](https://www.dremio.com/platform/sonar/) is a lakehouse query engine that provides interactive performance and DML on Apache Iceberg, as well as other formats and data sources.
* [Dremio Arctic](https://www.dremio.com/platform/arctic/) is a lakehouse catalog and optimization service for Apache Iceberg. Arctic automatically optimizes tables in the background to ensure high-performance access for any engine. Arctic also simplifies experimentation, data engineering, and data governance by providing Git concepts like branches and tags on Apache Iceberg tables.

### [Estuary](https://estuary.dev)

A low-latency, high-fidelity data movement platform, Estuary lets developers quickly set up pipelines to connect their entire data architecture. Intelligent schema inference and evolution determines field data types based on usage and keeps pipelines running when fields change. Flexible [deployment options](https://estuary.dev/deployment-options/) include public, private, and BYOC (Bring Your Own Cloud) for a range of compliance and privacy-oriented use cases.

Estuary's catalog of pre-built data connectors provides integrations with databases, APIs, event logs, and more. [Apache Iceberg](https://estuary.dev/solutions/technology/apache-iceberg/) is a primary destination option with two configurable materializations: one that [merges](https://docs.estuary.dev/reference/Connectors/materialization-connectors/apache-iceberg/) new updates and one that simply [appends](https://docs.estuary.dev/reference/Connectors/materialization-connectors/amazon-s3-iceberg/) them.

### [Firebolt](https://www.firebolt.io)

[Firebolt](https://www.firebolt.io) is a cloud data warehouse built to power data-intensive applications that demand low latency and high concurrency. It is optimized for reading Apache Iceberg tables with sub-second performance and integrates seamlessly with major Iceberg catalogs.

Firebolt is also available as [Firebolt Core](https://docs.firebolt.io/firebolt-core), a free, self-hosted edition of its distributed query engine.

Learn more about querying Iceberg with Firebolt [here](https://www.firebolt.io/blog/querying-apache-iceberg-with-sub-second-performance).

### [IBM watsonx.data](https://www.ibm.com/products/watsonx-data)

[IBM watsonx.data](https://www.ibm.com/products/watsonx-data) is an open data lakehouse for AI and analytics. It uses Apache Iceberg as a core table format, providing features like schema evolution, time travel, and partitioning. This allows developers to easily work with large, complex data sets while ensuring efficient performance and flexibility. watsonx.data simplifies the integration of Iceberg tables, making it easy to manage data across different environments and query historical data without disruption. 

Developers can leverage the benefits of Iceberg tables and take advantage of high performance compute capabilities like [Velox](https://velox-lib.io/), [Presto](https://prestodb.io/), [Apache Gluten](https://gluten.apache.org/), which are part of the watsonx.data ecosystem.

### [IOMETE](https://iomete.com/)

IOMETE is a fully-managed ready to use, batteries included Data Platform. IOMETE optimizes clustering, compaction, and access control to Apache Iceberg tables. Customer data remains on customer's account to prevent vendor lock-in. The core of IOMETE platform is a serverless Lakehouse that leverages Apache Iceberg as its core table format. IOMETE platform also includes Serverless Spark, an SQL Editor, A Data Catalog, and granular data access control. IOMETE supports Hybrid-multi-cloud setups. 

### [PuppyGraph](https://puppygraph.com)

PuppyGraph is a cloud-native graph analytics engine that enables users to query one or more relational data stores as a unified graph model. This eliminates the overhead of deploying and maintaining a siloed graph database system, with no ETL required. [PuppyGraph’s native Apache Iceberg integration](https://docs.puppygraph.com/user-manual/getting-started/iceberg) adds native graph capabilities to your existing data lake in an easy and performant way.

### [Redpanda](https://redpanda.com)

Redpanda is both a cloud-native and self-hosted streaming platform whose [Iceberg topics](https://www.redpanda.com/use-case/streaming-iceberg-tables) automatically transform Kafka messages into Iceberg tables in real-time. This allows users to query their Kafka data as part of an established Iceberg deployment, no connectors or additional technology required. Redpanda Iceberg integrates with an expanding list of Iceberg catalogs and query engines, including many listed here.

### [RisingWave](https://risingwave.com/)

[RisingWave](https://risingwave.com/) is a cloud-native streaming database for real-time data ingestion, processing, and management. It integrates with Iceberg to [read from](https://docs.risingwave.com/integrations/sources/apache-iceberg) and [write to](https://docs.risingwave.com/integrations/destinations/apache-iceberg) Iceberg tables, enabling efficient file compaction across sources like message queues, databases (via Change Data Capture), data lakes, and files. RisingWave is available as [open source](https://github.com/risingwavelabs/risingwave), a managed cloud service ([RisingWave Cloud](https://cloud.risingwave.com/auth/signin/)) with BYOC support, and an enterprise on-premises edition ([RisingWave Premium](https://docs.risingwave.com/get-started/rw-premium-edition-intro)).

### [Ryft](https://ryft.io/)

[Ryft](https://ryft.io/) is a fully automated Iceberg management platform. Ryft helps data teams create an open, automated and cost-effective Iceberg lakehouse, by maintaining and optimizing Iceberg tables in real time, based on actual usage patterns. The Ryft engine runs compaction intelligently, adapting to different use cases like streaming, batch jobs, CDC, and more. Ryft also automates compliance, disaster recovery and data lifecycle management for Iceberg tables, to ensure your lakehouse stays secure and compliant. It directly integrates with your existing catalog, storage and query engines, allowing for a very simple deployment.


### [SingleStore](https://singlestore.com/)

SingleStore is a high‑performance, scalable, distributed SQL platform that makes real‑time analytics and transactional processing available at scale. Its native Apache Iceberg integration removes costly ETL steps and powers intelligent, millisecond‑response applications.

By directly reading and [managing](https://docs.singlestore.com/cloud/load-data/data-sources/iceberg-ingest/) data from Iceberg tables, SingleStore unlocks enterprises' dormant data, boost generative AI development, and ensure seamless schema evolution with low‑latency queries. Available [self-managed](https://docs.singlestore.com/db/v8.9/) or in the cloud, it bridges the gap between traditional data lakes and real‑time analytics.

<!-- markdown-link-check-disable-next-line -->
### [Snowflake](https://snowflake.com/)

<!-- markdown-link-check-disable-next-line -->
[Snowflake](https://www.snowflake.com/) is a single, cross-cloud platform that enables every organization to mobilize their data with Snowflake’s Data Cloud. Snowflake supports Apache Iceberg by offering [Snowflake-managed Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg#use-snowflake-as-the-iceberg-catalog) for full DML as well as [externally managed Iceberg Tables with catalog integrations](https://docs.snowflake.com/en/user-guide/tables-iceberg#use-a-catalog-integration) for read-only access.

### [Stackable](https://stackable.tech)

Stackable is the provider of the Stackable Data Platform - a modular, open source data platform for innovative data applications.

True to the philosophy of 'your data, your platform', Stackable enables the creation of flexible, scalable data architectures for data meshes, data lakehouses, event streaming, machine learning, and with the seamless integration of Apache Iceberg via [Trino](https://trino.io), [Apache NiFi](https://nifi.apache.org) and [Apache Spark](https://spark.apache.org)

The Stackable Data Platform is completely open source, providing maximum portability without vendor lock-in. It also enables true data sovereignty - whether in the private or public cloud. With 24/7 support and strict SLAs, Stackable guarantees stability and efficiency - modern, flexible and secure.

### [Starburst](https://starburst.io)

Starburst is a commercial offering for the [Trino query engine](https://trino.io). Trino is a distributed MPP SQL query engine that can query data in Iceberg at interactive speeds. Trino also enables you to join Iceberg tables with an [array of other systems](https://trino.io/docs/current/connector.html). Starburst offers both an [enterprise deployment](https://www.starburst.io/platform/starburst-enterprise/) and a [fully managed service](https://www.starburst.io/platform/starburst-galaxy/) to make managing and scaling Trino a flawless experience. Starburst also provides customer support and houses many of the original contributors to the open-source project that know Trino best. Learn more about [the Starburst Iceberg connector](https://docs.starburst.io/latest/connector/iceberg.html).

### [Tinybird](https://tinybird.co)

[Tinybird](https://tinybird.co) is a real-time data platform that lets developers and data teams build fast APIs on top of analytical data using SQL. It now offers native support for Apache Iceberg through ClickHouse’s [iceberg() table function](https://www.tinybird.co/docs/forward/get-data-in/table-functions/iceberg), allowing seamless querying of Iceberg tables stored in S3.

This integration enables low-latency, high-concurrency access to Iceberg data, with Tinybird handling ingestion, transformation, and API publishing. Developers can now leverage Iceberg for open storage and governance, while using Tinybird for blazing-fast query performance and real-time delivery.

Learn more in the [Tinybird documentation](https://www.tinybird.co/docs/forward/get-data-in/table-functions/iceberg).

### [Upsolver](https://upsolver.com)

[Upsolver](https://upsolver.com) is a streaming data ingestion and table management solution for Apache Iceberg. With Upsolver, users can easily ingest batch and streaming data from files, streams and databases (CDC) into [Iceberg tables](https://docs.upsolver.com/content/reference-1/sql-commands/iceberg-tables/upsolver-managed-tables). In addition, Upsolver connects to your existing REST and Hive catalogs, and [analyzes the health](https://docs.upsolver.com/content/how-to-guides-1/apache-iceberg/optimize-your-iceberg-tables) of your tables. Use Upsolver to continuously optimize tables by compacting small files, sorting and compressing, repartitioning, and cleaning up dangling files and expired manifests. Upsolver is available from the [Upsolver Cloud](https://www.upsolver.com/sqlake-signup-wp) or can be deployed in your AWS VPC.

### [VeloDB](https://velodb.io)

[VeloDB](https://www.velodb.io/) is a commercial data warehouse powered by [Apache Doris](https://doris.apache.org/), an open-source, real-time data warehouse. It also provides powerful [query acceleration for Iceberg tables and efficient data writeback](https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog). VeloDB offers [enterprise version](https://www.velodb.io/enterprise) and [cloud service](https://www.velodb.io/cloud), which are fully compatible with open-source Apache Doris. Quick start with Apache Doris and Apache Iceberg [here](https://doris.apache.org/docs/lakehouse/lakehouse-best-practices/doris-iceberg).

### [OLake](https://olake.io/)

[OLake](https://olake.io/) is an open-source ELT tool to facilitate the replication of databases into Apache Iceberg™ data lakehouses. It offers native integration with PostgreSQL, MySQL, MongoDB, Oracle, and Kafka, enabling real-time data ingestion without the need for intermediary layers like Debezium, Kafka or Spark. The platform's modular architecture supports full-load operations, continuous Change Data Capture (CDC), and incremental synchronization with bookmark/cursor column support, with resumable syncs and schema evolution handling. By employing a parallelized chunking strategy, OLake accelerates initial syncs, while CDC cursor preservation ensures that incremental updates capture all events.

Learn more in the [OLake documentation](https://olake.io/docs) and explore the [Github repository](https://github.com/datazip-inc/olake).
