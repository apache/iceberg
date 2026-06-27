---
title: "Catalogs"
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

# Catalogs

## What is an Iceberg Catalog?

You may think of Iceberg as a format for managing data in a single table, but the Iceberg library needs a way to keep track of those tables by name. Tasks like creating, dropping, and renaming tables are the responsibility of a **catalog**. Catalogs manage a collection of tables that are usually grouped into namespaces. The most important responsibility of a catalog is tracking a table's current metadata, which is provided by the catalog when you load a table.

The first step when using an Iceberg client is almost always initializing and configuring a catalog. The configured catalog is then used by compute engines to execute catalog operations. Multiple types of compute engines using a shared Iceberg catalog allows them to share a common data layer.

!!! note
    The term "catalog" is overloaded in the data ecosystem. In Iceberg, a catalog specifically refers to the component that **tracks table metadata locations** — it maps table names to the location of the table's current metadata file. This is different from how some query engines use "catalog" to describe a broader namespace or data source configuration.

## How Catalogs Work

An Iceberg catalog stores the mapping between table identifiers (such as `db.my_table`) and the location of each table's current metadata file. When a query engine needs to read or write an Iceberg table, it asks the catalog for the metadata location and then interacts with the table's data and metadata files directly in storage.

This separation of concerns means that:

- **The catalog is lightweight** — it tracks metadata pointers, not the data itself.
- **Multiple engines can share tables** — any engine that can talk to the same catalog can read and write the same tables.
- **Storage is independent** — tables can live in S3, GCS, ADLS, HDFS, or local storage regardless of which catalog is used.

## Catalog Types

Iceberg catalogs are flexible and can be implemented using almost any backend system. They can be plugged into any Iceberg runtime and allow any processing engine that supports Iceberg to load the tracked Iceberg tables.

### REST Catalog

The REST catalog uses a server-side catalog exposed through the [REST Catalog API](rest-catalog-spec.md). Instead of using technology-specific logic contained in the catalog clients, the implementation details of a REST catalog live on the catalog server. The server-side logic can be written in any language and use any custom technology, as long as the API follows the Iceberg REST Open API specification.

The REST catalog was introduced in the Iceberg 0.14.0 release and has become widely adopted due to several key advantages:

- **Language and engine compatibility**: New languages and engines can support any catalog with just one client implementation.
- **Improved reliability**: Change-based commits enable server-side conflict resolution and retries.
- **Simplified metadata management**: Metadata version upgrades are easier because root metadata is written by the catalog service.
- **Advanced features**: Enables lazy snapshot loading, multi-table commits, and caching.
- **Security**: Supports secure table sharing using credential vending or remote signing.

REST catalog implementations include [Apache Polaris](https://polaris.apache.org/), [Apache Gravitino](https://gravitino.apache.org/), [Lakekeeper](https://github.com/lakekeeper/lakekeeper), and others. You can also use the REST catalog protocol with any built-in catalog using translation in the `CatalogHandlers` class or using the community-maintained [`iceberg-rest-fixture`](https://hub.docker.com/r/apache/iceberg-rest-fixture) docker image.

For the full protocol specification, see the [REST Catalog Spec](rest-catalog-spec.md).

### Hive Metastore Catalog

The Hive Metastore catalog uses an existing [Hive Metastore](https://hive.apache.org/) instance to track Iceberg table metadata. This is a common choice for organizations that already operate a Hive Metastore.

For detailed configuration, see the [Hive integration documentation](docs/latest/hive.md).

### JDBC Catalog

The JDBC catalog stores its tracking information in a relational database using JDBC. This is a straightforward option when you have an existing relational database and want to avoid operating additional infrastructure.

For configuration properties and examples, see the [JDBC documentation](docs/latest/jdbc.md).

### Hadoop Catalog

The Hadoop catalog tracks tables using the file system directly. It does not require an external service, but it does not support atomic transactions without a locking mechanism.

!!! warning
    The Hadoop catalog does not support concurrent writes from multiple processes without configuring a lock manager. It is typically used for local development and testing rather than production workloads.

For details, see the [HadoopCatalog Javadoc](https://iceberg.apache.org/javadoc/nightly/org/apache/iceberg/hadoop/HadoopCatalog.html).

### Nessie Catalog

[Nessie](https://projectnessie.org/) is a transactional catalog that provides git-like version control for your data lake. It enables branching and tagging of table states, which is useful for experimentation, rollbacks, and multi-table transactions.

For configuration and examples, see the [Nessie integration documentation](docs/latest/nessie.md).

### AWS Glue Catalog

The [AWS Glue Data Catalog](https://aws.amazon.com/glue/) can serve as an Iceberg catalog. This is a common choice for organizations already using the AWS analytics ecosystem including Athena, EMR, Redshift, and Lake Formation.

For configuration and examples, see the [AWS integration documentation](docs/latest/aws.md).

### AWS DynamoDB Catalog

[Amazon DynamoDB](https://aws.amazon.com/dynamodb/) can be used as an Iceberg catalog backend. It supports optimistic locking for concurrent writes and allows you to build secondary indexes on arbitrary table properties for efficient catalog queries.

For configuration and examples, see the [AWS integration documentation](docs/latest/aws.md).

## Choosing a Catalog

The right catalog depends on your existing infrastructure, performance needs, and operational requirements. Here are some guidelines:

| Consideration | Recommended Catalog |
|---|---|
| New deployment with no existing infrastructure | REST catalog |
| Multi-engine or multi-language environment | REST catalog |
| Already using AWS analytics ecosystem (Glue, Athena, EMR) | AWS Glue |
| Already running a Hive Metastore | Hive Metastore |
| Existing relational database, minimal infrastructure | JDBC |
| Need git-like versioning and branching for data | Nessie |
| High-throughput streaming writes on AWS | AWS Glue or DynamoDB |
| Local development and testing | Hadoop |

!!! tip
    The REST catalog protocol is the most flexible option for new projects. It decouples your engine configuration from the catalog backend, meaning you can change the server-side implementation without updating any client configurations. Many organizations are migrating to REST-based catalogs for this reason.

## Configuring a Catalog

A catalog is almost always configured through the processing engine, which passes a set of properties during initialization. Different engines have different ways to configure a catalog. When configuring a catalog, refer to the [catalog properties documentation](docs/latest/configuration.md#catalog-properties) as well as the documentation for the specific processing engine being used.

### Spark Example

```shell
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
          --conf spark.sql.catalog.my_catalog.type=rest \
          --conf spark.sql.catalog.my_catalog.uri=https://my-catalog-server:8181
```

### Java API Example

```java
Map<String, String> properties = new HashMap<>();
properties.put(CatalogProperties.URI, "https://my-catalog-server:8181");

RESTCatalog catalog = new RESTCatalog();
catalog.initialize("my_catalog", properties);
```

## Third-Party Catalog Implementations

In addition to the catalog implementations included with the Iceberg library, several third-party projects provide catalog services:

| Catalog | Description |
|---|---|
| [Apache Polaris](https://polaris.apache.org/) | Open-source REST catalog with role-based access control, credential vending, and multi-engine support. |
| [Apache Gravitino](https://gravitino.apache.org/) | Metadata management platform supporting multiple catalog backends including Hive, JDBC, and REST. |
| [Lakekeeper](https://github.com/lakekeeper/lakekeeper) | Lightweight REST catalog written in Rust. Single-binary deployment with no JVM dependency. |
| [Boring Catalog](https://github.com/boringdata/boring-catalog) | Simple, opinionated REST catalog for Iceberg. |
| [DataHub](https://docs.datahub.com/docs/iceberg-catalog) | Data platform with integrated Iceberg catalog support. |
| [Google BigLake metastore](https://cloud.google.com/bigquery/docs/blms-manage-resources) | Google Cloud-native metastore with Iceberg catalog capabilities. |

## Custom Catalogs

If none of the built-in or third-party catalogs meet your needs, you can implement a custom catalog. Iceberg provides interfaces that allow you to build your own catalog backed by any storage system.

For implementation details, see the [Custom Catalog documentation](docs/latest/custom-catalog.md).
