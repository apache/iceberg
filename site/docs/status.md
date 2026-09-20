---
title: "Implementation Status"
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

# Implementation Status

Apache Iceberg's table specification is implemented in multiple languages. This page provides an overview of the current
capabilities.

## Libraries

This section lists the libraries that implement the Apache Iceberg specification.

| Library | Released Version |
|---------|------------------|
| [Java](https://mvnrepository.com/artifact/org.apache.iceberg) | [1.11.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.11.0) |
| [PyIceberg](https://pypi.org/project/pyiceberg/) | [0.12.0](https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.12.0) |
| [Rust](https://crates.io/crates/iceberg) | [0.10.1](https://github.com/apache/iceberg-rust/releases/tag/v0.10.1) |
| [Go](https://pkg.go.dev/github.com/apache/iceberg-go) | [0.6.0](https://github.com/apache/iceberg-go/releases/tag/v0.6.0) |
| [C++](https://github.com/apache/iceberg-cpp/releases) | [0.3.0](https://github.com/apache/iceberg-cpp/releases/tag/v0.3.0) |

## Data Types

| Data Type      | Spec | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|------|-----------|------|----|-----|
| boolean        | V1+  | Y    | Y         | Y    | Y  | Y   |
| int            | V1+  | Y    | Y         | Y    | Y  | Y   |
| long           | V1+  | Y    | Y         | Y    | Y  | Y   |
| float          | V1+  | Y    | Y         | Y    | Y  | Y   |
| double         | V1+  | Y    | Y         | Y    | Y  | Y   |
| decimal        | V1+  | Y    | Y         | Y    | Y  | Y   |
| date           | V1+  | Y    | Y         | Y    | Y  | Y   |
| time           | V1+  | Y    | Y         | Y    | Y  | Y   |
| timestamp      | V1+  | Y    | Y         | Y    | Y  | Y   |
| timestamptz    | V1+  | Y    | Y         | Y    | Y  | Y   |
| timestamp_ns   | V3+  | Y    | Y         | Y    | Y  | Y   |
| timestamptz_ns | V3+  | Y    | Y         | Y    | Y  | Y   |
| unknown        | V3+  | Y    | Y         | N    | Y  | Y   |
| string         | V1+  | Y    | Y         | Y    | Y  | Y   |
| uuid           | V1+  | Y    | Y         | Y    | Y  | N   |
| fixed          | V1+  | Y    | Y         | Y    | Y  | Y   |
| binary         | V1+  | Y    | Y         | Y    | Y  | Y   |
| variant        | V3+  | Y    | Y         | N    | Y  | N   |
| geometry       | V3+  | Y    | Y         | N    | Y  | N   |
| geography      | V3+  | Y    | Y         | N    | Y  | N   |
| list           | V1+  | Y    | Y         | Y    | Y  | Y   |
| map            | V1+  | Y    | Y         | Y    | Y  | Y   |
| struct         | V1+  | Y    | Y         | Y    | Y  | Y   |

## Table Metadata and Features

The `Spec` column identifies the applicable table format versions. A trailing `+` means that the feature is supported
in that version and later versions, while a range means that the status is the same for every listed version. `Y`
indicates end-to-end support for the listed behavior, not only support for serializing its metadata fields.

| Capability                       | Spec  | Java | PyIceberg | Rust | Go | C++ |
|----------------------------------|-------|------|-----------|------|----|-----|
| Read table metadata              | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Read table metadata              | V3    | Y    | Y         | Y    | Y  | N   |
| Write table metadata             | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Write table metadata             | V3    | Y    | N         | Y    | Y  | N   |
| Read initial column defaults     | V3    | Y    | Y         | Y    | Y  | N   |
| Read multi-argument transforms   | V3    | N    | N         | N    | Y  | N   |
| Write multi-argument transforms  | V3    | N    | N         | N    | Y  | N   |
| Read row lineage columns         | V3    | Y    | N         | N    | Y  | N   |
| Write row lineage                | V3    | Y    | N         | N    | Y  | N   |
| Read encrypted tables            | V3    | Y    | N         | N    | N  | N   |
| Write encrypted tables           | V3    | Y    | N         | N    | N  | N   |

## Data File Formats

| Format  | Java | PyIceberg | Rust | Go | C++ |
|---------|------|-----------|------|----|-----|
| Parquet | Y    | Y         | Y    | Y  | Y   |
| ORC     | Y    | N         | N    | N  | N   |
| Puffin  | Y    | N         | N    | N  | N   |
| Avro    | Y    | N         | N    | N  | Y   |

## File IO

| Storage           | Java | PyIceberg | Rust | Go | C++  |
|-------------------|------|-----------|------|----|------|
| Local Filesystem  | Y    | Y         | Y    | Y  | Y    |
| Hadoop Filesystem | Y    | Y         | Y    | Y  | N    |
| S3 Compatible     | Y    | Y         | Y    | Y  | N    |
| GCS Compatible    | Y    | Y         | Y    | Y  | N    |
| ADLS Compatible   | Y    | Y         | Y    | Y  | N    |

## Table Maintenance Operations

| Operation                   | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|-------|------|-----------|------|----|-----|
| Update schema               | V1+   | Y    | Y         | Y    | Y  | Y   |
| Update partition spec       | V1+   | Y    | Y         | N    | Y  | Y   |
| Update table properties     | V1+   | Y    | Y         | Y    | Y  | Y   |
| Replace sort order          | V1+   | Y    | Y         | Y    | Y  | Y   |
| Update table location       | V1+   | Y    | Y         | Y    | Y  | Y   |
| Update statistics           | V1+   | Y    | Y         | Y    | Y  | Y   |
| Update partition statistics | V1+   | Y    | N         | N    | Y  | Y   |
| Expire snapshots            | V1+   | Y    | Y         | Y    | Y  | Y   |
| Manage snapshots            | V1+   | Y    | Y         | N    | Y  | Y   |

## Table Update Operations

| Operation         | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-------------------|-------|------|-----------|------|----|-----|
| Append data files | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Append data files | V3    | Y    | N         | Y    | Y  | Y   |
| Rewrite files     | V1-V2 | Y    | Y         | N    | Y  | N   |
| Rewrite files     | V3    | Y    | N         | N    | Y  | N   |
| Rewrite manifests | V1-V2 | Y    | Y         | N    | Y  | N   |
| Rewrite manifests | V3    | Y    | N         | N    | Y  | N   |
| Overwrite files   | V1-V2 | Y    | Y         | N    | Y  | N   |
| Overwrite files   | V3    | Y    | N         | N    | Y  | N   |
| Delete files      | V1-V2 | Y    | Y         | N    | Y  | N   |
| Delete files      | V3    | Y    | N         | N    | Y  | N   |
| Row delta         | V2+   | Y    | N         | N    | Y  | N   |

## Table Read Operations

| Operation                   | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|-------|------|-----------|------|----|-----|
| Plan with data file         | V1+   | Y    | Y         | Y    | Y  | Y   |
| Plan with position deletes  | V2+   | Y    | Y         | Y    | Y  | Y   |
| Plan with equality deletes  | V2+   | Y    | Y         | Y    | Y  | Y   |
| Plan with deletion vectors  | V3    | Y    | N         | N    | Y  | Y   |
| Plan with puffin statistics | V1+   | Y    | N         | N    | N  | N   |
| Read data file              | V1+   | Y    | Y         | Y    | Y  | Y   |
| Read with position deletes  | V2+   | Y    | Y         | Y    | Y  | Y   |
| Read with equality deletes  | V2+   | Y    | N         | Y    | Y  | Y   |
| Read with deletion vectors  | V3    | Y    | N         | N    | Y  | N   |

## Table Write Operations

| Operation              | Spec  | Java | PyIceberg | Rust | Go | C++ |
|------------------------|-------|------|-----------|------|----|-----|
| Append data            | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Append data            | V3    | Y    | N         | Y    | Y  | Y   |
| Write position deletes | V2    | Y    | N         | N    | Y  | Y   |
| Write equality deletes | V2-V3 | Y    | N         | Y    | Y  | Y   |
| Write deletion vectors | V3    | Y    | N         | N    | Y  | N   |

## Catalogs

### Rest Catalog

#### Table Operations

| Table Operation | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------|-------|------|-----------|------|----|-----|
| listTable       | V1+   | Y    | Y         | Y    | Y  | Y   |
| createTable     | V1+   | Y    | Y         | Y    | Y  | Y   |
| dropTable       | V1+   | Y    | Y         | Y    | Y  | Y   |
| loadTable       | V1+   | Y    | Y         | Y    | Y  | Y   |
| updateTable     | V1+   | Y    | Y         | Y    | Y  | Y   |
| renameTable     | V1+   | Y    | Y         | Y    | Y  | Y   |
| tableExists     | V1+   | Y    | Y         | Y    | Y  | Y   |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| createView     | Y    | N         | N    | Y  | N   |
| dropView       | Y    | Y         | N    | Y  | N   |
| listView       | Y    | Y         | N    | Y  | N   |
| viewExists     | Y    | Y         | N    | Y  | N   |
| replaceView    | Y    | N         | N    | N  | N   |
| renameView     | Y    | N         | N    | N  | N   |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go | C++ |
|---------------------------|------|-----------|------|----|-----|
| listNamespaces            | Y    | Y         | Y    | Y  | Y   |
| createNamespace           | Y    | Y         | Y    | Y  | Y   |
| dropNamespace             | Y    | Y         | Y    | Y  | Y   |
| namespaceExists           | Y    | Y         | Y    | Y  | Y   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | Y   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | Y   |

### Sql Catalog

The sql catalog is a catalog backed by a sql database, which is called jdbc catalog in java.

| Database | Java | PyIceberg | Rust | Go | C++ |
|----------|------|-----------|------|----|-----|
| Postgres | Y    | Y         | Y    | Y  | N   |
| MySQL    | Y    | Y         | Y    | Y  | N   |
| SQLite   | Y    | Y         | Y    | Y  | N   |

#### Table Operations

| Table Operation | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------|-------|------|-----------|------|----|-----|
| listTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| createTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1+   | Y    | Y         | Y    | Y  | N   |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| createView     | Y    | N         | N    | Y  | N   |
| dropView       | Y    | N         | N    | Y  | N   |
| listView       | Y    | N         | N    | Y  | N   |
| viewExists     | Y    | N         | N    | Y  | N   |
| replaceView    | Y    | N         | N    | N  | N   |
| renameView     | Y    | N         | N    | N  | N   |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go | C++ |
|---------------------------|------|-----------|------|----|-----|
| listNamespaces            | Y    | Y         | Y    | Y  | N   |
| createNamespace           | Y    | Y         | Y    | Y  | N   |
| dropNamespace             | Y    | Y         | Y    | Y  | N   |
| namespaceExists           | Y    | N         | Y    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | N   |

### Glue Catalog

#### Table Operations

| Table Operation | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------|-------|------|-----------|------|----|-----|
| listTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| createTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1+   | Y    | Y         | Y    | Y  | N   |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| createView     | Y    | N         | N    | N  | N   |
| dropView       | Y    | N         | N    | N  | N   |
| listView       | Y    | N         | N    | N  | N   |
| viewExists     | Y    | N         | N    | N  | N   |
| replaceView    | Y    | N         | N    | N  | N   |
| renameView     | Y    | N         | N    | N  | N   |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go | C++ |
|---------------------------|------|-----------|------|----|-----|
| listNamespaces            | Y    | Y         | Y    | Y  | N   |
| createNamespace           | Y    | Y         | Y    | Y  | N   |
| dropNamespace             | Y    | Y         | Y    | Y  | N   |
| namespaceExists           | Y    | N         | Y    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | N   |

### Hive Metastore Catalog

#### Table Operations

| Table Operation | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------|-------|------|-----------|------|----|-----|
| listTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| createTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1+   | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1+   | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1+   | Y    | Y         | Y    | Y  | N   |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| createView     | Y    | N         | N    | Y  | N   |
| dropView       | Y    | N         | N    | Y  | N   |
| listView       | Y    | N         | N    | Y  | N   |
| viewExists     | Y    | N         | N    | Y  | N   |
| replaceView    | Y    | N         | N    | N  | N   |
| renameView     | Y    | N         | N    | N  | N   |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go | C++ |
|---------------------------|------|-----------|------|----|-----|
| listNamespaces            | Y    | Y         | Y    | Y  | N   |
| createNamespace           | Y    | Y         | Y    | Y  | N   |
| dropNamespace             | Y    | Y         | Y    | Y  | N   |
| namespaceExists           | Y    | N         | Y    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | N   |
