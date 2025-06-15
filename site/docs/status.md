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

- [Java](https://mvnrepository.com/artifact/org.apache.iceberg)
- [PyIceberg](https://pypi.org/project/pyiceberg/)
- [Rust](https://crates.io/crates/iceberg)
- [Go](https://pkg.go.dev/github.com/apache/iceberg-go)

## Data Types

| Data Type      | Java | PyIceberg | Rust | Go |
|----------------|------|-----------|------|----|
| boolean        | Y    | Y         | Y    | Y  |
| int            | Y    | Y         | Y    | Y  |
| long           | Y    | Y         | Y    | Y  |
| float          | Y    | Y         | Y    | Y  |
| double         | Y    | Y         | Y    | Y  |
| decimal        | Y    | Y         | Y    | Y  |
| date           | Y    | Y         | Y    | Y  |
| time           | Y    | Y         | Y    | Y  |
| timestamp      | Y    | Y         | Y    | Y  |
| timestamptz    | Y    | Y         | Y    | Y  |
| timestamp_ns   | Y    | Y         | Y    | Y  |
| timestamptz_ns | Y    | Y         | Y    | Y  |
| string         | Y    | Y         | Y    | Y  |
| uuid           | Y    | Y         | Y    | Y  |
| fixed          | Y    | Y         | Y    | Y  |
| binary         | Y    | Y         | Y    | Y  |
| variant        | Y    | Y         | Y    | Y  |
| list           | Y    | Y         | Y    | Y  |
| map            | Y    | Y         | Y    | Y  |
| struct         | Y    | Y         | Y    | Y  |

## Data File Formats

| Format  | Java | PyIceberg | Rust | Go |
|---------|------|-----------|------|----|
| Parquet | Y    | Y         | Y    | Y  |
| ORC     | Y    | N         | N    | N  |
| Puffin  | Y    | N         | N    | N  |
| Avro    | Y    | N         | N    | N  |

## File IO

| Storage           | Java | PyIceberg | Rust | Go |
|-------------------|------|-----------|------|----|
| Local Filesystem  | Y    | Y         | Y    | Y  |
| Hadoop Filesystem | Y    | Y         | Y    | Y  |
| S3 Compatible     | Y    | Y         | Y    | Y  |
| GCS Compatible    | Y    | Y         | Y    | Y  |
| ADLS Compatible   | Y    | Y         | Y    | Y  |

## Table Maintenance Operations

### Table Spec V1

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Update schema               | Y    | Y         | Y    | N  |
| Update partition spec       | Y    | Y         | Y    | N  |
| Update table properties     | Y    | Y         | Y    | Y  |
| Replace sort order          | Y    | N         | N    | N  |
| Update table location       | Y    | Y         | N    | N  |
| Update statistics           | Y    | Y         | N    | N  |
| Update partition statistics | Y    | N         | N    | N  |
| Expire snapshots            | Y    | N         | N    | N  |
| Manage snapshots            | Y    | N         | N    | N  |

### Table Spec V2

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Update schema               | Y    | Y         | N    | N  |
| Update partition spec       | Y    | Y         | N    | N  |
| Update table properties     | Y    | Y         | Y    | Y  |
| Replace sort order          | Y    | N         | N    | N  |
| Update table location       | Y    | Y         | N    | N  |
| Update statistics           | Y    | Y         | N    | N  |
| Update partition statistics | Y    | N         | N    | N  |
| Expire snapshots            | Y    | N         | N    | N  |
| Manage snapshots            | Y    | N         | N    | N  |

## Table Update Operations

### Table Spec V1

| Operation         | Java | PyIceberg | Rust | Go |
|-------------------|------|-----------|------|----|
| Append data files | Y    | Y         | N    | Y  |
| Rewrite files     | Y    | Y         | N    | N  |
| Rewrite manifests | Y    | Y         | N    | Y  |
| Overwrite files   | Y    | Y         | N    | N  |
| Delete files      | Y    | Y         | N    | N  |

### Table Spec V2

| Operation         | Java | PyIceberg | Rust | Go |
|-------------------|------|-----------|------|----|
| Append data files | Y    | Y         | N    | Y  |
| Rewrite files     | Y    | Y         | N    | N  |
| Rewrite manifests | Y    | Y         | N    | Y  |
| Overwrite files   | Y    | Y         | N    | N  |
| Row delta         | Y    | N         | N    | N  |
| Delete files      | Y    | Y         | N    | N  |

## Table Read Operations

### Table Spec V1

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Plan with data file         | Y    | Y         | Y    | Y  |
| Plan with puffin statistics | Y    | Y         | Y    | Y  |
| Read data file              | Y    | N         | Y    | Y  |

### Table Spec V2

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Plan with data file         | Y    | Y         | Y    | Y  |
| Plan with position deletes  | Y    | Y         | N    | Y  |
| Plan with equality deletes  | Y    | Y         | N    | N  |
| Plan with puffin statistics | Y    | N         | N    | N  |
| Read data file              | Y    | Y         | Y    | Y  |
| Read with position deletes  | Y    | Y         | N    | Y  |
| Read with equality deletes  | Y    | N         | N    | N  |

## Table Write Operations

### Table Spec V1

| Operation   | Java | PyIceberg | Rust | Go |
|-------------|------|-----------|------|----|
| Append data | Y    | Y         | Y    | Y  |

### Table Spec V2

| Operation              | Java | PyIceberg | Rust | Go |
|------------------------|------|-----------|------|----|
| Append data            | Y    | Y         | Y    | Y  |
| Write position deletes | Y    | N         | N    | N  |
| Write equality deletes | Y    | N         | N    | N  |

## Catalogs

### Rest Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go |
|----------------|------|-----------|------|----|
| createView     | Y    | N         | N    | N  |
| dropView       | Y    | Y         | N    | N  |
| listView       | Y    | Y         | N    | N  |
| viewExists     | Y    | Y         | N    | N  |
| replaceView    | Y    | N         | N    | N  |
| renameView     | Y    | N         | N    | N  |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go |
|---------------------------|------|-----------|------|----|
| listNamespaces            | Y    | Y         | Y    | Y  |
| createNamespace           | Y    | Y         | Y    | Y  |
| dropNamespace             | Y    | Y         | Y    | Y  |
| namespaceExists           | Y    | Y         | Y    | Y  |
| updateNamespaceProperties | Y    | Y         | Y    | Y  |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  |

### Sql Catalog

The sql catalog is a catalog backed by a sql database, which is called jdbc catalog in java.

| Database | Java | PyIceberg | Rust | Go |
|----------|------|-----------|------|----|
| Postgres | Y    | Y         | Y    | Y  |
| MySQL    | Y    | Y         | Y    | Y  |
| SQLite   | Y    | Y         | Y    | Y  |

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go |
|----------------|------|-----------|------|----|
| createView     | Y    | N         | N    | N  |
| dropView       | Y    | N         | N    | N  |
| listView       | Y    | N         | N    | N  |
| viewExists     | Y    | N         | N    | N  |
| replaceView    | Y    | N         | N    | N  |
| renameView     | Y    | N         | N    | N  |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go |
|---------------------------|------|-----------|------|----|
| listNamespaces            | Y    | Y         | N    | Y  |
| createNamespace           | Y    | Y         | N    | Y  |
| dropNamespace             | Y    | Y         | Y    | Y  |
| namespaceExists           | Y    | N         | N    | Y  |
| updateNamespaceProperties | Y    | Y         | Y    | Y  |
| loadNamespaceMetadata     | Y    | Y         | N    | Y  |

### Glue Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go |
|----------------|------|-----------|------|----|
| createView     | Y    | N         | N    | N  |
| dropView       | Y    | N         | N    | N  |
| listView       | Y    | N         | N    | N  |
| viewExists     | Y    | N         | N    | N  |
| replaceView    | Y    | N         | N    | N  |
| renameView     | Y    | N         | N    | N  |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go |
|---------------------------|------|-----------|------|----|
| listNamespaces            | Y    | Y         | N    | Y  |
| createNamespace           | Y    | Y         | N    | Y  |
| dropNamespace             | Y    | Y         | N    | Y  |
| namespaceExists           | Y    | N         | N    | Y  |
| updateNamespaceProperties | Y    | Y         | Y    | Y  |
| loadNamespaceMetadata     | Y    | Y         | N    | Y  |

### Hive Metastore Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go |
|-----------------|------|-----------|------|----|
| listTable       | Y    | Y         | Y    | Y  |
| createTable     | Y    | Y         | Y    | Y  |
| dropTable       | Y    | Y         | Y    | Y  |
| loadTable       | Y    | Y         | Y    | Y  |
| updateTable     | Y    | Y         | Y    | Y  |
| renameTable     | Y    | Y         | Y    | Y  |
| tableExists     | Y    | Y         | Y    | Y  |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go |
|----------------|------|-----------|------|----|
| createView     | Y    | N         | N    | N  |
| dropView       | Y    | N         | N    | N  |
| listView       | Y    | N         | N    | N  |
| viewExists     | Y    | N         | N    | N  |
| replaceView    | Y    | N         | N    | N  |
| renameView     | Y    | N         | N    | N  |

#### Namespace Operations

| Namespace Operation       | Java | PyIceberg | Rust | Go |
|---------------------------|------|-----------|------|----|
| listNamespaces            | Y    | Y         | N    | N  |
| createNamespace           | Y    | Y         | N    | N  |
| dropNamespace             | Y    | Y         | N    | N  |
| namespaceExists           | Y    | N         | N    | N  |
| updateNamespaceProperties | Y    | Y         | Y    | Y  |
| loadNamespaceMetadata     | Y    | Y         | N    | N  |
