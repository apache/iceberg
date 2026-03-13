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
- [C++](https://github.com/apache/iceberg-cpp/releases)

## Data Types

| Data Type      | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| boolean        | Y    | Y         | Y    | Y  | Y   |
| int            | Y    | Y         | Y    | Y  | Y   |
| long           | Y    | Y         | Y    | Y  | Y   |
| float          | Y    | Y         | Y    | Y  | Y   |
| double         | Y    | Y         | Y    | Y  | Y   |
| decimal        | Y    | Y         | Y    | Y  | Y   |
| date           | Y    | Y         | Y    | Y  | Y   |
| time           | Y    | Y         | Y    | Y  | Y   |
| timestamp      | Y    | Y         | Y    | Y  | Y   |
| timestamptz    | Y    | Y         | Y    | Y  | Y   |
| timestamp_ns   | Y    | Y         | Y    | Y  | N   |
| timestamptz_ns | Y    | Y         | Y    | Y  | N   |
| string         | Y    | Y         | Y    | Y  | Y   |
| uuid           | Y    | Y         | Y    | Y  | N   |
| fixed          | Y    | Y         | Y    | Y  | Y   |
| binary         | Y    | Y         | Y    | Y  | Y   |
| variant        | Y    | Y         | Y    | Y  | N   |
| list           | Y    | Y         | Y    | Y  | Y   |
| map            | Y    | Y         | Y    | Y  | Y   |
| struct         | Y    | Y         | Y    | Y  | Y   |

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

### Table Spec V1

| Operation                   | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|------|-----------|------|----|-----|
| Update schema               | Y    | Y         | Y    | N  | Y   |
| Update partition spec       | Y    | Y         | Y    | N  | Y   |
| Update table properties     | Y    | Y         | Y    | Y  | Y   |
| Replace sort order          | Y    | N         | N    | N  | Y   |
| Update table location       | Y    | Y         | N    | N  | Y   |
| Update statistics           | Y    | Y         | N    | N  | Y   |
| Update partition statistics | Y    | N         | N    | N  | N   |
| Expire snapshots            | Y    | N         | N    | N  | N   |
| Manage snapshots            | Y    | N         | N    | N  | N   |

### Table Spec V2

| Operation                   | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|------|-----------|------|----|-----|
| Update schema               | Y    | Y         | N    | N  | Y   |
| Update partition spec       | Y    | Y         | N    | N  | Y   |
| Update table properties     | Y    | Y         | Y    | Y  | Y   |
| Replace sort order          | Y    | N         | N    | N  | Y   |
| Update table location       | Y    | Y         | N    | N  | Y   |
| Update statistics           | Y    | Y         | N    | N  | Y   |
| Update partition statistics | Y    | N         | N    | N  | N   |
| Expire snapshots            | Y    | N         | N    | N  | N   |
| Manage snapshots            | Y    | N         | N    | N  | N   |

## Table Update Operations

### Table Spec V1

| Operation         | Java | PyIceberg | Rust | Go | C++ |
|-------------------|------|-----------|------|----|-----|
| Append data files | Y    | Y         | N    | Y  | Y   |
| Rewrite files     | Y    | Y         | N    | N  | N   |
| Rewrite manifests | Y    | Y         | N    | Y  | N   |
| Overwrite files   | Y    | Y         | N    | N  | N   |
| Delete files      | Y    | Y         | N    | N  | N   |

### Table Spec V2

| Operation         | Java | PyIceberg | Rust | Go | C++ |
|-------------------|------|-----------|------|----|-----|
| Append data files | Y    | Y         | N    | Y  | Y   |
| Rewrite files     | Y    | Y         | N    | N  | N   |
| Rewrite manifests | Y    | Y         | N    | Y  | N   |
| Overwrite files   | Y    | Y         | N    | N  | N   |
| Row delta         | Y    | N         | N    | N  | N   |
| Delete files      | Y    | Y         | N    | N  | N   |

## Table Read Operations

### Table Spec V1

| Operation                   | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|------|-----------|------|----|-----|
| Plan with data file         | Y    | Y         | Y    | Y  | Y   |
| Plan with puffin statistics | Y    | Y         | Y    | Y  | N   |
| Read data file              | Y    | N         | Y    | Y  | Y   |

### Table Spec V2

| Operation                   | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|------|-----------|------|----|-----|
| Plan with data file         | Y    | Y         | Y    | Y  | Y   |
| Plan with position deletes  | Y    | Y         | N    | Y  | Y   |
| Plan with equality deletes  | Y    | Y         | N    | N  | Y   |
| Plan with puffin statistics | Y    | N         | N    | N  | N   |
| Read data file              | Y    | Y         | Y    | Y  | Y   |
| Read with position deletes  | Y    | Y         | N    | Y  | N   |
| Read with equality deletes  | Y    | N         | N    | N  | N   |

## Table Write Operations

### Table Spec V1

| Operation   | Java | PyIceberg | Rust | Go | C++ |
|-------------|------|-----------|------|----|-----|
| Append data | Y    | Y         | Y    | Y  | N   |

### Table Spec V2

| Operation              | Java | PyIceberg | Rust | Go | C++ |
|------------------------|------|-----------|------|----|-----|
| Append data            | Y    | Y         | Y    | Y  | N   |
| Write position deletes | Y    | N         | N    | N  | N   |
| Write equality deletes | Y    | N         | N    | N  | N   |

## Catalogs

### Rest Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | Y   |
| createTable     | Y    | Y         | Y    | Y  | Y   |
| dropTable       | Y    | Y         | Y    | Y  | Y   |
| loadTable       | Y    | Y         | Y    | Y  | Y   |
| updateTable     | Y    | Y         | Y    | Y  | Y   |
| renameTable     | Y    | Y         | Y    | Y  | Y   |
| tableExists     | Y    | Y         | Y    | Y  | Y   |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | Y   |
| createTable     | Y    | Y         | Y    | Y  | Y   |
| dropTable       | Y    | Y         | Y    | Y  | Y   |
| loadTable       | Y    | Y         | Y    | Y  | Y   |
| updateTable     | Y    | Y         | Y    | Y  | Y   |
| renameTable     | Y    | Y         | Y    | Y  | Y   |
| tableExists     | Y    | Y         | Y    | Y  | Y   |

#### View Spec V1

| View Operation | Java | PyIceberg | Rust | Go | C++ |
|----------------|------|-----------|------|----|-----|
| createView     | Y    | N         | N    | N  | N   |
| dropView       | Y    | Y         | N    | N  | N   |
| listView       | Y    | Y         | N    | N  | N   |
| viewExists     | Y    | Y         | N    | N  | N   |
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

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

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
| listNamespaces            | Y    | Y         | N    | Y  | N   |
| createNamespace           | Y    | Y         | N    | Y  | N   |
| dropNamespace             | Y    | Y         | Y    | Y  | N   |
| namespaceExists           | Y    | N         | N    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | N    | Y  | N   |

### Glue Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

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
| listNamespaces            | Y    | Y         | N    | Y  | N   |
| createNamespace           | Y    | Y         | N    | Y  | N   |
| dropNamespace             | Y    | Y         | N    | Y  | N   |
| namespaceExists           | Y    | N         | N    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | N    | Y  | N   |

### Hive Metastore Catalog

#### Table Spec V1

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

#### Table Spec V2

| Table Operation | Java | PyIceberg | Rust | Go | C++ |
|-----------------|------|-----------|------|----|-----|
| listTable       | Y    | Y         | Y    | Y  | N   |
| createTable     | Y    | Y         | Y    | Y  | N   |
| dropTable       | Y    | Y         | Y    | Y  | N   |
| loadTable       | Y    | Y         | Y    | Y  | N   |
| updateTable     | Y    | Y         | Y    | Y  | N   |
| renameTable     | Y    | Y         | Y    | Y  | N   |
| tableExists     | Y    | Y         | Y    | Y  | N   |

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
| listNamespaces            | Y    | Y         | N    | N  | N   |
| createNamespace           | Y    | Y         | N    | N  | N   |
| dropNamespace             | Y    | Y         | N    | N  | N   |
| namespaceExists           | Y    | N         | N    | N  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | N    | N  | N   |
