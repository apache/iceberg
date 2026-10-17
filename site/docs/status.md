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

`Y` means the library supports the type in schemas and can both read and write its values in at least one supported data file
format. `N` means one or more of these capabilities is missing. For example, recognizing `geometry` in a schema without being
able to read and write geometry values is `N`. Table-format write support is listed under Table Write Operations.

### Table Spec

=== "V1 - V2"
    | Data Type   | Java | PyIceberg | Rust | Go | C++ |
    |-------------|------|-----------|------|----|-----|
    | boolean     | Y    | Y         | Y    | Y  | Y   |
    | int         | Y    | Y         | Y    | Y  | Y   |
    | long        | Y    | Y         | Y    | Y  | Y   |
    | float       | Y    | Y         | Y    | Y  | Y   |
    | double      | Y    | Y         | Y    | Y  | Y   |
    | decimal     | Y    | Y         | Y    | Y  | Y   |
    | date        | Y    | Y         | Y    | Y  | Y   |
    | time        | Y    | Y         | Y    | Y  | Y   |
    | timestamp   | Y    | Y         | Y    | Y  | Y   |
    | timestamptz | Y    | Y         | Y    | Y  | Y   |
    | string      | Y    | Y         | Y    | Y  | Y   |
    | uuid        | Y    | Y         | Y    | Y  | Y   |
    | fixed       | Y    | Y         | Y    | Y  | Y   |
    | binary      | Y    | Y         | Y    | Y  | Y   |
    | list        | Y    | Y         | Y    | Y  | Y   |
    | map         | Y    | Y         | Y    | Y  | Y   |
    | struct      | Y    | Y         | Y    | Y  | Y   |

=== "V3"
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
    | timestamp_ns   | Y    | Y         | Y    | Y  | Y   |
    | timestamptz_ns | Y    | Y         | Y    | Y  | Y   |
    | unknown        | Y    | Y         | N    | Y  | Y   |
    | string         | Y    | Y         | Y    | Y  | Y   |
    | uuid           | Y    | Y         | Y    | Y  | Y   |
    | fixed          | Y    | Y         | Y    | Y  | Y   |
    | binary         | Y    | Y         | Y    | Y  | Y   |
    | variant        | Y    | N         | N    | Y  | N   |
    | geometry       | N    | N         | N    | N  | N   |
    | geography      | N    | N         | N    | N  | N   |
    | list           | Y    | Y         | Y    | Y  | Y   |
    | map            | Y    | Y         | Y    | Y  | Y   |
    | struct         | Y    | Y         | Y    | Y  | Y   |

## Table Metadata and Features

### Table Spec

=== "V1 - V2"
    | Capability           | Java | PyIceberg | Rust | Go | C++ |
    |----------------------|------|-----------|------|----|-----|
    | Read table metadata  | Y    | Y         | Y    | Y  | Y   |
    | Write table metadata | Y    | Y         | Y    | Y  | Y   |

=== "V3"
    | Capability                      | Java | PyIceberg | Rust | Go | C++ |
    |---------------------------------|------|-----------|------|----|-----|
    | Read table metadata             | Y    | Y         | Y    | Y  | Y   |
    | Write table metadata            | Y    | N         | Y    | Y  | Y   |
    | Read initial column defaults    | Y    | Y         | Y    | Y  | N   |
    | Read multi-argument transforms  | N    | N         | N    | N  | N   |
    | Write multi-argument transforms | N    | N         | N    | N  | N   |
    | Read row lineage columns        | Y    | N         | N    | Y  | N   |
    | Write row lineage               | Y    | N         | N    | Y  | Y   |
    | Read encrypted tables           | Y    | N         | N    | N  | N   |
    | Write encrypted tables          | Y    | N         | N    | N  | N   |

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

### Table Spec

=== "V1 - V2"
    | Operation                   | Java | PyIceberg | Rust | Go | C++ |
    |-----------------------------|------|-----------|------|----|-----|
    | Update schema               | Y    | Y         | Y    | Y  | Y   |
    | Update partition spec       | Y    | Y         | N    | Y  | Y   |
    | Update table properties     | Y    | Y         | Y    | Y  | Y   |
    | Replace sort order          | Y    | Y         | Y    | Y  | Y   |
    | Update table location       | Y    | N         | Y    | Y  | Y   |
    | Update statistics           | Y    | Y         | Y    | Y  | Y   |
    | Update partition statistics | Y    | N         | N    | Y  | Y   |
    | Expire snapshots            | Y    | Y         | Y    | Y  | Y   |
    | Manage snapshots            | Y    | Y         | N    | Y  | Y   |

=== "V3"
    | Operation                   | Java | PyIceberg | Rust | Go | C++ |
    |-----------------------------|------|-----------|------|----|-----|
    | Update schema               | Y    | Y         | Y    | Y  | Y   |
    | Update partition spec       | Y    | Y         | N    | Y  | Y   |
    | Update table properties     | Y    | Y         | Y    | Y  | Y   |
    | Replace sort order          | Y    | Y         | Y    | Y  | Y   |
    | Update table location       | Y    | N         | Y    | Y  | Y   |
    | Update statistics           | Y    | Y         | Y    | Y  | Y   |
    | Update partition statistics | Y    | N         | N    | Y  | Y   |
    | Expire snapshots            | Y    | Y         | Y    | Y  | Y   |
    | Manage snapshots            | Y    | Y         | N    | Y  | Y   |

## Table Update Operations

### Table Spec

=== "V1"
    | Operation         | Java | PyIceberg | Rust | Go | C++ |
    |-------------------|------|-----------|------|----|-----|
    | Append data files | Y    | Y         | Y    | Y  | Y   |
    | Rewrite files     | Y    | Y         | N    | Y  | N   |
    | Rewrite manifests | Y    | Y         | N    | N  | N   |
    | Overwrite files   | Y    | Y         | N    | Y  | N   |
    | Delete files      | Y    | Y         | N    | Y  | N   |

=== "V2"
    | Operation         | Java | PyIceberg | Rust | Go | C++ |
    |-------------------|------|-----------|------|----|-----|
    | Append data files | Y    | Y         | Y    | Y  | Y   |
    | Rewrite files     | Y    | Y         | N    | Y  | N   |
    | Rewrite manifests | Y    | Y         | N    | N  | N   |
    | Overwrite files   | Y    | Y         | N    | Y  | N   |
    | Delete files      | Y    | Y         | N    | Y  | N   |
    | Row delta         | Y    | N         | N    | Y  | N   |

=== "V3"
    | Operation         | Java | PyIceberg | Rust | Go | C++ |
    |-------------------|------|-----------|------|----|-----|
    | Append data files | Y    | N         | Y    | Y  | Y   |
    | Rewrite files     | Y    | N         | N    | Y  | N   |
    | Rewrite manifests | Y    | N         | N    | N  | N   |
    | Overwrite files   | Y    | N         | N    | Y  | N   |
    | Delete files      | Y    | N         | N    | Y  | N   |
    | Row delta         | Y    | N         | N    | Y  | N   |

## Table Read Operations

### Table Spec

=== "V1"
    | Operation                   | Java | PyIceberg | Rust | Go | C++ |
    |-----------------------------|------|-----------|------|----|-----|
    | Plan with data file         | Y    | Y         | Y    | Y  | Y   |
    | Plan with puffin statistics | Y    | N         | N    | N  | N   |
    | Read data file              | Y    | Y         | Y    | Y  | Y   |

=== "V2"
    | Operation                   | Java | PyIceberg | Rust | Go | C++ |
    |-----------------------------|------|-----------|------|----|-----|
    | Plan with data file         | Y    | Y         | Y    | Y  | Y   |
    | Plan with position deletes  | Y    | Y         | Y    | Y  | Y   |
    | Plan with equality deletes  | Y    | N         | Y    | Y  | Y   |
    | Plan with puffin statistics | Y    | N         | N    | N  | N   |
    | Read data file              | Y    | Y         | Y    | Y  | Y   |
    | Read with position deletes  | Y    | Y         | Y    | Y  | Y   |
    | Read with equality deletes  | Y    | N         | Y    | Y  | Y   |

=== "V3"
    | Operation                   | Java | PyIceberg | Rust | Go | C++ |
    |-----------------------------|------|-----------|------|----|-----|
    | Plan with data file         | Y    | Y         | Y    | Y  | Y   |
    | Plan with position deletes  | Y    | Y         | Y    | Y  | Y   |
    | Plan with equality deletes  | Y    | N         | Y    | Y  | Y   |
    | Plan with deletion vectors  | Y    | Y         | N    | Y  | Y   |
    | Plan with puffin statistics | Y    | N         | N    | N  | N   |
    | Read data file              | Y    | Y         | Y    | Y  | Y   |
    | Read with position deletes  | Y    | Y         | Y    | Y  | Y   |
    | Read with equality deletes  | Y    | N         | Y    | Y  | Y   |
    | Read with deletion vectors  | Y    | Y         | N    | N  | N   |

## Table Write Operations

### Table Spec

=== "V1"
    | Operation   | Java | PyIceberg | Rust | Go | C++ |
    |-------------|------|-----------|------|----|-----|
    | Append data | Y    | Y         | Y    | Y  | Y   |

=== "V2"
    | Operation              | Java | PyIceberg | Rust | Go | C++ |
    |------------------------|------|-----------|------|----|-----|
    | Append data            | Y    | Y         | Y    | Y  | Y   |
    | Write position deletes | Y    | N         | N    | Y  | Y   |
    | Write equality deletes | Y    | N         | Y    | Y  | Y   |

=== "V3"
    V3 writers must use deletion vectors instead of adding position delete files. Existing position deletes remain readable.

    | Operation              | Java | PyIceberg | Rust | Go | C++ |
    |------------------------|------|-----------|------|----|-----|
    | Append data            | Y    | N         | Y    | Y  | Y   |
    | Write equality deletes | Y    | N         | Y    | Y  | Y   |
    | Write deletion vectors | Y    | N         | N    | Y  | N   |

## Catalogs

### Rest Catalog

#### Table Spec

=== "V1 - V3"
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
| Postgres | Y    | Y         | Y    | Y  | Y   |
| MySQL    | Y    | Y         | Y    | Y  | Y   |
| SQLite   | Y    | Y         | Y    | Y  | Y   |

#### Table Spec

=== "V1 - V2"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | Y   |
    | createTable     | Y    | Y         | Y    | Y  | Y   |
    | dropTable       | Y    | Y         | Y    | Y  | Y   |
    | loadTable       | Y    | Y         | Y    | Y  | Y   |
    | updateTable     | Y    | Y         | Y    | Y  | Y   |
    | renameTable     | Y    | Y         | Y    | Y  | Y   |
    | tableExists     | Y    | Y         | Y    | Y  | Y   |

=== "V3"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | Y   |
    | createTable     | Y    | N         | Y    | Y  | Y   |
    | dropTable       | Y    | Y         | Y    | Y  | Y   |
    | loadTable       | Y    | Y         | Y    | Y  | Y   |
    | updateTable     | Y    | N         | Y    | Y  | Y   |
    | renameTable     | Y    | Y         | Y    | Y  | Y   |
    | tableExists     | Y    | Y         | Y    | Y  | Y   |

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
| listNamespaces            | Y    | Y         | Y    | Y  | Y   |
| createNamespace           | Y    | Y         | Y    | Y  | Y   |
| dropNamespace             | Y    | Y         | Y    | Y  | Y   |
| namespaceExists           | Y    | N         | Y    | Y  | Y   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | Y   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | Y   |

### Glue Catalog

#### Table Spec

=== "V1 - V2"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | N   |
    | createTable     | Y    | Y         | Y    | Y  | N   |
    | dropTable       | Y    | Y         | Y    | Y  | N   |
    | loadTable       | Y    | Y         | Y    | Y  | N   |
    | updateTable     | Y    | Y         | Y    | Y  | N   |
    | renameTable     | Y    | Y         | Y    | Y  | N   |
    | tableExists     | Y    | Y         | Y    | Y  | N   |

=== "V3"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | N   |
    | createTable     | Y    | N         | Y    | Y  | N   |
    | dropTable       | Y    | Y         | Y    | Y  | N   |
    | loadTable       | Y    | Y         | Y    | Y  | N   |
    | updateTable     | Y    | N         | Y    | Y  | N   |
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
| listNamespaces            | Y    | Y         | Y    | Y  | N   |
| createNamespace           | Y    | Y         | Y    | Y  | N   |
| dropNamespace             | Y    | Y         | Y    | Y  | N   |
| namespaceExists           | Y    | N         | Y    | Y  | N   |
| updateNamespaceProperties | Y    | Y         | Y    | Y  | N   |
| loadNamespaceMetadata     | Y    | Y         | Y    | Y  | N   |

### Hive Metastore Catalog

#### Table Spec

=== "V1 - V2"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | N   |
    | createTable     | Y    | Y         | Y    | Y  | N   |
    | dropTable       | Y    | Y         | Y    | Y  | N   |
    | loadTable       | Y    | Y         | Y    | Y  | N   |
    | updateTable     | Y    | Y         | N    | Y  | N   |
    | renameTable     | Y    | Y         | Y    | Y  | N   |
    | tableExists     | Y    | Y         | Y    | Y  | N   |

=== "V3"
    | Table Operation | Java | PyIceberg | Rust | Go | C++ |
    |-----------------|------|-----------|------|----|-----|
    | listTable       | Y    | Y         | Y    | Y  | N   |
    | createTable     | Y    | N         | Y    | Y  | N   |
    | dropTable       | Y    | Y         | Y    | Y  | N   |
    | loadTable       | Y    | Y         | Y    | Y  | N   |
    | updateTable     | Y    | N         | N    | Y  | N   |
    | renameTable     | Y    | Y         | Y    | Y  | N   |
    | tableExists     | Y    | Y         | Y    | Y  | N   |

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
