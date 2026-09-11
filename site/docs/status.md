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
| timestamp_ns   | Y    | Y         | Y    | Y  | Y   |
| timestamptz_ns | Y    | Y         | Y    | Y  | Y   |
| unknown        | Y    | Y         | N    | Y  | Y   |
| string         | Y    | Y         | Y    | Y  | Y   |
| uuid           | Y    | Y         | Y    | Y  | Y   |
| fixed          | Y    | Y         | Y    | Y  | Y   |
| binary         | Y    | Y         | Y    | Y  | Y   |
| variant        | Y    | Y         | Y    | Y  | N   |
| geometry       | Y    | Y         | N    | Y  | Y   |
| geography      | Y    | Y         | N    | Y  | Y   |
| list           | Y    | Y         | Y    | Y  | Y   |
| map            | Y    | Y         | Y    | Y  | Y   |
| struct         | Y    | Y         | Y    | Y  | Y   |

## Table Spec V3

[Table Spec V3](spec.md#version-3-extended-types-and-capabilities) adds capabilities that are not present in V2. The
table below tracks those V3-specific capabilities separately because most table and catalog operations are independent
of the format version. `Y` indicates end-to-end support for the listed behavior, not only support for serializing its
metadata fields.

In all operation tables below, a range in the `Spec` column means that the status is the same for every listed version.

| Capability                       | Spec | Java | PyIceberg | Rust | Go | C++ |
|----------------------------------|------|------|-----------|------|----|-----|
| Read table metadata              | V3   | Y    | Y         | Y    | Y  | Y   |
| Write table metadata             | V3   | Y    | N         | Y    | Y  | Y   |
| Read initial column defaults     | V3   | Y    | Y         | Y    | Y  | Y   |
| Read multi-argument transforms   | V3   | N    | N         | N    | Y  | N   |
| Write multi-argument transforms  | V3   | N    | N         | N    | Y  | N   |
| Read row lineage columns         | V3   | Y    | N         | Y    | Y  | Y   |
| Write row lineage                | V3   | Y    | N         | Y    | Y  | Y   |
| Read encrypted tables            | V3   | Y    | N         | Y    | N  | N   |
| Write encrypted tables           | V3   | Y    | N         | Y    | N  | N   |

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
| Update schema               | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Update partition spec       | V1-V3 | Y    | Y         | N    | Y  | Y   |
| Update table properties     | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Replace sort order          | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Update table location       | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Update statistics           | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Update partition statistics | V1-V3 | Y    | N         | N    | Y  | Y   |
| Expire snapshots            | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Manage snapshots            | V1-V3 | Y    | Y         | N    | Y  | Y   |

## Table Update Operations

| Operation         | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-------------------|-------|------|-----------|------|----|-----|
| Append data files | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Append data files | V3    | Y    | N         | Y    | Y  | Y   |
| Rewrite files     | V1-V2 | Y    | Y         | N    | Y  | Y   |
| Rewrite files     | V3    | Y    | N         | N    | Y  | Y   |
| Rewrite manifests | V1-V2 | Y    | Y         | N    | Y  | N   |
| Rewrite manifests | V3    | Y    | N         | N    | Y  | N   |
| Overwrite files   | V1-V2 | Y    | Y         | N    | Y  | Y   |
| Overwrite files   | V3    | Y    | N         | N    | Y  | Y   |
| Delete files      | V1-V2 | Y    | Y         | N    | Y  | Y   |
| Delete files      | V3    | Y    | N         | N    | Y  | Y   |
| Row delta         | V2-V3 | Y    | N         | N    | Y  | Y   |

## Table Read Operations

| Operation                   | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------------------|-------|------|-----------|------|----|-----|
| Plan with data file         | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Plan with position deletes  | V2-V3 | Y    | Y         | Y    | Y  | Y   |
| Plan with equality deletes  | V2-V3 | Y    | Y         | Y    | Y  | Y   |
| Plan with deletion vectors  | V3    | Y    | Y         | Y    | Y  | Y   |
| Plan with puffin statistics | V1-V3 | Y    | N         | N    | N  | N   |
| Read data file              | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| Read with position deletes  | V2-V3 | Y    | Y         | Y    | Y  | Y   |
| Read with equality deletes  | V2-V3 | Y    | N         | Y    | Y  | Y   |
| Read with deletion vectors  | V3    | Y    | Y         | Y    | Y  | Y   |

## Table Write Operations

| Operation              | Spec  | Java | PyIceberg | Rust | Go | C++ |
|------------------------|-------|------|-----------|------|----|-----|
| Append data            | V1-V2 | Y    | Y         | Y    | Y  | Y   |
| Append data            | V3    | Y    | N         | Y    | Y  | Y   |
| Write position deletes | V2    | Y    | N         | N    | Y  | Y   |
| Write equality deletes | V2-V3 | Y    | N         | Y    | Y  | Y   |
| Write deletion vectors | V3    | Y    | N         | N    | Y  | Y   |

## Catalogs

### Rest Catalog

#### Table Operations

| Table Operation | Spec  | Java | PyIceberg | Rust | Go | C++ |
|-----------------|-------|------|-----------|------|----|-----|
| listTable       | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| createTable     | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| dropTable       | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| loadTable       | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| updateTable     | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| renameTable     | V1-V3 | Y    | Y         | Y    | Y  | Y   |
| tableExists     | V1-V3 | Y    | Y         | Y    | Y  | Y   |

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
| listTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| createTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1-V3 | Y    | Y         | Y    | Y  | N   |

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
| listTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| createTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1-V3 | Y    | Y         | Y    | Y  | N   |

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
| listTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| createTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| dropTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| loadTable       | V1-V3 | Y    | Y         | Y    | Y  | N   |
| updateTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| renameTable     | V1-V3 | Y    | Y         | Y    | Y  | N   |
| tableExists     | V1-V3 | Y    | Y         | Y    | Y  | N   |

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
