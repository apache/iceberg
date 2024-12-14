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

# Implementations Status

Apache iceberg now has implementations of the iceberg spec in multiple languages. This page provides a summary of the
current status of these implementations.

## Versions

This section describes the versions of each implementation that are being tracked in this page.

| Language  | Version |
|-----------|---------|
| Java      | 1.7.1   |
| PyIceberg | 0.7.0   |
| Rust      | 0.3.0   |
| Go        | 0.1.0   |

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

## Data File Formats

| Format  | Java | PyIceberg | Rust | Go |
|---------|------|-----------|------|----|
| Parquet | Y    | Y         | Y    | Y  |
| ORC     | Y    | N         | N    | N  |
| Puffin  | Y    | N         | N    | N  |

## File IO

| Storage              | Java | PyIceberg | Rust | Go |
|----------------------|------|-----------|------|----|
| Local Filesystem     | Y    | Y         | Y    | Y  |
| Hadoop Filesystem    | Y    | Y         | Y    | Y  |
| Aws S3               | Y    | Y         | Y    | Y  |
| Google Cloud Storage | Y    | Y         | Y    | Y  |
| Memory Fs            | Y    | Y         | Y    | Y  |

## Table Update Operations

### Table Spec V1

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Update schema               | Y    | N         | Y    | N  |
| Update partition spec       | Y    | N         | Y    | N  |
| Update table properties     | Y    | Y         | Y    | N  |
| Replace sort order          | Y    | N         | N    | N  |
| Update table location       | Y    | N         | N    | N  |
| Append data files           | Y    | Y         | N    | N  |
| Rewrite files               | Y    | Y         | N    | N  |
| Rewrite manifests           | Y    | Y         | N    | N  |
| Overwrite files             | Y    | Y         | N    | N  |
| Row delta                   | Y    | N         | N    | N  |
| Delete files                | Y    | N         | N    | N  |
| Update statistics           | Y    | N         | N    | N  |
| Update partition statistics | Y    | N         | N    | N  |
| Expire snapshots            | Y    | N         | N    | N  |
| Manage snapshots            | Y    | N         | N    | N  |

### Table Spec V2

| Operation                   | Java | PyIceberg | Rust | Go |
|-----------------------------|------|-----------|------|----|
| Update schema               | Y    | Y         | N    | N  |
| Update partition spec       | Y    | Y         | N    | N  |
| Update table properties     | Y    | Y         | Y    | N  |
| Replace sort order          | Y    | N         | N    | N  |
| Update table location       | Y    | N         | N    | N  |
| Append data files           | Y    | Y         | N    | N  |
| Rewrite files               | Y    | Y         | N    | N  |
| Rewrite manifests           | Y    | Y         | N    | N  |
| Overwrite files             | Y    | Y         | N    | N  |
| Row delta                   | Y    | N         | N    | N  |
| Delete files                | Y    | Y         | N    | N  |
| Update statistics           | Y    | N         | N    | N  |
| Update partition statistics | Y    | N         | N    | N  |
| Expire snapshots            | Y    | N         | N    | N  |
| Manage snapshots            | Y    | N         | N    | N  |

## Table Read Operations

### Table Spec V1

| Operation           | Java | PyIceberg | Rust | Go |
|---------------------|------|-----------|------|----|
| Plan with data file | Y    | Y         | Y    | Y  |
| Read data file      | Y    | N         | Y    | N  |

### Table Spec V2

| Operation                  | Java | PyIceberg | Rust | Go |
|----------------------------|------|-----------|------|----|
| Plan with data file        | Y    | Y         | Y    | Y  |
| Plan with position deletes | Y    | N         | N    | N  |
| Plan with equality deletes | Y    | N         | N    | N  |
| Read data file             | Y    | Y         | Y    | N  |
| Read with position deletes | Y    | Y         | N    | N  |
| Read with equality deletes | Y    | N         | N    | N  |

## Table Write Operations

### Table Spec V1

| Operation   | Java | PyIceberg | Rust | Go |
|-------------|------|-----------|------|----|
| Append data | Y    | N         | Y    | N  |

### Table Spec V2

| Operation              | Java | PyIceberg | Rust | Go |
|------------------------|------|-----------|------|----|
| Append data            | Y    | N         | Y    | N  |
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
| dropView       | Y    | N         | N    | N  |
| listView       | Y    | N         | N    | N  |
| viewExists     | Y    | N         | N    | N  |
| replaceView    | Y    | N         | N    | N  |
| renameView     | Y    | N         | N    | N  |

#### Namespace Operations

| Namespace Operation | Java | PyIceberg | Rust | Go |
|---------------------|------|-----------|------|----|
| listNamespaces      | Y    | N         | N    | N  |
| createNamespace     | Y    | N         | N    | N  |
| dropNamespace       | Y    | N         | N    | N  |
| namespaceExists     | Y    | N         | N    | N  |
| renameNamespace     | Y    | N         | N    | N  |
| updateNamespace     | Y    | N         | N    | N  |
| deleteNamespace     | Y    | N         | N    | N  |
| loadNamespace       | Y    | N         | N    | N  |

### Sql Catalog

The sql catalog is a catalog backed by a sql database, which is called jdbc catalog in java.

| Database | Java | PyIceberg | Rust | Go |
|----------|------|-----------|------|----|
| Postgres | Y    | N         | Y    | N  |
| MySQL    | Y    | N         | Y    | N  |
| Sqlite   | Y    | N         | Y    | N  |

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

| Namespace Operation | Java | PyIceberg | Rust | Go |
|---------------------|------|-----------|------|----|
| listNamespaces      | Y    | N         | N    | N  |
| createNamespace     | Y    | N         | N    | N  |
| dropNamespace       | Y    | N         | N    | N  |
| namespaceExists     | Y    | N         | N    | N  |
| renameNamespace     | Y    | N         | N    | N  |
| updateNamespace     | Y    | N         | N    | N  |
| deleteNamespace     | Y    | N         | N    | N  |
| loadNamespace       | Y    | N         | N    | N  |

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

| Namespace Operation | Java | PyIceberg | Rust | Go |
|---------------------|------|-----------|------|----|
| listNamespaces      | Y    | N         | N    | N  |
| createNamespace     | Y    | N         | N    | N  |
| dropNamespace       | Y    | N         | N    | N  |
| namespaceExists     | Y    | N         | N    | N  |
| renameNamespace     | Y    | N         | N    | N  |
| updateNamespace     | Y    | N         | N    | N  |
| deleteNamespace     | Y    | N         | N    | N  |
| loadNamespace       | Y    | N         | N    | N  |

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

| Namespace Operation | Java | PyIceberg | Rust | Go |
|---------------------|------|-----------|------|----|
| listNamespaces      | Y    | N         | N    | N  |
| createNamespace     | Y    | N         | N    | N  |
| dropNamespace       | Y    | N         | N    | N  |
| namespaceExists     | Y    | N         | N    | N  |
| renameNamespace     | Y    | N         | N    | N  |
| updateNamespace     | Y    | N         | N    | N  |
| deleteNamespace     | Y    | N         | N    | N  |
| loadNamespace       | Y    | N         | N    | N  |