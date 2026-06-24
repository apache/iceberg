---
title: "FileIO"
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

# Iceberg FileIO

## Overview

Iceberg comes with a flexible abstraction around reading and writing data and metadata files. The FileIO interface allows the Iceberg library to communicate with the underlying storage layer, and is used for all metadata operations during job planning and commit. FileIO instances are passed across JVM boundaries by engines like Spark, so every implementation is required to be serializable.

## Iceberg Files

The metadata for an Iceberg table tracks the absolute path for data files which allows greater abstraction over the physical layout. Additionally, changes to table state are performed by writing new metadata files and never involve renaming files. This allows a much smaller set of requirements for file operations. The essential functionality for a FileIO implementation is that it can read files, write files, and seek to any position within a stream.

## Built-in implementations

Iceberg ships with FileIO implementations for the storage systems most data platforms use. Each entry below names the implementation class, the storage system it talks to, and the operations it can perform efficiently.

| Implementation | Storage | What it can do efficiently |
| --- | --- | --- |
| `HadoopFileIO` | Any Hadoop-compatible file system, including HDFS and the local file system | Single-file read, write, and delete; bulk delete; prefix listing |
| `S3FileIO` | Amazon S3, via `s3://`, `s3a://`, `s3n://` | Single-file read, write, and delete; bulk delete; prefix listing; best-effort file recovery |
| `GCSFileIO` | Google Cloud Storage, via `gs://` | Single-file read, write, and delete; bulk delete; prefix listing |
| `ADLSFileIO` | Azure Data Lake Storage Gen2, via `abfs://`, `abfss://`, `wasb://`, `wasbs://` | Single-file read, write, and delete; bulk delete; prefix listing |
| `OSSFileIO` | Aliyun Object Storage Service | Single-file read, write, and delete |
| `EcsFileIO` | Dell EMC Enterprise Cloud Storage | Single-file read, write, and delete |

`OSSFileIO` and `EcsFileIO` do not currently provide bulk delete or prefix listing. Table-maintenance operations that scan or remove many files at once, such as orphan-file cleanup and table purge, are therefore slower against those backends than against the other implementations.

## Routing by URI scheme

By default, the REST catalog uses `ResolvingFileIO`, which picks an implementation from the URI scheme of each file location:

| URI scheme | Implementation |
| --- | --- |
| `s3`, `s3a`, `s3n` | `S3FileIO` |
| `gs` | `GCSFileIO` |
| `abfs`, `abfss`, `wasb`, `wasbs` | `ADLSFileIO` |
| any other scheme | `HadoopFileIO` |

Schemes that are not in this table, such as `oss://`, `ecs://`, or a bare `https://`, are not routed to `OSSFileIO` or `EcsFileIO`; they fall through to `HadoopFileIO`. To use `OSSFileIO` or `EcsFileIO`, set the catalog `io-impl` property to the implementation's fully qualified class name.

## Choosing the FileIO

Most users do not need to set anything. The REST catalog defaults to `ResolvingFileIO`, so writing to an `s3://` path automatically uses `S3FileIO`. The Hadoop and JDBC catalogs default to `HadoopFileIO`.

To override the default, set the catalog property `io-impl` to the fully qualified class name of the FileIO implementation you want to use. Provider-specific tuning lives on the pages that own each backend:

- [Catalog properties](catalog-properties.md) for the `io-impl` property and other catalog-wide knobs
- [AWS](aws.md) for `S3FileIO` configuration and behavior
- [Dell](dell.md) for the Dell EMC ECS catalog and FileIO

## Encryption

Iceberg supports table encryption by wrapping any FileIO with `EncryptingFileIO`, which preserves the capabilities of the wrapped FileIO so bulk delete and prefix listing continue to work when those are available on the underlying implementation. See the [Encryption](encryption.md) document for the full picture.

## Implementing a custom FileIO

The FileIO surface is intentionally small: read a file, write a file, delete a file. The interfaces live under the `org.apache.iceberg.io` package in the `iceberg-api` module. Once implemented, a custom FileIO is plugged in by setting the catalog `io-impl` property to its fully qualified class name. For a walkthrough, including the optional capability interfaces that enable bulk delete, prefix listing, best-effort recovery, and storage credentials, see the [Custom file IO implementation](custom-catalog.md#custom-file-io-implementation) section of the Java Custom Catalog guide.

## Usage in Processing Engines

The responsibility for reading and writing data files lies with the processing engine and happens during task execution. Everything related to table metadata, including manifests, manifest lists, table metadata files, and the commit itself, goes through FileIO. Because engines distribute the FileIO instance to executors, every implementation is required to be serializable.
