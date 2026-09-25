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

Iceberg comes with a flexible abstraction around reading and writing data and metadata files. The FileIO interface allows the Iceberg library to communicate with the underlying storage layer. FileIO is used for all metadata operations during the job planning and commit stages.

## Iceberg Files

The metadata for an Iceberg table tracks the absolute path for data files which allows greater abstraction over the physical layout. Additionally, changes to table state are performed by writing new metadata files and never involve renaming files. This allows a much smaller set of requirements for file operations. The essential functionality for a FileIO implementation is that it can read files, write files, and seek to any position within a stream.

## Usage in Processing Engines

The responsibility of reading and writing data files lies with the processing engines and happens during task execution. However, after data files are written, processing engines use FileIO to write new Iceberg metadata files that capture the new state of the table.

Different FileIO implementations are used depending on the type of storage. Iceberg comes with a set of FileIO implementations for popular storage providers.

- Amazon S3
- Google Cloud Storage
- Object Service Storage (including https)
- Dell Enterprise Cloud Storage
- Hadoop (adapts any Hadoop FileSystem implementation)

## Google Cloud Storage FileIO

Iceberg writes to GCS through `GCSFileIO`. Catalogs can load it with the `io-impl` catalog property.

### Single-shot uploads

Objects smaller than `gcs.write.threshold-bytes` are uploaded with a single GCS insert (`Storage.create`).
Larger objects use a `WriteChannel`, which is the existing streaming write path.

The default is 8 MiB so typical Iceberg metadata, snapshot lists, manifests, and delete files use the
single-shot path. Data files are larger than the threshold and still stream after an 8 MiB in-memory
prefix. GCS recommends simple/direct upload under about 5 MiB; 8 MiB is a coverage tradeoff and should
not be raised without evidence. Set the property to `0` to always use `WriteChannel`.

| Property                    | Default | Description                                                                                          |
| --------------------------- | ------- | ---------------------------------------------------------------------------------------------------- |
| gcs.write.threshold-bytes   | 8MB     | Max size for a single-shot upload. `0` disables single-shot upload and always streams via WriteChannel. |
