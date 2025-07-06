<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Iceberg Flink Quickstart Docker Image

A pre-configured Apache Flink image with Apache Iceberg dependencies for quickly getting started with Iceberg on Flink.

See the [Flink quickstart documentation](https://iceberg.apache.org/flink-quickstart/) for details.

## Overview

This Docker image extends the official Apache Flink image to include:

- Iceberg Flink runtime
- Iceberg AWS bundle for S3/Glue support
- Minimal Hadoop dependencies necessary for Flink

## Build Arguments

The following build arguments can be customized when building the image:

| Argument | Default | Description |
|----------|---------|-------------|
| `FLINK_VERSION` | `2.0` | Apache Flink version |
| `ICEBERG_FLINK_RUNTIME_VERSION` | `2.0` | Iceberg Flink runtime version |
| `ICEBERG_VERSION` | `1.10.1` | Apache Iceberg version |
| `HADOOP_VERSION` | `3.4.2` | Apache Hadoop version |

## Building Locally

To build the image locally with default versions:

```bash
docker build -t apache/iceberg-flink-quickstart docker/iceberg-flink-quickstart/
```

To build with custom versions:

```bash
docker build \
  --build-arg FLINK_VERSION=2.0 \
  --build-arg ICEBERG_VERSION=1.10.1 \
  -t apache/iceberg-flink-quickstart \
  docker/iceberg-flink-quickstart/
```

## Usage

See the [Flink quickstart documentation](https://iceberg.apache.org/flink-quickstart/) for details.


## Test Script

A test script (`test.sql`) is provided to validate the Iceberg-Flink integration and future changes to the Docker image. 

Start up the Docker containers:

```sh
docker compose -f docker/iceberg-flink-quickstart/docker-compose.yml up -d --build
```

Execute the test script directly from the host:

```bash
docker exec -i jobmanager ./bin/sql-client.sh < docker/iceberg-flink-quickstart/test.sql
```

**Expected behavior:**
- Exit code: 0 (success)
- Creates: 1 catalog (`iceberg_catalog`), 1 database (`nyc`), 1 table (`taxis`)
- Inserts: 4 records
- Final state: Table `iceberg_catalog.nyc.taxis` contains 4 rows

To stop the stack:

```bash
docker compose -f docker/iceberg-flink-quickstart/docker-compose.yml down
```
