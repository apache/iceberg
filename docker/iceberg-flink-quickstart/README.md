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
| `ICEBERG_AWS_BUNDLE_VERSION` | `1.10.1` | Iceberg AWS bundle version |
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

The easiest way to get started is using the quickstart docker-compose file from the repository root:

```bash
docker compose -f docker/iceberg-flink-quickstart/docker-compose.yml up -d --build
```

Then connect to Flink SQL client:

```bash
docker exec -it jobmanager ./bin/sql-client.sh
```

To stop the stack:

```bash
docker compose -f docker/iceberg-flink-quickstart/docker-compose.yml down
```
