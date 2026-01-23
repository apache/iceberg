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

This Docker image extends the official Apache Flink image and includes:

- Iceberg Flink runtime JAR
- Iceberg AWS bundle for S3/Glue support
- Hadoop dependencies for filesystem operations

## Build Arguments

The following build arguments can be customized when building the image:

| Argument | Default | Description |
|----------|---------|-------------|
| `FLINK_VERSION` | `2.0` | Apache Flink version |
| `ICEBERG_VERSION` | `1.10.1` | Apache Iceberg version |
| `ICEBERG_FLINK_RUNTIME_VERSION` | `2.0` | Iceberg Flink runtime version |
| `ICEBERG_AWS_BUNDLE_VERSION` | `1.9.2` | Iceberg AWS bundle version |
| `HADOOP_VERSION` | `3.3.4` | Apache Hadoop version |

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

### Using docker-compose

The easiest way to get started is using the quickstart docker-compose file:

```bash
cd flink/quickstart
docker compose up -d
```

Then connect to Flink SQL client:

```bash
docker exec -it jobmanager ./bin/sql-client.sh
```

### For Local Development

If you want to build from the local Dockerfile instead of using the published image:

```bash
cd flink/quickstart
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

### Running Directly

```bash
# Start JobManager
docker run -d --name jobmanager \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
  apache/iceberg-flink-quickstart:latest \
  jobmanager

# Start TaskManager
docker run -d --name taskmanager \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager" \
  --link jobmanager:jobmanager \
  apache/iceberg-flink-quickstart:latest \
  taskmanager
```

## Example: Creating an Iceberg Catalog

Once connected to the Flink SQL client:

```sql
-- Create an Iceberg catalog using REST
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
  'uri' = 'http://rest:8181'
);

-- Use the catalog
USE CATALOG iceberg;

-- Create a namespace
CREATE DATABASE IF NOT EXISTS quickstart;
USE quickstart;

-- Create a table
CREATE TABLE sample (
  id BIGINT,
  data STRING
);

-- Insert data
INSERT INTO sample VALUES (1, 'hello'), (2, 'world');

-- Query data
SELECT * FROM sample;
```
