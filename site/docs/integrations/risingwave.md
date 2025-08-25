---
title: "RisingWave"
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

# RisingWave

[RisingWave](https://risingwave.com/) is a Postgres-compatible SQL database designed for real-time event streaming data processing, analysis, and management. It can ingest millions of events per second, continuously join and analyze live data streams with historical tables, serve ad-hoc queries in real-time, and deliver fresh, consistent results.

## Supported Features

RisingWave supports batch reads and streaming writes of Apache Iceberg™ tables via its built-in source and sink connectors. For more information, see the [Iceberg source connector documentation](https://docs.risingwave.com/integrations/sources/apache-iceberg) and [Iceberg sink connector documentation](https://docs.risingwave.com/integrations/destinations/apache-iceberg).

## Table Formats and Warehouse Locations

Currently, RisingWave only supports the Iceberg V2 table format and S3-compatible object storage as Iceberg warehouse locations.

## Catalogs

RisingWave supports the following catalogs:

- `rest`
- `jdbc` / `sql`
- `glue`
- `storage`
- `hive`

See [RisingWave’s Iceberg catalog documentation](https://docs.risingwave.com/integrations/destinations/apache-iceberg#catalog) for more details.

## Getting Started

### Writing Data to Iceberg Tables

To write data to an Iceberg table, create a sink in RisingWave. The following example writes data from an existing table or materialized view `rw_data` to an Iceberg table `t1`.

```sql
CREATE SINK sink_to_iceberg FROM t1 WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'id',
    database.name = 'demo_db',
    table.name = 't1',
    catalog.name = 'demo',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/demo',
    s3.endpoint = '<http://127.0.0.1:9301>',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin'
);
```

**Note**: From RisingWave 2.1, you can use the `create_table_if_not_exists` parameter to create a table if it doesn't exist.

### Reading from Iceberg Tables

To read data from an Iceberg table, create a source in RisingWave. The following example reads data from an Iceberg table `t1`.

```sql
CREATE SOURCE iceberg_t1_source WITH (
    connector = 'iceberg',
    s3.endpoint = '<http://127.0.0.1:9301>',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    catalog.type = 'storage',
    warehouse.path = 's3a://hummock001/demo',
    database.name = 'demo_db',
    table.name = 't1',
);
```
After this source is created, you can query the data using the following SQL statement: 

```sql
SELECT * FROM iceberg_t1_source;
```