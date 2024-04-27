---
title: "Hive and Iceberg Quickstart"
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


## Hive and Iceberg Quickstart

This guide will get you up and running with an Iceberg and Hive environment, including sample code to
highlight some powerful features. You can learn more about Iceberg's Hive runtime by checking out the [Hive](site:docs/latest/hive/) section.

- [Docker Images](#docker-images)
- [Creating a Table](#creating-a-table)
- [Writing Data to a Table](#writing-data-to-a-table)
- [Reading Data from a Table](#reading-data-from-a-table)
- [Next Steps](#next-steps)

### Docker Images

The fastest way to get started is to use [Apache Hive images](https://hub.docker.com/r/apache/hive) 
which provides a SQL-like interface to create and query Iceberg tables from your laptop. You need to install the [Docker Desktop](https://www.docker.com/products/docker-desktop/).

Take a look at the Tags tab in [Apache Hive docker images](https://hub.docker.com/r/apache/hive/tags?page=1&ordering=-last_updated) to see the available Hive versions.

Set the version variable.
```sh
export HIVE_VERSION=4.0.0-beta-1
```

Start the container, using the option `--platform linux/amd64` for a Mac with an M-Series chip:
```sh
docker run -d --platform linux/amd64 -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:${HIVE_VERSION}
```

The docker run command above configures Hive to use the embedded derby database for Hive Metastore. Hive Metastore functions as the Iceberg catalog to locate Iceberg files, which can be anywhere. 

Give HiveServer (HS2) a little time to come up in the docker container, and then start the Hive Beeline client using the following command to connect with the HS2 containers you already started:
```sh
docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'
```

The hive prompt appears:
```sh
0: jdbc:hive2://localhost:10000>
```

You can now run SQL queries to create Iceberg tables and query the tables.
```sql
show databases;
```

### Creating a Table

To create your first Iceberg table in Hive, run a [`CREATE TABLE`](site:docs/latest/hive/#create-table) command. Let's create a table
using `nyc.taxis` where `nyc` is the database name and `taxis` is the table name.
```sql
CREATE DATABASE nyc;
```
```sql
CREATE TABLE nyc.taxis
(
  trip_id bigint,
  trip_distance float,
  fare_amount double,
  store_and_fwd_flag string
)
PARTITIONED BY (vendor_id bigint) STORED BY ICEBERG;
```
Iceberg catalogs support the full range of SQL DDL commands, including:

* [`CREATE TABLE`](site:docs/latest/hive/#create-table)
* [`CREATE TABLE AS SELECT`](site:docs/latest/hive/#create-table-as-select)
* [`CREATE TABLE LIKE TABLE`](site:docs/latest/hive/#create-table-like-table)
* [`ALTER TABLE`](site:docs/latest/hive/#alter-table)
* [`DROP TABLE`](site:docs/latest/hive/#drop-table)

### Writing Data to a Table

After your table is created, you can insert records.
```sql
INSERT INTO nyc.taxis
VALUES (1000371, 1.8, 15.32, 'N', 1), (1000372, 2.5, 22.15, 'N', 2), (1000373, 0.9, 9.01, 'N', 2), (1000374, 8.4, 42.13, 'Y', 1);
```

### Reading Data from a Table

To read a table, simply use the Iceberg table's name.
```sql
SELECT * FROM nyc.taxis;
```

### Next steps

#### Adding Iceberg to Hive

If you already have a Hive 4.0.0-alpha-1, or later, environment, it comes with the Iceberg 0.13.1 included. No additional downloads or jars are needed. If you have a Hive 2.3.x or Hive 3.1.x environment see [Enabling Iceberg support in Hive](site:docs/latest/hive/#enabling-iceberg-support-in-hive).

#### Learn More

To learn more about setting up a database other than Derby, see [Apache Hive Quick Start](https://hive.apache.org/developement/quickstart/). You can also [set up a standalone metastore, HS2 and Postgres](https://github.com/apache/hive/blob/master/packaging/src/docker/docker-compose.yml). Now that you're up and running with Iceberg and Hive, check out the [Iceberg-Hive docs](site:docs/latest/hive/) to learn more!
