---
title: "Hive Migration"
url: hive-migration
menu:
  main:
    parent: "Migration"
    identifier: hive_migration
    weight: 200
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

# Hive Table Migration
Apache Hive supports ORC, Parquet, and Avro file formats that could be migrated to Iceberg.
When migrating data to an Iceberg table, which provides versioning and transactional updates, only the most recent data files need to be migrated.

Iceberg supports all three migration actions: Snapshot Table, Migrate Table, and Add Files for migrating from Hive tables to Iceberg tables. Since Hive tables do not maintain snapshots,
the migration process essentially involves creating a new Iceberg table with the existing schema and committing all data files across all partitions to the new Iceberg table.
After the initial migration, any new data files are added to the new Iceberg table using the Add Files action.

## Enabling Migration from Hive to Iceberg
The Hive table migration actions are supported by the Spark Integration module via Spark Procedures. 
The procedures are bundled in the Spark runtime jar, which is available in the [Iceberg Release Downloads](https://iceberg.apache.org/releases/#downloads).

## Snapshot Hive Table to Iceberg
To snapshot a Hive table, users can run the following Spark SQL:
```sql
CALL catalog_name.system.snapshot('db.source', 'db.dest')
```
See [Spark Procedure: snapshot](../spark-procedures/#snapshot) for more details.

## Migrate Hive Table To Iceberg
To migrate a Hive table to Iceberg, users can run the following Spark SQL:
```sql
CALL catalog_name.system.migrate('db.sample')
```
See [Spark Procedure: migrate](../spark-procedures/#migrate) for more details.

## Add Files From Hive Table to Iceberg
To add data files from a Hive table to a given Iceberg table, users can run the following Spark SQL:
```sql
CALL spark_catalog.system.add_files(
table => 'db.tbl',
source_table => 'db.src_tbl'
)
```
See [Spark Procedure: add_files](../spark-procedures/#add_files) for more details.
