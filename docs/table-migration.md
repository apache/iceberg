---
title: "Overview"
url: table-migration
menu:
  main:
    parent: "Migration"
    weight: 100
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
# Table Migration
Apache Iceberg supports converting existing tables in other formats to Iceberg tables. This section introduces the general concept of table migration, its approaches, and existing implementations in Iceberg.

## Migration Approach
There are two primary methods for executing table migration: full data migration and in-place migration. In this doc, we will describe more about in-place migration.

In-place migration preserves the existing data files while incorporating Iceberg metadata on top of them.
This method is not only faster but also eliminates the need for data duplication, making it more appropriate for production environments.

![In-Place Migration](../../../img/iceberg-in-place-migration.png)

Apache Iceberg primarily supports the in-place migration approach, which includes three important actions: `Snapshot Table`, `Migrate Table`, and `Add Files`.

## Snapshot Table
The Snapshot Table action creates a new iceberg table with the same schema and partitioning as the source table, leaving the source table unchanged during and after the action.

- Create a new Iceberg table with the same metadata (schema, partition spec, etc.) as the source table

![Snapshot Table Step 1](../../../img/iceberg-snapshotaction-step1.png)

- Commit all data files across all partitions to the new Iceberg table. The source table remains unchanged.

![Snapshot Table Step 2](../../../img/iceberg-snapshotaction-step2.png)
## Migrate Table
The Migrate Table action also creates a new Iceberg table with the same schema and partitioning as the source table. However, during the action execution, it locks and drops the source table from the catalog.
Consequently, Migrate Table requires all modifications working on the source table to be stopped before the action is performed.

Stop all writers interacting with the source table. Readers that also support Iceberg may continue reading.

![Migrate Table Step 1](../../../img/iceberg-migrateaction-step1.png)

- Create a new Iceberg table with the same identifier and metadata (schema, partition spec, etc.) as the source table. Rename the source table for a backup in case of failure and rollback.

![Migrate Table Step 2](../../../img/iceberg-migrateaction-step2.png)

- Commit all data files across all partitions to the new Iceberg table. Drop the source table.

![Migrate Table Step 3](../../../img/iceberg-migrateaction-step3.png)
## Add Files
After the initial step (either Snapshot Table or Migrate Table), it is common to find some data files that have not been migrated. These files often originate from concurrent writers who continue writing to the source table during or after the migration process.
In practice, these files can be new data files in Hive tables or new snapshots (versions) of Delta Lake tables. The Add Files action is essential for incorporating these files into the Iceberg table.

## In-Place Migration Completion
Once all data files have been migrated and there are no more concurrent writers writing to the source table, the migration process is complete.
Readers and writers can now switch to the new Iceberg table for their operations.

# Migrating From Different Table Formats
## From Hive to Iceberg
Apache Hive supports ORC, Parquet, and Avro file formats that could be migrated to Iceberg.
When migrating data to an Iceberg table, which provides versioning and transactional updates, only the most recent data files need to be migrated.

Iceberg supports all three migration actions: Snapshot Table, Migrate Table, and Add Files for migrating from Hive tables to Iceberg tables. Since Hive tables do not maintain snapshots,
the migration process essentially involves creating a new Iceberg table with the existing schema and committing all data files across all partitions to the new Iceberg table.
After the initial migration, any new data files are added to the new Iceberg table using the Add Files action.

For more details on how to perform the migration on Hive tables, please refer to the [Hive Table Migration via Spark Procedures](../spark-procedures/#table-migration) page.

## From Delta Lake to Iceberg
Delta Lake is a table format that supports Parquet file format and provides time travel and versioning features. When migrating data from Delta Lake to Iceberg,
it is common to migrate all snapshots to maintain the history of the data.

Currently, Iceberg only supports the Snapshot Table action for migrating from Delta Lake to Iceberg tables. Since Delta Lake tables maintain snapshots, all available snapshots will be committed to the new Iceberg table as transactions in order.
For Delta Lake tables, any additional data files added after the initial migration will be included in their corresponding snapshots and subsequently added to the new Iceberg table using the Add Snapshot action. The Add Snapshot action, a variant of the Add File action, is still under development.

For more details on how to perform the migration on Delta Lake tables, please refer to the [Delta Lake Migration](../delta-lake-migration/#delta-lake-table-migration) page.
