---
title: "Table Migration"
url: table-migration
weight: 1300
menu: main
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

## Migration Approaches
There are two main approaches to perform table migration: CTAS (Create Table As Select) and in-place migration.

### Create-Table-As-Select Migration
CTAS migration involves creating a new Iceberg table and copying data from the existing table to the new one. This method is preferred when you want to completely cut ties with your old table, ensuring the new table is independent and fully managed by Iceberg.
However, CTAS migration may require more time to complete and might not be suitable for production use cases where downtime is not acceptable.

```bash
  source_table_folder/         migrated_table_folder/
  ├── data/                    ├── data/
  │   ├── data_file1           │   ├── data_file1_copy
  │   ├── data_file2           │   ├── data_file2_copy
  │   ├── data_file3           │   ├── data_file3_copy
  │   └── ...                  │   └── ...
  └── metadata/                └── metadata/
      ├── metadata_file1           ├── metadata_file2
      └── ...                      └── ...
```
### In-Place Migration
In-place migration retains the existing data files but adds Iceberg metadata on top of them. This approach is faster and does not require copying data, making it more suitable for production use cases.

```bash
  source_table_folder/         migrated_table_folder/
  ├── data/                    ├── data/
  │   ├── data_file1           │   ├── <data_file_1_ptr>
  │   ├── data_file2           │   ├── <data_file_2_ptr>
  │   ├── data_file3           │   ├── <data_file_3_ptr>
  │   └── ...                  │   └── ...
  └── metadata/                └── metadata/
      ├── metadata_file1           ├── metadata_file2
      └── ...                      └── ...
```
## In-Place Migration Actions
Apache Iceberg primarily supports the in-place migration approach, which includes three important actions:

1. Snapshot Table
2. Migrate Table
3. Add Files

### Snapshot Table
The Snapshot Table action creates a new iceberg table with the same schema and partitioning as the source table, leaving the source table unchanged during and after the action.

### Migrate Table
The Migrate Table action also creates a new Iceberg table with the same schema and partitioning as the source table. However, during the action execution, it locks and drops the source table from the catalog.
Consequently, Migrate Table requires all readers and writers working on the source table to be stopped before the action is performed.

### Add Files
After the initial step (either Snapshot Table or Migrate Table), it is common to find some data files that have not been migrated. These files often originate from concurrent writers who continue writing to the source table during or after the migration process.
In practice, these files can be new data files in Hive tables or new snapshots (versions) of Delta Lake tables. The Add Files action is essential for incorporating these files into the Iceberg table.

## In-Place Migration Completion
Once all data files have been migrated and there are no more concurrent writers writing to the source table, the migration process is complete.
Readers and writers can now switch to the new Iceberg table for their operations.

## Migration Implementation: From Hive/Spark to Iceberg
Apache Hive and Apache Spark are two popular data warehouse systems used for big data processing and analysis.
However, both systems do not natively support time travel or rollback to previous snapshots.
When migrating data to an Iceberg table, which provides versioning and transactional updates, only the most recent data files need to be migrated.

Iceberg supports all three migration actions: Snapshot Table, Migrate Table, and Add Files for migrating from Hive or Spark tables to Iceberg tables. Since Hive or Spark tables do not maintain snapshots,
the migration process essentially involves creating a new Iceberg table with the existing schema and committing all data files across all partitions to the new Iceberg table.
After the initial migration, any new data files are added to the new Iceberg table using the Add Files action.

For more details on how to perform the migration on Hive/Spark tables, please refer to the [Table Migration via Spark Procedures](../spark-procedures/#table-migration) page.

## Migration Implementation: From Delta Lake to Iceberg
Delta Lake is a popular data storage system that provides time travel and versioning features. When migrating data from Delta Lake to Iceberg,
it is common to migrate all snapshots to maintain the history of the data.

Currently, Iceberg only supports the Snapshot Table action for migrating from Delta Lake to Iceberg tables. Since Delta Lake tables maintain snapshots, all available snapshots will be committed to the new Iceberg table as transactions in order.
For Delta Lake tables, any additional data files added after the initial migration will be included in their corresponding snapshots and subsequently added to the new Iceberg table using the Add Snapshot action. The Add Snapshot action, a variant of the Add File action, is still under development.

For more details on how to perform the migration on Delta Lake tables, please refer to the [Delta Lake Migration](../delta-lake-migration/#delta-lake-table-migration) page.
