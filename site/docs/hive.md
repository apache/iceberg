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

# Hive

## Hive Read Support
Iceberg supports the reading of Iceberg tables from (Hive)[https://hive.apache.org] by using a (StorageHandler)[https://cwiki.apache.org/confluence/display/Hive/StorageHandlers]. 

### Table Creation
This section explains the various steps needed in order to overlay a Hive table "on top of" an existing Iceberg table.

#### Create an Iceberg Table
The first step is to create an Iceberg table using the Spark/Java/Python API. For the purposes of this documentation we will assume that the table is called `table_a` and that the base location of the table is `s3://some_bucket/some_path/table_a`.

#### Create A Hive Table
Now overlay a Hive table over this Iceberg table by issuing Hive DDL like so:
```sql
CREATE EXTERNAL TABLE table_a 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 's3://some_bucket/some_path/table_a';
```
You should now be able to issue Hive SQL `SELECT` queries using the above table and see the results returned.

### Features

#### Predicate pushdown
Pushdown of the Hive SQL `WHERE` clause has been implemented so that filters are pushed to the Iceberg TableScan level as well as the Parquet and ORC Readers.

#### Column selection
The projection of columns from the HiveSQL `SELECT` clause down to the Iceberg readers to reduce the number of columns read is currently being worked on.

### Time Travel and System Tables
Support for accesing Iceberg's time travel feature and other system tables isn't currently supported but there is a plan to add this soon.

## Hive Write Support
Iceberg intends to support the creation of, and writing into, Iceberg tables from Hive. This is currently being worked on and is still considered experimental.
