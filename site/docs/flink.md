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

# Flink

Apache Iceberg support both [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API to write records into iceberg table. Currently,
we only integrate iceberg with apache flink 1.11.x .

| Feature support                                                        |  Flink 1.11.0      |  Notes                                                 |
|------------------------------------------------------------------------|--------------------|--------------------------------------------------------|
| [SQL create catalog](#creating-catalogs-and-using-catalogs)            | ✔️                 |                                                        |
| [SQL create database](#create-database)                                | ✔️                 |                                                        |
| [SQL create table](#create-table)                                      | ✔️                 |                                                        |
| [SQL alter table](#alter-table)                                        | ✔️                 | Only support altering table properties, Columns/PartitionKey changes are not supported now|
| [SQL drop_table](#drop-table)                                          | ✔️                 |                                                        |
| [SQL select](#querying-with-sql)                                       | ✔️                 | Only support batch mode now.                           |
| [SQL insert into](#insert-into)                                        | ✔️ ️               | Support both streaming and batch mode                  |
| [SQL insert overwrite](#insert-overwrite)                              | ✔️ ️               |                                                        |
| [DataStream read](#reading-with-datastream)                            | ✔️ ️               |                                                        |
| [DataStream append](#appending-data)                                   | ✔️ ️               |                                                        |
| [DataStream overwrite](#overwrite-data)                                | ✔️ ️               |                                                        |
| [Metadata tables](#inspecting-tables)                                  |    ️               |                                                        |

## Preparation

To create iceberg table in flink, we recommend to use [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) because it's easier for users to understand the concepts.

Step.1 Downloading the flink 1.11.x binary package from the apache flink [download page](https://flink.apache.org/downloads.html). We now use scala 2.12 to archive the apache iceberg-flink-runtime jar, so it's recommended to use flink 1.11 bundled with scala 2.12.

```bash
wget https://downloads.apache.org/flink/flink-1.11.1/flink-1.11.1-bin-scala_2.12.tgz
tar xzvf flink-1.11.1-bin-scala_2.12.tgz
```

Step.2 Start a standalone flink cluster within hadoop environment.

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the flink standalone cluster
./bin/start-cluster.sh
```

Step.3 Start the flink SQL client.

We've created a separate `flink-runtime` module in iceberg project to generate a bundled jar, which could be loaded by flink SQL client directly.

If we want to build the `flink-runtime` bundled jar manually, please just build the `iceberg` project and it will generate the jar under `<iceberg-root-dir>/flink-runtime/build/libs`. Of course, we could also download the `flink-runtime` jar from the [apache official repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/).

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

./bin/sql-client.sh embedded -j <flink-runtime-directory>/iceberg-flink-runtime-xxx.jar shell
```

By default, iceberg has included hadoop jars for hadoop catalog. If we want to use hive catalog, we will need to load the hive jars when opening the flink sql client. Fortunately, apache flink has provided a [bundled hive jar](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/hive/#using-bundled-hive-jar) for sql client. So we could open the sql client
as the following:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# wget the flink-sql-connector-hive-2.3.6_2.11-1.11.0.jar from the above bundled jar URL firstly.

# open the SQL client.
./bin/sql-client.sh embedded \
    -j <flink-runtime-directory>/iceberg-flink-runtime-xxx.jar \
    -j <hive-bundlded-jar-directory>/flink-sql-connector-hive-2.3.6_2.11-1.11.0.jar \
    shell
```

## Creating catalogs and using catalogs.

Flink 1.11 support to create catalogs by using flink sql.

This creates an iceberg catalog named `hive_catalog` that loads tables from a hive metastore:

```sql
CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://localhost:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://nn:8020/warehouse/path'
);
```

* `type`: Please just use `iceberg` for iceberg table format. (Required)
* `catalog-type`: Iceberg currently support `hive` or `hadoop` catalog type. (Required)
* `uri`: The Hive metastore's thrift URI. (Required)
* `clients`: The Hive metastore client pool size, default value is 2. (Optional)
* `property-version`: Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The currently property version is `1`. (Optional)
* `warehouse`: The Hive warehouse location, users should specify this path if neither set the `hive-conf-dir` to specify a location containing a `hive-site.xml` configuration file nor add a correct `hive-site.xml` to classpath.
* `hive-conf-dir`: Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. The value of `hive.metastore.warehouse.dir` from `<hive-conf-dir>/hive-site.xml` (or hive configure file from classpath) will be overwrote with the `warehouse` value if setting both `hive-conf-dir` and `warehouse` when creating iceberg catalog.

Iceberg also supports a directory-based catalog in HDFS that can be configured using `'catalog-type'='hadoop'`:

```sql
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://nn:8020/warehouse/path',
  'property-version'='1'
);
```

* `warehouse`: The HDFS directory to store metadata files and data files. (Required)

We could execute the sql command `USE CATALOG hive_catalog` to set the current catalog.

## DDL commands

### `CREATE DATABASE`

By default, iceberg will use the `default` database in flink. Using the following example to create a separate database if we don't want to create tables under the `default` database:

```sql
CREATE DATABASE iceberg_db;
USE iceberg_db;
```

### `CREATE TABLE`

```sql
CREATE TABLE hive_catalog.default.sample (
    id BIGINT COMMENT 'unique id',
    data STRING
);
```

Table create commands support the most commonly used [flink create clauses](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/create.html#create-table) now, including: 

* `PARTITION BY (column1, column2, ...)` to configure partitioning, apache flink does not yet support hidden partitioning.
* `COMMENT 'table document'` to set a table description.
* `WITH ('key'='value', ...)` to set [table configuration](../configuration) which will be stored in apache iceberg table properties.

Currently, it does not support computed column, primary key and watermark definition etc.

### `PARTITIONED BY`

To create a partition table, use `PARTITIONED BY`:

```sql
CREATE TABLE hive_catalog.default.sample (
    id BIGINT COMMENT 'unique id',
    data STRING
) PARTITIONED BY (data);
```

Apache Iceberg support hidden partition but apache flink don't support partitioning by a function on columns, so we've no way to support hidden partition in flink DDL now, we will improve apache flink DDL in future.

### `ALTER TABLE`

Iceberg only support altering table properties in flink 1.11 now.

```sql
ALTER TABLE hive_catalog.default.sample SET ('write.format.default'='avro')
```

### `ALTER TABLE .. RENAME TO`

```sql
ALTER TABLE hive_catalog.default.sample RENAME TO hive_catalog.default.new_sample;
```

### `DROP TABLE`

To delete a table, run:

```sql
DROP TABLE hive_catalog.default.sample;
```

## Querying with SQL

Iceberg does not support streaming read in flink now, it's still working in-progress. But it support batch read to scan the existing records in iceberg table.

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.type = batch ;
SELECT * FROM sample       ;
```

Notice: we could execute the following sql command to switch the execute type from 'streaming' mode to 'batch' mode, and vice versa:

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.type = streaming

-- Execute the flink job in batch mode for current session context
SET execution.type = batch
```

## Writing with SQL

Iceberg support both `INSERT INTO` and `INSERT OVERWRITE` in flink 1.11 now.

### `INSERT INTO`

To append new data to a table with a flink streaming job, use `INSERT INTO`:

```sql
INSERT INTO hive_catalog.default.sample VALUES (1, 'a');
INSERT INTO hive_catalog.default.sample SELECT id, data from other_kafka_table;
```

### `INSERT OVERWRITE`

To replace data in the table with the result of a query, use `INSERT OVERWRITE` in batch job (flink streaming job does not support `INSERT OVERWRITE`). Overwrites are atomic operations for Iceberg tables.

Partitions that have rows produced by the SELECT query will be replaced, for example:

```sql
INSERT OVERWRITE sample VALUES (1, 'a');
```

Iceberg also support overwriting given partitions by the `select` values:

```sql
INSERT OVERWRITE hive_catalog.default.sample PARTITION(data='a') SELECT 6;
```

For a partitioned iceberg table, when all the partition columns are set a value in `PARTITION` clause, it is inserting into a static partition, otherwise if partial partition columns (prefix part of all partition columns) are set a value in `PARTITION` clause, it is writing the query result into a dynamic partition.
For an unpartitioned iceberg table, its data will be completely overwritten by `INSERT OVERWRITE`.

## Reading with DataStream

Iceberg does not support streaming or batch read now, but it's working in-progress.

## Writing with DataStream

Iceberg support writing to iceberg table from different DataStream input.


### Appending data.

we have supported writing `DataStream<RowData>` and `DataStream<Row` to the sink iceberg table natively.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .hadoopConf(hadoopConf)
    .build();

env.execute("Test Iceberg DataStream");
```

The iceberg API also allows users to write generic `DataStream<T>` to iceberg table, more example could be found in this [unit test](https://github.com/apache/iceberg/blob/master/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkIcebergSink.java).

### Overwrite data

To overwrite the data in existing iceberg table dynamically, we could set the `overwrite` flag in FlinkSink builder.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadooptable("hdfs://nn:8020/warehouse/path");

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .hadoopConf(hadoopConf)
    .build();

env.execute("Test Iceberg DataStream");
```

## Inspecting tables.

Iceberg does not support inspecting table in flink sql now, we need to use [iceberg's Java API](../api) to read iceberg's meta data to get those table information.

## Future improvement.

There are some features that we do not yet support in the current flink iceberg integration work:

* Don't support creating iceberg table with hidden partitioning. [Discussion](http://mail-archives.apache.org/mod_mbox/flink-dev/202008.mbox/%3cCABi+2jQCo3MsOa4+ywaxV5J-Z8TGKNZDX-pQLYB-dG+dVUMiMw@mail.gmail.com%3e) in flink mail list.
* Don't support creating iceberg table with computed column.
* Don't support creating iceberg table with watermark.
* Don't support adding columns, removing columns, renaming columns, changing columns. [FLINK-19062](https://issues.apache.org/jira/browse/FLINK-19062) is tracking this.
* Don't support flink read iceberg table in streaming mode. [#1383](https://github.com/apache/iceberg/issues/1383) is tracking this.
