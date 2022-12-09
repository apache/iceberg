---
title: "Enabling Iceberg in Flink"
url: flink
menu:
    main:
        parent: Flink
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

# Flink

Apache Iceberg supports both [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API. See the [Multi-Engine Support#apache-flink](https://iceberg.apache.org/multi-engine-support/#apache-flink) page for the integration of Apache Flink.

| Feature support                                             | Flink  | Notes                                                        |
| ----------------------------------------------------------- | -----  | ------------------------------------------------------------ |
| [SQL create catalog](#creating-catalogs-and-using-catalogs) | ✔️     |                                                              |
| [SQL create database](#create-database)                     | ✔️     |                                                              |
| [SQL create table](#create-table)                           | ✔️     |                                                              |
| [SQL create table like](#create-table-like)                 | ✔️     |                                                              |
| [SQL alter table](#alter-table)                             | ✔️     | Only support altering table properties, column and partition changes are not supported |
| [SQL drop_table](#drop-table)                               | ✔️     |                                                              |
| [SQL select](#querying-with-sql)                            | ✔️     | Support both streaming and batch mode                        |
| [SQL insert into](#insert-into)                             | ✔️ ️   | Support both streaming and batch mode                        |
| [SQL insert overwrite](#insert-overwrite)                   | ✔️ ️   |                                                              |
| [DataStream read](#reading-with-datastream)                 | ✔️ ️   |                                                              |
| [DataStream append](#appending-data)                        | ✔️ ️   |                                                              |
| [DataStream overwrite](#overwrite-data)                     | ✔️ ️   |                                                              |
| [Metadata tables](#inspecting-tables)                       | ️      | Support Java API but does not support Flink SQL              |
| [Rewrite files action](#rewrite-files-action)               | ✔️ ️   |                                                              |

## Preparation when using Flink SQL Client

To create iceberg table in flink, we recommend to use [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) because it's easier for users to understand the concepts.

Step.1 Downloading the flink 1.11.x binary package from the apache flink [download page](https://flink.apache.org/downloads.html). We now use scala 2.12 to archive the apache iceberg-flink-runtime jar, so it's recommended to use flink 1.11 bundled with scala 2.12.

```bash
FLINK_VERSION=1.11.1
SCALA_VERSION=2.12
APACHE_FLINK_URL=archive.apache.org/dist/flink/
wget ${APACHE_FLINK_URL}/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
tar xzvf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
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

By default, iceberg has included hadoop jars for hadoop catalog. If we want to use hive catalog, we will need to load the hive jars when opening the flink sql client. Fortunately, apache flink has provided a [bundled hive jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.6_2.11/1.11.0/flink-sql-connector-hive-2.3.6_2.11-1.11.0.jar) for sql client. So we could open the sql client
as the following:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# download Iceberg dependency
ICEBERG_VERSION=0.11.1
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_MAVEN_URL=${MAVEN_URL}/org/apache/iceberg
ICEBERG_PACKAGE=iceberg-flink-runtime
wget ${ICEBERG_MAVEN_URL}/${ICEBERG_PACKAGE}/${ICEBERG_VERSION}/${ICEBERG_PACKAGE}-${ICEBERG_VERSION}.jar

# download the flink-sql-connector-hive-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar
HIVE_VERSION=2.3.6
SCALA_VERSION=2.11
FLINK_VERSION=1.11.0
FLINK_CONNECTOR_URL=${MAVEN_URL}/org/apache/flink
FLINK_CONNECTOR_PACKAGE=flink-sql-connector-hive
wget ${FLINK_CONNECTOR_URL}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}/${FLINK_VERSION}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar

# open the SQL client.
/path/to/bin/sql-client.sh embedded \
    -j ${ICEBERG_PACKAGE}-${ICEBERG_VERSION}.jar \
    -j ${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar \
    shell
```
## Preparation when using Flink's Python API

Install the Apache Flink dependency using `pip`
```python
pip install apache-flink==1.11.1
```

In order for `pyflink` to function properly, it needs to have access to all Hadoop jars. For `pyflink`
we need to copy those Hadoop jars to the installation directory of `pyflink`, which can be found under
`<PYTHON_ENV_INSTALL_DIR>/site-packages/pyflink/lib/` (see also a mention of this on
the [Flink ML](http://mail-archives.apache.org/mod_mbox/flink-user/202105.mbox/%3C3D98BDD2-89B1-42F5-B6F4-6C06A038F978%40gmail.com%3E)).
We can use the following short Python script to copy all Hadoop jars (you need to make sure that `HADOOP_HOME`
points to your Hadoop installation):

```python
import os
import shutil
import site


def copy_all_hadoop_jars_to_pyflink():
    if not os.getenv("HADOOP_HOME"):
        raise Exception("The HADOOP_HOME env var must be set and point to a valid Hadoop installation")

    jar_files = []

    def find_pyflink_lib_dir():
        for dir in site.getsitepackages():
            package_dir = os.path.join(dir, "pyflink", "lib")
            if os.path.exists(package_dir):
                return package_dir
        return None

    for root, _, files in os.walk(os.getenv("HADOOP_HOME")):
        for file in files:
            if file.endswith(".jar"):
                jar_files.append(os.path.join(root, file))

    pyflink_lib_dir = find_pyflink_lib_dir()

    num_jar_files = len(jar_files)
    print(f"Copying {num_jar_files} Hadoop jar files to pyflink's lib directory at {pyflink_lib_dir}")
    for jar in jar_files:
        shutil.copy(jar, pyflink_lib_dir)


if __name__ == '__main__':
    copy_all_hadoop_jars_to_pyflink()
```

Once the script finished, you should see output similar to
```
Copying 645 Hadoop jar files to pyflink's lib directory at <PYTHON_DIR>/lib/python3.8/site-packages/pyflink/lib
```

Now we need to provide a `file://` path to the `iceberg-flink-runtime` jar, which we can either get by building the project
and looking at `<iceberg-root-dir>/flink-runtime/build/libs`, or downloading it from the [Apache official repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/).
Third-party libs can be added to `pyflink` via `env.add_jars("file:///my/jar/path/connector.jar")` / `table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar")`, which is also mentioned in the official [docs](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/python/dependency_management/).
In our example we're using `env.add_jars(..)` as shown below:

```python
import os

from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-{{% icebergVersion %}}.jar")

env.add_jars("file://{}".format(iceberg_flink_runtime_jar))
```

Once we reached this point, we can then create a `StreamTableEnvironment` and execute Flink SQL statements. 
The below example shows how to create a custom catalog via the Python Table API:
```python
from pyflink.table import StreamTableEnvironment
table_env = StreamTableEnvironment.create(env)
table_env.execute_sql("CREATE CATALOG my_catalog WITH ("
                      "'type'='iceberg', "
                      "'catalog-impl'='com.my.custom.CatalogImpl', "
                      "'my-additional-catalog-config'='my-value')")
```

For more details, please refer to the [Python Table API](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/python/table/intro_to_table_api/).

## Creating catalogs and using catalogs.

Flink 1.11 support to create catalogs by using flink sql.

### Catalog Configuration

A catalog is created and named by executing the following query (replace `<catalog_name>` with your catalog name and
`<config_key>`=`<config_value>` with catalog implementation config):   

```sql
CREATE CATALOG <catalog_name> WITH (
  'type'='iceberg',
  `<config_key>`=`<config_value>`
); 
```

The following properties can be set globally and are not limited to a specific catalog implementation:

* `type`: Must be `iceberg`. (required)
* `catalog-type`: `hive` or `hadoop` for built-in catalogs, or left unset for custom catalog implementations using catalog-impl. (Optional)
* `catalog-impl`: The fully-qualified class name of a custom catalog implementation. Must be set if `catalog-type` is unset. (Optional)
* `property-version`: Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The current property version is `1`. (Optional)
* `cache-enabled`: Whether to enable catalog cache, default value is `true`. (Optional)
* `cache.expiration-interval-ms`: How long catalog entries are locally cached, in milliseconds; negative values like `-1` will disable expiration, value 0 is not allowed to set. default value is `-1`. (Optional)

### Hive catalog

This creates an iceberg catalog named `hive_catalog` that can be configured using `'catalog-type'='hive'`, which loads tables from a hive metastore:

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

The following properties can be set if using the Hive catalog:

* `uri`: The Hive metastore's thrift URI. (Required)
* `clients`: The Hive metastore client pool size, default value is 2. (Optional)
* `warehouse`: The Hive warehouse location, users should specify this path if neither set the `hive-conf-dir` to specify a location containing a `hive-site.xml` configuration file nor add a correct `hive-site.xml` to classpath.
* `hive-conf-dir`: Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. The value of `hive.metastore.warehouse.dir` from `<hive-conf-dir>/hive-site.xml` (or hive configure file from classpath) will be overwrote with the `warehouse` value if setting both `hive-conf-dir` and `warehouse` when creating iceberg catalog.
* `hadoop-conf-dir`: Path to a directory containing `core-site.xml` and `hdfs-site.xml` configuration files which will be used to provide custom Hadoop configuration values. 

### Hadoop catalog

Iceberg also supports a directory-based catalog in HDFS that can be configured using `'catalog-type'='hadoop'`:

```sql
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://nn:8020/warehouse/path',
  'property-version'='1'
);
```

The following properties can be set if using the Hadoop catalog:

* `warehouse`: The HDFS directory to store metadata files and data files. (Required)

We could execute the sql command `USE CATALOG hive_catalog` to set the current catalog.

### Custom catalog

Flink also supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property. Here is an example:

```sql
CREATE CATALOG my_catalog WITH (
  'type'='iceberg',
  'catalog-impl'='com.my.custom.CatalogImpl',
  'my-additional-catalog-config'='my-value'
);
```

### Create through YAML config

Catalogs can be registered in `sql-client-defaults.yaml` before starting the SQL client. Here is an example:

```yaml
catalogs: 
  - name: my_catalog
    type: iceberg
    catalog-type: hadoop
    warehouse: hdfs://nn:8020/warehouse/path
```

### Create through SQL Files

Since the `sql-client-defaults.yaml` file was removed in flink 1.14, SQL Client supports the -i startup option to execute an initialization SQL file to setup environment when starting up the SQL Client.
An example of such a file is presented below.
```sql
-- define available catalogs
CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://localhost:9083',
  'warehouse'='hdfs://nn:8020/warehouse/path'
);

USE CATALOG hive_catalog;
```
using -i <init.sql> option to initialize SQL Client session
```bash
/path/to/bin/sql-client.sh -i /path/to/init.sql
```



## DDL commands

### `CREATE DATABASE`

By default, iceberg will use the `default` database in flink. Using the following example to create a separate database if we don't want to create tables under the `default` database:

```sql
CREATE DATABASE iceberg_db;
USE iceberg_db;
```

### `CREATE TABLE`

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
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
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
) PARTITIONED BY (data);
```

Apache Iceberg support hidden partition but apache flink don't support partitioning by a function on columns, so we've no way to support hidden partition in flink DDL now, we will improve apache flink DDL in future.

### `CREATE TABLE LIKE`

To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
);

CREATE TABLE  `hive_catalog`.`default`.`sample_like` LIKE `hive_catalog`.`default`.`sample`;
```

For more details, refer to the [Flink `CREATE TABLE` documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/create.html#create-table).


### `ALTER TABLE`

Iceberg only support altering table properties in flink 1.11 now.

```sql
ALTER TABLE `hive_catalog`.`default`.`sample` SET ('write.format.default'='avro')
```

### `ALTER TABLE .. RENAME TO`

```sql
ALTER TABLE `hive_catalog`.`default`.`sample` RENAME TO `hive_catalog`.`default`.`new_sample`;
```

### `DROP TABLE`

To delete a table, run:

```sql
DROP TABLE `hive_catalog`.`default`.`sample`;
```

## Querying with SQL

Iceberg support both streaming and batch read in flink now. we could execute the following sql command to switch the execute type from 'streaming' mode to 'batch' mode, and vice versa:

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.runtime-mode = streaming;

-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

### Flink batch read

If want to check all the rows in iceberg table by submitting a flink __batch__ job, you could execute the following sentences:

```sql
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
SELECT * FROM sample       ;
```

### Flink streaming read

Iceberg supports processing incremental data in flink streaming jobs which starts from a historical snapshot-id:

```sql
-- Submit the flink job in streaming mode for current session.
SET execution.runtime-mode = streaming;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

-- Read all incremental data starting from the snapshot-id '3821550127947089987' (records from this snapshot will be excluded).
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;
```

Those are the options that could be set in flink SQL hint options for streaming job:

* monitor-interval: time interval for consecutively monitoring newly committed data files (default value: '10s').
* start-snapshot-id: the snapshot id that streaming job starts from.

### FLIP-27 source for SQL

Here are the SQL settings for the FLIP-27 source, which is only available
for Flink 1.14 or above.  All other SQL settings and options
documented above are applicable to the FLIP-27 source.

```sql
-- Opt in the FLIP-27 source. Default is false.
SET table.exec.iceberg.use-flip27-source = true;
```

## Writing with SQL

Iceberg support both `INSERT INTO` and `INSERT OVERWRITE` in flink 1.11 now.

### `INSERT INTO`

To append new data to a table with a flink streaming job, use `INSERT INTO`:

```sql
INSERT INTO `hive_catalog`.`default`.`sample` VALUES (1, 'a');
INSERT INTO `hive_catalog`.`default`.`sample` SELECT id, data from other_kafka_table;
```

### `INSERT OVERWRITE`

To replace data in the table with the result of a query, use `INSERT OVERWRITE` in batch job (flink streaming job does not support `INSERT OVERWRITE`). Overwrites are atomic operations for Iceberg tables.

Partitions that have rows produced by the SELECT query will be replaced, for example:

```sql
INSERT OVERWRITE sample VALUES (1, 'a');
```

Iceberg also support overwriting given partitions by the `select` values:

```sql
INSERT OVERWRITE `hive_catalog`.`default`.`sample` PARTITION(data='a') SELECT 6;
```

For a partitioned iceberg table, when all the partition columns are set a value in `PARTITION` clause, it is inserting into a static partition, otherwise if partial partition columns (prefix part of all partition columns) are set a value in `PARTITION` clause, it is writing the query result into a dynamic partition.
For an unpartitioned iceberg table, its data will be completely overwritten by `INSERT OVERWRITE`.

### `UPSERT` 

Iceberg supports `UPSERT` based on the primary key when writing data into v2 table format. There are two ways to enable upsert.

1. Enable the `UPSERT` mode as table-level property `write.upsert.enabled`. Here is an example SQL statement to set the table property when creating a table. It would be applied for all write paths to this table (batch or streaming) unless overwritten by write options as described later.

```
CREATE TABLE `hive_catalog`.`default`.`sample` (
  `id`  INT UNIQUE COMMENT 'unique id',
  `data` STRING NOT NULL,
 PRIMARY KEY(`id`) NOT ENFORCED
) with ('format-version'='2', 'write.upsert.enabled'='true');
```

2. Enabling `UPSERT` mode using `upsert-enabled` in the [write options](#Write-options) provides more flexibility than a table level config. Note that you still need to use v2 table format and specify the primary key when creating the table.

```
INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */
...
```

{{< hint info >}}
OVERWRITE and UPSERT can't be set together. In UPSERT mode, if the table is partitioned, the partition fields should be included in equality fields.
{{< /hint >}}

## Reading with DataStream

Iceberg support streaming or batch read in Java API now.

### Batch Read

This example will read all records from iceberg table and then print to the stdout console in flink batch job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> batch = FlinkSource.forRowData()
     .env(env)
     .tableLoader(tableLoader)
     .streaming(false)
     .build();

// Print all records to stdout.
batch.print();

// Submit and execute this batch read job.
env.execute("Test Iceberg Batch Read");
```

### Streaming read

This example will read incremental records which start from snapshot-id '3821550127947089987' and print to stdout console in flink streaming job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
DataStream<RowData> stream = FlinkSource.forRowData()
     .env(env)
     .tableLoader(tableLoader)
     .streaming(true)
     .startSnapshotId(3821550127947089987L)
     .build();

// Print all records to stdout.
stream.print();

// Submit and execute this streaming read job.
env.execute("Test Iceberg Streaming Read");
```

There are other options that we could set by Java API, please see the [FlinkSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/FlinkSource.html).

## Reading with DataStream (FLIP-27 source)

[FLIP-27 source interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
was introduced in Flink 1.12. It aims to solve several shortcomings of the old `SourceFunction`
streaming source interface. It also unifies the source interfaces for both batch and streaming executions.
Most source connectors (like Kafka, file) in Flink repo have  migrated to the FLIP-27 interface.
Flink is planning to deprecate the old `SourceFunction` interface in the near future.

A FLIP-27 based Flink `IcebergSource` is added in `iceberg-flink` module for Flink 1.14 or above.
The FLIP-27 `IcebergSource` is currently an experimental feature.

### Batch Read

This example will read all records from iceberg table and then print to the stdout console in flink batch job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");

IcebergSource<RowData> source = IcebergSource.forRowData()
    .tableLoader(tableLoader)
    .assignerFactory(new SimpleSplitAssignerFactory())
    .build();

DataStream<RowData> batch = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "My Iceberg Source",
    TypeInformation.of(RowData.class));

// Print all records to stdout.
batch.print();

// Submit and execute this batch read job.
env.execute("Test Iceberg Batch Read");
```

### Streaming read

This example will start the streaming read from the latest table snapshot (inclusive).
Every 60s, it polls Iceberg table to discover new append-only snapshots.
CDC read is not supported yet.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");

IcebergSource source = IcebergSource.forRowData()
    .tableLoader(tableLoader)
    .assignerFactory(new SimpleSplitAssignerFactory())
    .streaming(true)
    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
    .monitorInterval(Duration.ofSeconds(60))
    .build()

DataStream<RowData> stream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "My Iceberg Source",
    TypeInformation.of(RowData.class));

// Print all records to stdout.
stream.print();

// Submit and execute this streaming read job.
env.execute("Test Iceberg Streaming Read");
```

There are other options that we could set by Java API, please see the 
[IcebergSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/IcebergSource.html).


## Writing with DataStream

Iceberg support writing to iceberg table from different DataStream input.


### Appending data.

we have supported writing `DataStream<RowData>` and `DataStream<Row>` to the sink iceberg table natively.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .append();

env.execute("Test Iceberg DataStream");
```

The iceberg API also allows users to write generic `DataStream<T>` to iceberg table, more example could be found in this [unit test](https://github.com/apache/iceberg/blob/master/flink/v1.15/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkIcebergSink.java).

### Overwrite data

To overwrite the data in existing iceberg table dynamically, we could set the `overwrite` flag in FlinkSink builder.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .append();

env.execute("Test Iceberg DataStream");
```

### Upsert data

To upsert the data in existing iceberg table, we could set the `upsert` flag in FlinkSink builder. The table must use v2 table format and have a primary key.

```java
StreamExecutionEnvironment env = ...;

DataStream<RowData> input = ... ;
Configuration hadoopConf = new Configuration();
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .upsert(true)
    .append();

env.execute("Test Iceberg DataStream");
```

{{< hint info >}}
OVERWRITE and UPSERT can't be set together. In UPSERT mode, if the table is partitioned, the partition fields should be included in equality fields.
{{< /hint >}}

## Options
### Read options

Flink read options are passed when configuring the Flink IcebergSource, like this:

```
IcebergSource.forRowData()
    .tableLoader(TableLoader.fromCatalog(...))
    .assignerFactory(new SimpleSplitAssignerFactory())
    .streaming(true)
    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
    .startSnapshotId(3821550127947089987L)
    .monitorInterval(Duration.ofMillis(10L)) // or .set("monitor-interval", "10s") \ set(FlinkReadOptions.MONITOR_INTERVAL, "10s")
    .build()
```
For Flink SQL, read options can be passed in via SQL hints like this:
```
SELECT * FROM tableName /*+ OPTIONS('monitor-interval'='10s') */
...
```

Options can be passed in via Flink configuration, which will be applied to current session. Note that not all options support this mode.

```
env.getConfig()
    .getConfiguration()
    .set(FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION, 1000L);
...
```

`Read option` has the highest priority, followed by `Flink configuration` and then `Table property`.

| Read option                 | Flink configuration                           | Table property               | Default                          | Description                                                  |
| --------------------------- | --------------------------------------------- | ---------------------------- | -------------------------------- | ------------------------------------------------------------ |
| snapshot-id                 | N/A                                           | N/A                          | null                             | For time travel in batch mode. Read data from the specified snapshot-id. |
| case-sensitive              | connector.iceberg.case-sensitive              | N/A                          | false                            | If true, match column name in a case sensitive way.          |
| as-of-timestamp             | N/A                                           | N/A                          | null                             | For time travel in batch mode. Read data from the most recent snapshot as of the given time in milliseconds. |
| starting-strategy           | connector.iceberg.starting-strategy           | N/A                          | INCREMENTAL_FROM_LATEST_SNAPSHOT | Starting strategy for streaming execution. TABLE_SCAN_THEN_INCREMENTAL: Do a regular table scan then switch to the incremental mode. The incremental mode starts from the current snapshot exclusive. INCREMENTAL_FROM_LATEST_SNAPSHOT: Start incremental mode from the latest snapshot inclusive. If it is an empty map, all future append snapshots should be discovered. INCREMENTAL_FROM_EARLIEST_SNAPSHOT: Start incremental mode from the earliest snapshot inclusive. If it is an empty map, all future append snapshots should be discovered. INCREMENTAL_FROM_SNAPSHOT_ID: Start incremental mode from a snapshot with a specific id inclusive. INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP: Start incremental mode from a snapshot with a specific timestamp inclusive. If the timestamp is between two snapshots, it should start from the snapshot after the timestamp. Just for FIP27 Source. |
| start-snapshot-timestamp    | N/A                                           | N/A                          | null                             | Start to read data from the most recent snapshot as of the given time in milliseconds. |
| start-snapshot-id           | N/A                                           | N/A                          | null                             | Start to read data from the specified snapshot-id.           |
| end-snapshot-id             | N/A                                           | N/A                          | The latest snapshot id           | Specifies the end snapshot.                                  |
| split-size                  | connector.iceberg.split-size                  | read.split.target-size       | 128 MB                           | Target size when combining input splits.                     |
| split-lookback              | connector.iceberg.split-file-open-cost        | read.split.planning-lookback | 10                               | Number of bins to consider when combining input splits.      |
| split-file-open-cost        | connector.iceberg.split-file-open-cost        | read.split.open-file-cost    | 4MB                              | The estimated cost to open a file, used as a minimum weight when combining splits. |
| streaming                   | connector.iceberg.streaming                   | N/A                          | false                            | Sets whether the current task runs in streaming or batch mode. |
| monitor-interval            | connector.iceberg.monitor-interval            | N/A                          | 60s                              | Monitor interval to discover splits from new snapshots. Applicable only for streaming read. |
| include-column-stats        | connector.iceberg.include-column-stats        | N/A                          | false                            | Create a new scan from this that loads the column stats with each data file. Column stats include: value count, null value count, lower bounds, and upper bounds. |
| max-planning-snapshot-count | connector.iceberg.max-planning-snapshot-count | N/A                          | Integer.MAX_VALUE                | Max number of snapshots limited per split enumeration. Applicable only to streaming read. |
| limit                       | connector.iceberg.limit                       | N/A                          | -1                               | Limited output number of rows.                               |


### Write options

Flink write options are passed when configuring the FlinkSink, like this:

```
FlinkSink.Builder builder = FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
    .table(table)
    .tableLoader(tableLoader)
    .set("write-format", "orc")
    .set(FlinkWriteOptions.OVERWRITE_MODE, "true");
```
For Flink SQL, write options can be passed in via SQL hints like this:
```
INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */
...
```

| Flink option           | Default                                    | Description                                                                                                |
|------------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------|
| write-format           | Table write.format.default                 | File format to use for this write operation; parquet, avro, or orc                                         |
| target-file-size-bytes | As per table property                      | Overrides this table's write.target-file-size-bytes                                                        |
| upsert-enabled         | Table write.upsert.enabled                 | Overrides this table's write.upsert.enabled                                                                |
| overwrite-enabled      | false                                      | Overwrite the table's data, overwrite mode shouldn't be enable when configuring to use UPSERT data stream. |
| distribution-mode      | Table write.distribution-mode              | Overrides this table's write.distribution-mode                                                             |
| compression-codec      | Table write.(fileformat).compression-codec | Overrides this table's compression codec for this write                                                    |
| compression-level      | Table write.(fileformat).compression-level | Overrides this table's compression level for Parquet and Avro tables for this write                        |
| compression-strategy   | Table write.orc.compression-strategy       | Overrides this table's compression strategy for ORC tables for this write                                  |


## Inspecting tables.

Iceberg does not support inspecting table in flink sql now, we need to use [iceberg's Java API](../api) to read iceberg's meta data to get those table information.

## Rewrite files action.

Iceberg provides API to rewrite small files into large files by submitting flink batch job. The behavior of this flink action is the same as the spark's [rewriteDataFiles](../maintenance/#compact-data-files).

```java
import org.apache.iceberg.flink.actions.Actions;

TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
Table table = tableLoader.loadTable();
RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();
```

For more doc about options of the rewrite files action, please see [RewriteDataFilesAction](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/actions/RewriteDataFilesAction.html)

## Type conversion

Iceberg's integration for Flink automatically converts between Flink and Iceberg types. When writing to a table with types that are not supported by Flink, like UUID, Iceberg will accept and convert values from the Flink type.

### Flink to Iceberg

Flink types are converted to Iceberg types according to the following table:

| Flink               | Iceberg                    | Notes         |
|-----------------    |----------------------------|---------------|
| boolean             | boolean                    |               |
| tinyint             | integer                    |               |
| smallint            | integer                    |               |
| integer             | integer                    |               |
| bigint              | long                       |               |
| float               | float                      |               |
| double              | double                     |               |
| char                | string                     |               |
| varchar             | string                     |               |
| string              | string                     |               |
| binary              | binary                     |               |
| varbinary           | fixed                      |               |
| decimal             | decimal                    |               |
| date                | date                       |               |
| time                | time                       |               |
| timestamp           | timestamp without timezone |               |
| timestamp_ltz       | timestamp with timezone    |               |
| array               | list                       |               |
| map                 | map                        |               |
| multiset            | map                        |               |
| row                 | struct                     |               |
| raw                 |                            | Not supported |
| interval            |                            | Not supported |
| structured          |                            | Not supported |
| timestamp with zone |                            | Not supported |
| distinct            |                            | Not supported |
| null                |                            | Not supported |
| symbol              |                            | Not supported |
| logical             |                            | Not supported |

### Iceberg to Flink

Iceberg types are converted to Flink types according to the following table:

| Iceberg                    | Flink                 |
|----------------------------|-----------------------|
| boolean                    | boolean               |
| struct                     | row                   |
| list                       | array                 |
| map                        | map                   |
| integer                    | integer               |
| long                       | bigint                |
| float                      | float                 |
| double                     | double                |
| date                       | date                  |
| time                       | time                  |
| timestamp without timezone | timestamp(6)          |
| timestamp with timezone    | timestamp_ltz(6)      |
| string                     | varchar(2147483647)   |
| uuid                       | binary(16)            |
| fixed(N)                   | binary(N)             |
| binary                     | varbinary(2147483647) |
| decimal(P, S)              | decimal(P, S)         |

## Future improvement.

There are some features that we do not yet support in the current flink iceberg integration work:

* Don't support creating iceberg table with hidden partitioning. [Discussion](http://mail-archives.apache.org/mod_mbox/flink-dev/202008.mbox/%3cCABi+2jQCo3MsOa4+ywaxV5J-Z8TGKNZDX-pQLYB-dG+dVUMiMw@mail.gmail.com%3e) in flink mail list.
* Don't support creating iceberg table with computed column.
* Don't support creating iceberg table with watermark.
* Don't support adding columns, removing columns, renaming columns, changing columns. [FLINK-19062](https://issues.apache.org/jira/browse/FLINK-19062) is tracking this.
