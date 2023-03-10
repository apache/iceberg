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

To create Iceberg table in Flink, recommended is to use [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) as it's easier for users to understand the concepts.

Download the Flink 1.16.1 binary package from the Apache Flink [download page](https://flink.apache.org/downloads.html). Iceberg uses Scala 2.12 when compiling the Apache `iceberg-flink-runtime` jar, so it's recommended to use Flink 1.16 bundled with Scala 2.12.

```bash
FLINK_VERSION=1.16.1
SCALA_VERSION=2.12
APACHE_FLINK_URL=https://archive.apache.org/dist/flink/
wget ${APACHE_FLINK_URL}/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
tar xzvf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
```

Start a standalone Flink cluster within Hadoop environment:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
APACHE_HADOOP_URL=https://archive.apache.org/dist/hadoop/
HADOOP_VERSION=2.8.5
wget ${APACHE_HADOOP_URL}/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
tar xzvf hadoop-${HADOOP_VERSION}.tar.gz
HADOOP_HOME=`pwd`/hadoop-${HADOOP_VERSION}

export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

# Start the flink standalone cluster
./bin/start-cluster.sh
```

Start the Flink SQL client. There is a separate `flink-runtime` module in the Iceberg project to generate a bundled jar, which could be loaded by Flink SQL client directly. To build the `flink-runtime` bundled jar manually, build the `iceberg` project, and it will generate the jar under `<iceberg-root-dir>/flink-runtime/build/libs`. Or download the `flink-runtime` jar from the [Apache repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.16/{{% icebergVersion %}}/).

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`   

./bin/sql-client.sh embedded -j <flink-runtime-directory>/iceberg-flink-runtime-1.16-{{% icebergVersion %}}.jar shell
```

By default, Iceberg ships with Hadoop jars for Hadoop catalog. To use Hive catalog, load the Hive jars when opening the Flink SQL client. Fortunately, Flink has provided a [bundled hive jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.16.1/flink-sql-connector-hive-2.3.9_2.12-1.16.1.jar) for the SQL client. An example on how to download the dependencies and get started:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

ICEBERG_VERSION={{% icebergVersion %}}
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_MAVEN_URL=${MAVEN_URL}/org/apache/iceberg
ICEBERG_PACKAGE=iceberg-flink-runtime
wget ${ICEBERG_MAVEN_URL}/${ICEBERG_PACKAGE}-${FLINK_VERSION_MAJOR}/${ICEBERG_VERSION}/${ICEBERG_PACKAGE}-${FLINK_VERSION_MAJOR}-${ICEBERG_VERSION}.jar -P lib/

HIVE_VERSION=2.3.9
SCALA_VERSION=2.12
FLINK_VERSION=1.16.1
FLINK_CONNECTOR_URL=${MAVEN_URL}/org/apache/flink
FLINK_CONNECTOR_PACKAGE=flink-sql-connector-hive
wget ${FLINK_CONNECTOR_URL}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}/${FLINK_VERSION}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar

./bin/sql-client.sh embedded shell
```

## Flink's Python API

{{< hint info >}}
PyFlink 1.6.1 [does not work on OSX with a M1 cpu](https://issues.apache.org/jira/browse/FLINK-28786) 
{{< /hint >}}

Install the Apache Flink dependency using `pip`:

```python
pip install apache-flink==1.16.1
```

Provide a `file://` path to the `iceberg-flink-runtime` jar, which can be obtained by building the project and looking at `<iceberg-root-dir>/flink-runtime/build/libs`, or downloading it from the [Apache official repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/). Third-party jars can be added to `pyflink` via:

- `env.add_jars("file:///my/jar/path/connector.jar")`
- `table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar")`

This is also mentioned in the official [docs](https://ci.apache.org/projects/flink/flink-docs-release-1.16/docs/dev/python/dependency_management/). The example below uses `env.add_jars(..)`:

```python
import os

from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-1.16-{{% icebergVersion %}}.jar")

env.add_jars("file://{}".format(iceberg_flink_runtime_jar))
```

Next, create a `StreamTableEnvironment` and execute Flink SQL statements. The below example shows how to create a custom catalog via the Python Table API:

```python
from pyflink.table import StreamTableEnvironment
table_env = StreamTableEnvironment.create(env)
table_env.execute_sql("""
CREATE CATALOG my_catalog WITH (
    'type'='iceberg', 
    'catalog-impl'='com.my.custom.CatalogImpl',
    'my-additional-catalog-config'='my-value'
)
""")
```

Run a query:

```python
(table_env
    .sql_query("SELECT PULocationID, DOLocationID, passenger_count FROM my_catalog.nyc.taxis LIMIT 5")
    .execute()
    .print()) 
```

```
+----+----------------------+----------------------+--------------------------------+
| op |         PULocationID |         DOLocationID |                passenger_count |
+----+----------------------+----------------------+--------------------------------+
| +I |                  249 |                   48 |                            1.0 |
| +I |                  132 |                  233 |                            1.0 |
| +I |                  164 |                  107 |                            1.0 |
| +I |                   90 |                  229 |                            1.0 |
| +I |                  137 |                  249 |                            1.0 |
+----+----------------------+----------------------+--------------------------------+
5 rows in set
```

For more details, please refer to the [Python Table API](https://ci.apache.org/projects/flink/flink-docs-release-1.16/docs/dev/python/table/intro_to_table_api/).

## Creating catalogs and using catalogs.

Flink support to create catalogs by using Flink SQL.

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
* `catalog-type`: `hive`, `hadoop` or `rest` for built-in catalogs, or left unset for custom catalog implementations using catalog-impl. (Optional)
* `catalog-impl`: The fully-qualified class name of a custom catalog implementation. Must be set if `catalog-type` is unset. (Optional)
* `property-version`: Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The current property version is `1`. (Optional)
* `cache-enabled`: Whether to enable catalog cache, default value is `true`. (Optional)
* `cache.expiration-interval-ms`: How long catalog entries are locally cached, in milliseconds; negative values like `-1` will disable expiration, value 0 is not allowed to set. default value is `-1`. (Optional)

### Hive catalog

This creates an Iceberg catalog named `hive_catalog` that can be configured using `'catalog-type'='hive'`, which loads tables from Hive metastore:

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
* `hive-conf-dir`: Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. The value of `hive.metastore.warehouse.dir` from `<hive-conf-dir>/hive-site.xml` (or hive configure file from classpath) will be overwritten with the `warehouse` value if setting both `hive-conf-dir` and `warehouse` when creating iceberg catalog.
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

Execute the sql command `USE CATALOG hadoop_catalog` to set the current catalog.

### REST catalog

This creates an iceberg catalog named `rest_catalog` that can be configured using `'catalog-type'='rest'`, which loads tables from a REST catalog:

```sql
CREATE CATALOG rest_catalog WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='https://localhost/'
);
```

The following properties can be set if using the REST catalog:

* `uri`: The URL to the REST Catalog (Required)
* `credential`: A credential to exchange for a token in the OAuth2 client credentials flow (Optional)
* `token`: A token which will be used to interact with the server (Optional)

### Custom catalog

Flink also supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property:

```sql
CREATE CATALOG my_catalog WITH (
  'type'='iceberg',
  'catalog-impl'='com.my.custom.CatalogImpl',
  'my-additional-catalog-config'='my-value'
);
```

### Create through YAML config

Catalogs can be registered in `sql-client-defaults.yaml` before starting the SQL client.

```yaml
catalogs: 
  - name: my_catalog
    type: iceberg
    catalog-type: hadoop
    warehouse: hdfs://nn:8020/warehouse/path
```

### Create through SQL Files

The Flink SQL Client supports the `-i` startup option to execute an initialization SQL file to set up environment when starting up the SQL Client.

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

Using `-i <init.sql>` option to initialize SQL Client session:

```bash
/path/to/bin/sql-client.sh -i /path/to/init.sql
```

## DDL commands

### `CREATE DATABASE`

By default, Iceberg will use the `default` database in Flink. Using the following example to create a separate database in order to avoid creating tables under the `default` database:

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

Table create commands support the commonly used [Flink create clauses](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/create/) including: 

* `PARTITION BY (column1, column2, ...)` to configure partitioning, Flink does not yet support hidden partitioning.
* `COMMENT 'table document'` to set a table description.
* `WITH ('key'='value', ...)` to set [table configuration](../configuration) which will be stored in Iceberg table properties.

Currently, it does not support computed column, primary key and watermark definition etc.

### `PARTITIONED BY`

To create a partition table, use `PARTITIONED BY`:

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
) PARTITIONED BY (data);
```

Iceberg support hidden partition but Flink don't support partitioning by a function on columns, so there is no way to support hidden partition in Flink DDL.

### `CREATE TABLE LIKE`

To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
);

CREATE TABLE  `hive_catalog`.`default`.`sample_like` LIKE `hive_catalog`.`default`.`sample`;
```

For more details, refer to the [Flink `CREATE TABLE` documentation](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/sql/create/).


### `ALTER TABLE`

Iceberg only support altering table properties:

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

Iceberg support both streaming and batch read in Flink. Execute the following sql command to switch execution mode from `streaming` to `batch`, and vice versa:

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.runtime-mode = streaming;

-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

### Flink batch read

Submit a Flink __batch__ job using the following sentences:

```sql
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
SELECT * FROM sample;
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

There are some options that could be set in Flink SQL hint options for streaming job, see [read options](#Read-options) for details.

### FLIP-27 source for SQL

Here are the SQL settings for the [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) source. All other SQL settings and options documented above are applicable to the FLIP-27 source.

```sql
-- Opt in the FLIP-27 source. Default is false.
SET table.exec.iceberg.use-flip27-source = true;
```

## Writing with SQL

Iceberg support both `INSERT INTO` and `INSERT OVERWRITE`.

### `INSERT INTO`

To append new data to a table with a Flink streaming job, use `INSERT INTO`:

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

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
  `id`  INT UNIQUE COMMENT 'unique id',
  `data` STRING NOT NULL,
 PRIMARY KEY(`id`) NOT ENFORCED
) with ('format-version'='2', 'write.upsert.enabled'='true');
```

2. Enabling `UPSERT` mode using `upsert-enabled` in the [write options](#Write-options) provides more flexibility than a table level config. Note that you still need to use v2 table format and specify the primary key when creating the table.

```sql
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

There are other options that can be set, please see the [FlinkSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/FlinkSource.html).

## Reading with DataStream (FLIP-27 source)

[FLIP-27 source interface](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface)
was introduced in Flink 1.12. It aims to solve several shortcomings of the old `SourceFunction`
streaming source interface. It also unifies the source interfaces for both batch and streaming executions.
Most source connectors (like Kafka, file) in Flink repo have  migrated to the FLIP-27 interface.
Flink is planning to deprecate the old `SourceFunction` interface in the near future.

A FLIP-27 based Flink `IcebergSource` is added in `iceberg-flink` module. The FLIP-27 `IcebergSource` is currently an experimental feature.

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

There are other options that could be set by Java API, please see the 
[IcebergSource#Builder](../../../javadoc/{{% icebergVersion %}}/org/apache/iceberg/flink/source/IcebergSource.html).

### Read as Avro GenericRecord

FLIP-27 Iceberg source provides `AvroGenericRecordReaderFunction` that converts
Flink `RowData` Avro `GenericRecord`. You can use the convert to read from
Iceberg table as Avro GenericRecord DataStream.

Please make sure `flink-avro` jar is included in the classpath.
Also `iceberg-flink-runtime` shaded bundle jar can't be used
because the runtime jar shades the avro package.
Please use non-shaded `iceberg-flink` jar instead.

```java
TableLoader tableLoader = ...;
Table table;
try (TableLoader loader = tableLoader) {
    loader.open();
    table = loader.loadTable();
}

AvroGenericRecordReaderFunction readerFunction = AvroGenericRecordReaderFunction.fromTable(table);

IcebergSource<GenericRecord> source =
    IcebergSource.<GenericRecord>builder()
        .tableLoader(tableLoader)
        .readerFunction(readerFunction)
        .assignerFactory(new SimpleSplitAssignerFactory())
        ...
        .build();

DataStream<Row> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
    "Iceberg Source as Avro GenericRecord", new GenericRecordAvroTypeInfo(avroSchema));
```

## Writing with DataStream

Iceberg support writing to iceberg table from different DataStream input.


### Appending data.

Flink supports writing `DataStream<RowData>` and `DataStream<Row>` to the sink iceberg table natively.

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

The iceberg API also allows users to write generic `DataStream<T>` to iceberg table, more example could be found in this [unit test](https://github.com/apache/iceberg/blob/master/flink/v1.16/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkIcebergSink.java).

### Overwrite data

To overwrite the data in existing iceberg table dynamically, set the `overwrite` flag in FlinkSink builder.

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

To upsert the data in existing iceberg table, set the `upsert` flag in FlinkSink builder. The table must use v2 table format and have a primary key.

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

### Write with Avro GenericRecord

Flink Iceberg sink provides `AvroGenericRecordToRowDataMapper` that converts
Avro `GenericRecord` to Flink `RowData`. You can use the mapper to write 
Avro GenericRecord DataStream to Iceberg.

Please make sure `flink-avro` jar is included in the classpath.
Also `iceberg-flink-runtime` shaded bundle jar can't be used
because the runtime jar shades the avro package.
Please use non-shaded `iceberg-flink` jar instead.

```java
DataStream<org.apache.avro.generic.GenericRecord> dataStream = ...;

Schema icebergSchema = table.schema();

// if the Iceberg table schema contains time fields, and can't use
// the Avro schema converted from Iceberg schema via AvroSchemaUtil.
// Instead, use the Avro schema defined directly.
// See AvroGenericRecordToRowDataMapper Javadoc for more details.
org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, table.name());

GenericRecordAvroTypeInfo avroTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
RowType rowType = FlinkSchemaUtil.convert(icebergSchema);

FlinkSink.builderFor(
    dataStream,
    AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema),
    FlinkCompatibilityUtil.toTypeInfo(rowType))
  .table(table)
  .tableLoader(tableLoader)
  .append();
```

### Netrics

The following Flink metrics are provided by the Flink Iceberg sink.

Parallel writer metrics are added under the sub group of `IcebergStreamWriter`. 
They should have the following key-value tags.
* table: full table name (like iceberg.my_db.my_table)
* subtask_index: writer subtask index starting from 0

 Metric name                | Metric type | Description                                                                                         |
| ------------------------- |------------|-----------------------------------------------------------------------------------------------------|
| lastFlushDurationMs       | Gague      | The duration (in milli) that writer subtasks take to flush and upload the files during checkpoint.  |
| flushedDataFiles          | Counter    | Number of data files flushed and uploaded.                                                          |
| flushedDeleteFiles        | Counter    | Number of delete files flushed and uploaded.                                                        |
| flushedReferencedDataFiles| Counter    | Number of data files referenced by the flushed delete files.                                        |
| dataFilesSizeHistogram    | Histogram  | Histogram distribution of data file sizes (in bytes).                                               |
| deleteFilesSizeHistogram  | Histogram  | Histogram distribution of delete file sizes (in bytes).                                             |

Committer metrics are added under the sub group of `IcebergFilesCommitter`.
They should have the following key-value tags.
* table: full table name (like iceberg.my_db.my_table)

 Metric name                      | Metric type | Description                                                                |
|---------------------------------|--------|----------------------------------------------------------------------------|
| lastCheckpointDurationMs        | Gague  | The duration (in milli) that the committer operator checkpoints its state. |
| lastCommitDurationMs            | Gague  | The duration (in milli) that the Iceberg table commit takes.               |
| committedDataFilesCount         | Counter | Number of data files committed.                                            |
| committedDataFilesRecordCount   | Counter | Number of records contained in the committed data files.                   |
| committedDataFilesByteCount     | Counter | Number of bytes contained in the committed data files.                     |
| committedDeleteFilesCount       | Counter | Number of delete files committed.                                          |
| committedDeleteFilesRecordCount | Counter | Number of records contained in the committed delete files.                 |
| committedDeleteFilesByteCount   | Counter | Number of bytes contained in the committed delete files.                   |
| elapsedSecondsSinceLastSuccessfulCommit| Gague  | Elapsed time (in seconds) since last successful Iceberg commit.            |

`elapsedSecondsSinceLastSuccessfulCommit` is an ideal alerting metric
to detect failed or missing Iceberg commits.
* Iceberg commit happened after successful Flink checkpoint in the `notifyCheckpointComplete` callback.
It could happen that Iceberg commits failed (for whatever reason), while Flink checkpoints succeeding.
* It could also happen that `notifyCheckpointComplete` wasn't triggered (for whatever bug).
As a result, there won't be any Iceberg commits attempted.

If the checkpoint interval (and expected Iceberg commit interval) is 5 minutes, set up alert with rule like `elapsedSecondsSinceLastSuccessfulCommit > 60 minutes` to detect failed or missing Iceberg commits in the past hour.

## Options
### Read options

Flink read options are passed when configuring the Flink IcebergSource:

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


## Inspecting tables

To inspect a table's history, snapshots, and other metadata, Iceberg supports metadata tables.

Metadata tables are identified by adding the metadata table name after the original table name. For example, history for `db.table` is read using `db.table$history`.

### History

To show table history:

```sql
SELECT * FROM prod.db.table$history;
```

| made_current_at         | snapshot_id         | parent_id           | is_current_ancestor |
| ----------------------- | ------------------- | ------------------- | ------------------- |
| 2019-02-08 03:29:51.215 | 5781947118336215154 | NULL                | true                |
| 2019-02-08 03:47:55.948 | 5179299526185056830 | 5781947118336215154 | true                |
| 2019-02-09 16:24:30.13  | 296410040247533544  | 5179299526185056830 | false               |
| 2019-02-09 16:32:47.336 | 2999875608062437330 | 5179299526185056830 | true                |
| 2019-02-09 19:42:03.919 | 8924558786060583479 | 2999875608062437330 | true                |
| 2019-02-09 19:49:16.343 | 6536733823181975045 | 8924558786060583479 | true                |

{{< hint info >}}
**This shows a commit that was rolled back.** In this example, snapshot 296410040247533544 and 2999875608062437330 have the same parent snapshot 5179299526185056830. Snapshot 296410040247533544 was rolled back and is *not* an ancestor of the current table state.
{{< /hint >}}

### Metadata Log Entries

To show table metadata log entries:

```sql
SELECT * from prod.db.table$metadata_log_entries;
```

| timestamp               | file                                                         | latest_snapshot_id | latest_schema_id | latest_sequence_number |
| ----------------------- | ------------------------------------------------------------ | ------------------ | ---------------- | ---------------------- |
| 2022-07-28 10:43:52.93  | s3://.../table/metadata/00000-9441e604-b3c2-498a-a45a-6320e8ab9006.metadata.json | null               | null             | null                   |
| 2022-07-28 10:43:57.487 | s3://.../table/metadata/00001-f30823df-b745-4a0a-b293-7532e0c99986.metadata.json | 170260833677645300 | 0                | 1                      |
| 2022-07-28 10:43:58.25  | s3://.../table/metadata/00002-2cc2837a-02dc-4687-acc1-b4d86ea486f4.metadata.json | 958906493976709774 | 0                | 2                      |

### Snapshots

To show the valid snapshots for a table:

```sql
SELECT * FROM prod.db.table$snapshots;
```

| committed_at            | snapshot_id    | parent_id | operation | manifest_list                                      | summary                                                      |
| ----------------------- | -------------- | --------- | --------- | -------------------------------------------------- | ------------------------------------------------------------ |
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | { added-records -> 2478404, total-records -> 2478404, added-data-files -> 438, total-data-files -> 438, flink.job-id -> 2e274eecb503d85369fb390e8956c813 } |

You can also join snapshots to table history. For example, this query will show table history, with the application ID that wrote each snapshot:

```sql
select
    h.made_current_at,
    s.operation,
    h.snapshot_id,
    h.is_current_ancestor,
    s.summary['flink.job-id']
from prod.db.table$history h
join prod.db.table$snapshots s
  on h.snapshot_id = s.snapshot_id
order by made_current_at
```

| made_current_at         | operation | snapshot_id    | is_current_ancestor | summary[flink.job-id]            |
| ----------------------- | --------- | -------------- | ------------------- | -------------------------------- |
| 2019-02-08 03:29:51.215 | append    | 57897183625154 | true                | 2e274eecb503d85369fb390e8956c813 |

### Files

To show a table's current data files:

```sql
SELECT * FROM prod.db.table$files;
```

| content | file_path                                                    | file_format | spec_id | partition        | record_count | file_size_in_bytes | column_sizes       | value_counts     | null_value_counts | nan_value_counts | lower_bounds    | upper_bounds    | key_metadata | split_offsets | equality_ids | sort_order_id |
| ------- | ------------------------------------------------------------ | ----------- | ------- | ---------------- | ------------ | ------------------ | ------------------ | ---------------- | ----------------- | ---------------- | --------------- | --------------- | ------------ | ------------- | ------------ | ------------- |
| 0       | s3:/.../table/data/00000-3-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 01} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> c] | [1 -> , 2 -> c] | null         | [4]           | null         | null          |
| 0       | s3:/.../table/data/00001-4-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 02} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> b] | [1 -> , 2 -> b] | null         | [4]           | null         | null          |
| 0       | s3:/.../table/data/00002-5-8d6d60e8-d427-4809-bcf0-f5d45a4aad96.parquet | PARQUET     | 0       | {1999-01-01, 03} | 1            | 597                | [1 -> 90, 2 -> 62] | [1 -> 1, 2 -> 1] | [1 -> 0, 2 -> 0]  | []               | [1 -> , 2 -> a] | [1 -> , 2 -> a] | null         | [4]           | null         | null          |

### Manifests

To show a table's current file manifests:

```sql
SELECT * FROM prod.db.table$manifests;
```

| path                                                         | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries                  |
| ------------------------------------------------------------ | ------ | ----------------- | ------------------- | ---------------------- | ------------------------- | ------------------------ | ------------------------------------ |
| s3://.../table/metadata/45b5290b-ee61-4788-b324-b1e2735c0e10-m0.avro | 4479   | 0                 | 6668963634911763636 | 8                      | 0                         | 0                        | [[false,null,2019-05-13,2019-05-15]] |

Note:

1. Fields within `partition_summaries` column of the manifests table correspond to `field_summary` structs within [manifest list](../../../spec#manifest-lists), with the following order:
   - `contains_null`
   - `contains_nan`
   - `lower_bound`
   - `upper_bound`
2. `contains_nan` could return null, which indicates that this information is not available from the file's metadata.
   This usually occurs when reading from V1 table, where `contains_nan` is not populated.

### Partitions

To show a table's current partitions:

```sql
SELECT * FROM prod.db.table$partitions;
```

| partition      | record_count | file_count | spec_id |
| -------------- | ------------ | ---------- | ------- |
| {20211001, 11} | 1            | 1          | 0       |
| {20211002, 11} | 1            | 1          | 0       |
| {20211001, 10} | 1            | 1          | 0       |
| {20211002, 10} | 1            | 1          | 0       |

Note:
For unpartitioned tables, the partitions table will contain only the record_count and file_count columns.

### All Metadata Tables

These tables are unions of the metadata tables specific to the current snapshot, and return metadata across all snapshots.

{{< hint danger >}}
The "all" metadata tables may produce more than one row per data file or manifest file because metadata files may be part of more than one table snapshot.
{{< /hint >}}

#### All Data Files

To show all of the table's data files and each file's metadata:

```sql
SELECT * FROM prod.db.table$all_data_files;
```

| content | file_path                                                    | file_format | partition  | record_count | file_size_in_bytes | column_sizes       | value_counts       | null_value_counts | nan_value_counts | lower_bounds            | upper_bounds            | key_metadata | split_offsets | equality_ids | sort_order_id |
| ------- | ------------------------------------------------------------ | ----------- | ---------- | ------------ | ------------------ | ------------------ | ------------------ | ----------------- | ---------------- | ----------------------- | ----------------------- | ------------ | ------------- | ------------ | ------------- |
| 0       | s3://.../dt=20210102/00000-0-756e2512-49ae-45bb-aae3-c0ca475e7879-00001.parquet | PARQUET     | {20210102} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210102} | {1 -> 2, 2 -> 20210102} | null         | [4]           | null         | 0             |
| 0       | s3://.../dt=20210103/00000-0-26222098-032f-472b-8ea5-651a55b21210-00001.parquet | PARQUET     | {20210103} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210103} | {1 -> 3, 2 -> 20210103} | null         | [4]           | null         | 0             |
| 0       | s3://.../dt=20210104/00000-0-a3bb1927-88eb-4f1c-bc6e-19076b0d952e-00001.parquet | PARQUET     | {20210104} | 14           | 2444               | {1 -> 94, 2 -> 17} | {1 -> 14, 2 -> 14} | {1 -> 0, 2 -> 0}  | {}               | {1 -> 1, 2 -> 20210104} | {1 -> 3, 2 -> 20210104} | null         | [4]           | null         | 0             |

#### All Manifests

To show all of the table's manifest files:

```sql
SELECT * FROM prod.db.table$all_manifests;
```

| path                                                         | length | partition_spec_id | added_snapshot_id   | added_data_files_count | existing_data_files_count | deleted_data_files_count | partition_summaries                  |
| ------------------------------------------------------------ | ------ | ----------------- | ------------------- | ---------------------- | ------------------------- | ------------------------ | ------------------------------------ |
| s3://.../metadata/a85f78c5-3222-4b37-b7e4-faf944425d48-m0.avro | 6376   | 0                 | 6272782676904868561 | 2                      | 0                         | 0                        | [{false, false, 20210101, 20210101}] |

Note:

1. Fields within `partition_summaries` column of the manifests table correspond to `field_summary` structs within [manifest list](../../../spec#manifest-lists), with the following order:
   - `contains_null`
   - `contains_nan`
   - `lower_bound`
   - `upper_bound`
2. `contains_nan` could return null, which indicates that this information is not available from the file's metadata.
   This usually occurs when reading from V1 table, where `contains_nan` is not populated.

### References

To show a table's known snapshot references:

```sql
SELECT * FROM prod.db.table$refs;
```

| name    | type   | snapshot_id         | max_reference_age_in_ms | min_snapshots_to_keep | max_snapshot_age_in_ms |
| ------- | ------ | ------------------- | ----------------------- | --------------------- | ---------------------- |
| main    | BRANCH | 4686954189838128572 | 10                      | 20                    | 30                     |
| testTag | TAG    | 4686954189838128572 | 10                      | null                  | null                   |

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

There are some features that are do not yet supported in the current Flink Iceberg integration work:

* Don't support creating iceberg table with hidden partitioning. [Discussion](http://mail-archives.apache.org/mod_mbox/flink-dev/202008.mbox/%3cCABi+2jQCo3MsOa4+ywaxV5J-Z8TGKNZDX-pQLYB-dG+dVUMiMw@mail.gmail.com%3e) in flink mail list.
* Don't support creating iceberg table with computed column.
* Don't support creating iceberg table with watermark.
* Don't support adding columns, removing columns, renaming columns, changing columns. [FLINK-19062](https://issues.apache.org/jira/browse/FLINK-19062) is tracking this.
