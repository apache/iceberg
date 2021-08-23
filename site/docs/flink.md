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

Apache Iceberg supports both [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API to write records into an Iceberg table. Currently,
we only integrate Iceberg with Apache Flink 1.11.x.

| Feature support                                                        |  Flink 1.11.0      |  Notes                                                 |
|------------------------------------------------------------------------|--------------------|--------------------------------------------------------|
| [SQL create catalog](#creating-catalogs-and-using-catalogs)            | ✔️                 |                                                        |
| [SQL create database](#create-database)                                | ✔️                 |                                                        |
| [SQL create table](#create-table)                                      | ✔️                 |                                                        |
| [SQL create table like](#create-table-like)                            | ✔️                 |                                                        |
| [SQL alter table](#alter-table)                                        | ✔️                 | Only support altering table properties, Columns/PartitionKey changes are not supported now|
| [SQL drop_table](#drop-table)                                          | ✔️                 |                                                        |
| [SQL select](#querying-with-sql)                                       | ✔️                 | Support both streaming and batch mode                  |
| [SQL insert into](#insert-into)                                        | ✔️ ️               | Support both streaming and batch mode                  |
| [SQL insert overwrite](#insert-overwrite)                              | ✔️ ️               |                                                        |
| [DataStream read](#reading-with-datastream)                            | ✔️ ️               |                                                        |
| [DataStream append](#appending-data)                                   | ✔️ ️               |                                                        |
| [DataStream overwrite](#overwrite-data)                                | ✔️ ️               |                                                        |
| [Metadata tables](#inspecting-tables)                                  |    ️               | Support Java API but does not support Flink SQL        |
| [Rewrite files action](#rewrite-files-action)                          | ✔️ ️               |                                                        |

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
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-{{ versions.iceberg }}.jar")

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

### Hive catalog

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
* `property-version`: Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The current property version is `1`. (Optional)
* `warehouse`: The Hive warehouse location, users should specify this path if neither set the `hive-conf-dir` to specify a location containing a `hive-site.xml` configuration file nor add a correct `hive-site.xml` to classpath.
* `hive-conf-dir`: Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. The value of `hive.metastore.warehouse.dir` from `<hive-conf-dir>/hive-site.xml` (or hive configure file from classpath) will be overwrote with the `warehouse` value if setting both `hive-conf-dir` and `warehouse` when creating iceberg catalog.
* `cache-enabled`: Whether to enable catalog cache, default value is `true`

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

* `warehouse`: The HDFS directory to store metadata files and data files. (Required)

We could execute the sql command `USE CATALOG hive_catalog` to set the current catalog.

### Custom catalog

Flink also supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property.
When `catalog-impl` is set, the value of `catalog-type` is ignored. Here is an example:

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
* `WITH ('key'='value', ...)` to set [table configuration](./configuration.md) which will be stored in apache iceberg table properties.

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

### `CREATE TABLE LIKE`

To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql
CREATE TABLE hive_catalog.default.sample (
    id BIGINT COMMENT 'unique id',
    data STRING
);

CREATE TABLE  hive_catalog.default.sample_like LIKE hive_catalog.default.sample;
```

For more details, refer to the [Flink `CREATE TABLE` documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/sql/create.html#create-table).


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

Iceberg support both streaming and batch read in flink now. we could execute the following sql command to switch the execute type from 'streaming' mode to 'batch' mode, and vice versa:

```sql
-- Execute the flink job in streaming mode for current session context
SET execution.type = streaming

-- Execute the flink job in batch mode for current session context
SET execution.type = batch
```

### Flink batch read

If want to check all the rows in iceberg table by submitting a flink __batch__ job, you could execute the following sentences:

```sql
-- Execute the flink job in batch mode for current session context
SET execution.type = batch ;
SELECT * FROM sample       ;
```

### Flink streaming read

Iceberg supports processing incremental data in flink streaming jobs which starts from a historical snapshot-id:

```sql
-- Submit the flink job in streaming mode for current session.
SET execution.type = streaming ;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

-- Read all incremental data starting from the snapshot-id '3821550127947089987' (records from this snapshot will be excluded).
SELECT * FROM sample /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;
```

Those are the options that could be set in flink SQL hint options for streaming job:

* monitor-interval: time interval for consecutively monitoring newly committed data files (default value: '1s').
* start-snapshot-id: the snapshot id that streaming job starts from.

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
env.execute("Test Iceberg Batch Read");
```

There are other options that we could set by Java API, please see the [FlinkSource#Builder](./javadoc/{{ versions.iceberg }}/org/apache/iceberg/flink/source/FlinkSource.html).

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
TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path", hadoopConf);

FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .overwrite(true)
    .build();

env.execute("Test Iceberg DataStream");
```

## Inspecting tables.

Iceberg does not support inspecting table in flink sql now, we need to use [iceberg's Java API](./api.md) to read iceberg's meta data to get those table information.

## Rewrite files action.

Iceberg provides API to rewrite small files into large files by submitting flink batch job. The behavior of this flink action is the same as the spark's [rewriteDataFiles](./maintenance/#compact-data-files).

```java
import org.apache.iceberg.flink.actions.Actions;

TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://nn:8020/warehouse/path");
Table table = tableLoader.loadTable();
RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();
```

For more doc about options of the rewrite files action, please see [RewriteDataFilesAction](./javadoc/{{ versions.iceberg }}/org/apache/iceberg/flink/actions/RewriteDataFilesAction.html)

## Future improvement.

There are some features that we do not yet support in the current flink iceberg integration work:

* Don't support creating iceberg table with hidden partitioning. [Discussion](http://mail-archives.apache.org/mod_mbox/flink-dev/202008.mbox/%3cCABi+2jQCo3MsOa4+ywaxV5J-Z8TGKNZDX-pQLYB-dG+dVUMiMw@mail.gmail.com%3e) in flink mail list.
* Don't support creating iceberg table with computed column.
* Don't support creating iceberg table with watermark.
* Don't support adding columns, removing columns, renaming columns, changing columns. [FLINK-19062](https://issues.apache.org/jira/browse/FLINK-19062) is tracking this.
