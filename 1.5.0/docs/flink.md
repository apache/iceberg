---
title: "Flink Getting Started"
search:
  exclude: true
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

Apache Iceberg supports both [Apache Flink](https://flink.apache.org/)'s DataStream API and Table API. See the [Multi-Engine Support](../../multi-engine-support.md#apache-flink) page for the integration of Apache Flink.

| Feature support                                          | Flink | Notes                                                                                  |
| -------------------------------------------------------- |-------|----------------------------------------------------------------------------------------|
| [SQL create catalog](flink-ddl.md#create-catalog) | ✔️    |                                                                                        |
| [SQL create database](flink-ddl.md#create-database) | ✔️    |                                                                                        |
| [SQL create table](flink-ddl.md#create-table)                        | ✔️    |                                                                                        |
| [SQL create table like](flink-ddl.md#create-table-like)              | ✔️    |                                                                                        |
| [SQL alter table](flink-ddl.md#alter-table)                          | ✔️    | Only support altering table properties, column and partition changes are not supported |
| [SQL drop_table](flink-ddl.md#drop-table)                            | ✔️    |                                                                                        |
| [SQL select](flink-queries.md#reading-with-sql)                         | ✔️    | Support both streaming and batch mode                                                  |
| [SQL insert into](flink-writes.md#insert-into)                          | ✔️ ️  | Support both streaming and batch mode                                                  |
| [SQL insert overwrite](flink-writes.md#insert-overwrite)                | ✔️ ️  |                                                                                        |
| [DataStream read](flink-queries.md#reading-with-datastream)              | ✔️ ️  |                                                                                        |
| [DataStream append](flink-writes.md#appending-data)                    | ✔️ ️  |                                                                                        |
| [DataStream overwrite](flink-writes.md#overwrite-data)                 | ✔️ ️  |                                                                                        |
| [Metadata tables](flink-queries.md#inspecting-tables)                    | ✔️    |                                                                                        |
| [Rewrite files action](flink-actions.md#rewrite-files-action)           | ✔️ ️  |                                                                                        |

## Preparation when using Flink SQL Client

To create Iceberg table in Flink, it is recommended to use [Flink SQL Client](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sqlClient.html) as it's easier for users to understand the concepts.

Download Flink from the [Apache download page](https://flink.apache.org/downloads.html). Iceberg uses Scala 2.12 when compiling the Apache `iceberg-flink-runtime` jar, so it's recommended to use Flink 1.16 bundled with Scala 2.12.

```bash
FLINK_VERSION=1.16.2
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

<!-- markdown-link-check-disable-next-line -->
Start the Flink SQL client. There is a separate `flink-runtime` module in the Iceberg project to generate a bundled jar, which could be loaded by Flink SQL client directly. To build the `flink-runtime` bundled jar manually, build the `iceberg` project, and it will generate the jar under `<iceberg-root-dir>/flink-runtime/build/libs`. Or download the `flink-runtime` jar from the [Apache repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.16/{{ icebergVersion }}/).

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`   

# Below works for 1.15 or less
./bin/sql-client.sh embedded -j <flink-runtime-directory>/iceberg-flink-runtime-1.15-{{ icebergVersion }}.jar shell

# 1.16 or above has a regression in loading external jar via -j option. See FLINK-30035 for details.
put iceberg-flink-runtime-1.16-{{ icebergVersion }}.jar in flink/lib dir
./bin/sql-client.sh embedded shell
```

By default, Iceberg ships with Hadoop jars for Hadoop catalog. To use Hive catalog, load the Hive jars when opening the Flink SQL client. Fortunately, Flink has provided a [bundled hive jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.9_2.12/1.16.2/flink-sql-connector-hive-2.3.9_2.12-1.16.2.jar) for the SQL client. An example on how to download the dependencies and get started:

```bash
# HADOOP_HOME is your hadoop root directory after unpack the binary package.
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

ICEBERG_VERSION={{ icebergVersion }}
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_MAVEN_URL=${MAVEN_URL}/org/apache/iceberg
ICEBERG_PACKAGE=iceberg-flink-runtime
wget ${ICEBERG_MAVEN_URL}/${ICEBERG_PACKAGE}-${FLINK_VERSION_MAJOR}/${ICEBERG_VERSION}/${ICEBERG_PACKAGE}-${FLINK_VERSION_MAJOR}-${ICEBERG_VERSION}.jar -P lib/

HIVE_VERSION=2.3.9
SCALA_VERSION=2.12
FLINK_VERSION=1.16.2
FLINK_CONNECTOR_URL=${MAVEN_URL}/org/apache/flink
FLINK_CONNECTOR_PACKAGE=flink-sql-connector-hive
wget ${FLINK_CONNECTOR_URL}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}/${FLINK_VERSION}/${FLINK_CONNECTOR_PACKAGE}-${HIVE_VERSION}_${SCALA_VERSION}-${FLINK_VERSION}.jar

./bin/sql-client.sh embedded shell
```

## Flink's Python API

!!! info
    PyFlink 1.6.1 [does not work on OSX with a M1 cpu](https://issues.apache.org/jira/browse/FLINK-28786)


Install the Apache Flink dependency using `pip`:

```python
pip install apache-flink==1.16.2
```

Provide a `file://` path to the `iceberg-flink-runtime` jar, which can be obtained by building the project and looking at `<iceberg-root-dir>/flink-runtime/build/libs`, or downloading it from the [Apache official repository](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime/). Third-party jars can be added to `pyflink` via:

- `env.add_jars("file:///my/jar/path/connector.jar")`
- `table_env.get_config().get_configuration().set_string("pipeline.jars", "file:///my/jar/path/connector.jar")`

This is also mentioned in the official [docs](https://ci.apache.org/projects/flink/flink-docs-release-1.16/docs/dev/python/dependency_management/). The example below uses `env.add_jars(..)`:

```python
import os

from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-1.16-{{ icebergVersion }}.jar")

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

## Adding catalogs.

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
* `catalog-type`: `hive`, `hadoop`, `rest`, `glue`, `jdbc` or `nessie` for built-in catalogs, or left unset for custom catalog implementations using catalog-impl. (Optional)
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

##  Creating a table

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
);
```

## Writing

To append new data to a table with a Flink streaming job, use `INSERT INTO`:

```sql
INSERT INTO `hive_catalog`.`default`.`sample` VALUES (1, 'a');
INSERT INTO `hive_catalog`.`default`.`sample` SELECT id, data from other_kafka_table;
```

To replace data in the table with the result of a query, use `INSERT OVERWRITE` in batch job (flink streaming job does not support `INSERT OVERWRITE`). Overwrites are atomic operations for Iceberg tables.

Partitions that have rows produced by the SELECT query will be replaced, for example:

```sql
INSERT OVERWRITE `hive_catalog`.`default`.`sample` VALUES (1, 'a');
```

Iceberg also support overwriting given partitions by the `select` values:

```sql
INSERT OVERWRITE `hive_catalog`.`default`.`sample` PARTITION(data='a') SELECT 6;
```

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

### Branch Writes
Writing to branches in Iceberg tables is also supported via the `toBranch` API in `FlinkSink`
For more information on branches please refer to [branches](branching.md).
```java
FlinkSink.forRowData(input)
    .tableLoader(tableLoader)
    .toBranch("audit-branch")
    .append();
```

## Reading

Submit a Flink __batch__ job using the following sentences:

```sql
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
SELECT * FROM `hive_catalog`.`default`.`sample`;
```

Iceberg supports processing incremental data in flink __streaming__ jobs which starts from a historical snapshot-id:

```sql
-- Submit the flink job in streaming mode for current session.
SET execution.runtime-mode = streaming;

-- Enable this switch because streaming read SQL will provide few job options in flink SQL hint options.
SET table.dynamic-table-options.enabled=true;

-- Read all the records from the iceberg current snapshot, and then read incremental data starting from that snapshot.
SELECT * FROM `hive_catalog`.`default`.`sample` /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ ;

-- Read all incremental data starting from the snapshot-id '3821550127947089987' (records from this snapshot will be excluded).
SELECT * FROM `hive_catalog`.`default`.`sample` /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s', 'start-snapshot-id'='3821550127947089987')*/ ;
```

SQL is also the recommended way to inspect tables. To view all of the snapshots in a table, use the snapshots metadata table:

```sql
SELECT * FROM `hive_catalog`.`default`.`sample`.`snapshots`
```

Iceberg support streaming or batch read in Java API:

```
DataStream<RowData> batch = FlinkSource.forRowData()
     .env(env)
     .tableLoader(tableLoader)
     .streaming(false)
     .build();
```




## Type conversion

Iceberg's integration for Flink automatically converts between Flink and Iceberg types. When writing to a table with types that are not supported by Flink, like UUID, Iceberg will accept and convert values from the Flink type.

### Flink to Iceberg

Flink types are converted to Iceberg types according to the following table:

| Flink               | Iceberg                    | Notes         |
| ------------------- | -------------------------- | ------------- |
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
| -------------------------- | --------------------- |
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

## Future improvements

There are some features that are do not yet supported in the current Flink Iceberg integration work:

* Don't support creating iceberg table with hidden partitioning. [Discussion](http://mail-archives.apache.org/mod_mbox/flink-dev/202008.mbox/%3cCABi+2jQCo3MsOa4+ywaxV5J-Z8TGKNZDX-pQLYB-dG+dVUMiMw@mail.gmail.com%3e) in flink mail list.
* Don't support creating iceberg table with computed column.
* Don't support creating iceberg table with watermark.
* Don't support adding columns, removing columns, renaming columns, changing columns. [FLINK-19062](https://issues.apache.org/jira/browse/FLINK-19062) is tracking this.
