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

Iceberg supports reading and writing Iceberg tables through [Hive](https://hive.apache.org) by using a [StorageHandler](https://cwiki.apache.org/confluence/display/Hive/StorageHandlers).
Here is the current compatibility matrix for Iceberg Hive support: 

| Feature                  | Hive 2.x               | Hive 3.1.2             |
| ------------------------ | ---------------------- | ---------------------- |
| CREATE EXTERNAL TABLE    | ✔️                     | ✔️                     |
| CREATE TABLE             | ✔️                     | ✔️                     |
| DROP TABLE               | ✔️                     | ✔️                     |
| SELECT                   | ✔️ (MapReduce and Tez) | ✔️ (MapReduce and Tez) |
| INSERT INTO              | ✔️ (MapReduce only)️    | ✔️ (MapReduce only)    |

## Enabling Iceberg support in Hive

### Loading runtime jar

To enable Iceberg support in Hive, the `HiveIcebergStorageHandler` and supporting classes need to be made available on Hive's classpath. 
These are provided by the `iceberg-hive-runtime` jar file. 
For example, if using the Hive shell, this can be achieved by issuing a statement like so:

```
add jar /path/to/iceberg-hive-runtime.jar;
```

There are many others ways to achieve this including adding the jar file to Hive's auxiliary classpath so it is available by default.
Please refer to Hive's documentation for more information.

### Enabling support

#### Hadoop configuration

The value `iceberg.engine.hive.enabled` needs to be set to `true` in the Hadoop configuration in the environment.
For example, it can be added to the Hive configuration file on the classpath of the application creating or modifying (altering, inserting etc.) the table by modifying the relevant `hive-site.xml`.
You can also do it programmatically like so:

```java
Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
hadoopConfiguration.set(ConfigProperties.ENGINE_HIVE_ENABLED, "true"); // iceberg.engine.hive.enabled=true
HiveCatalog catalog = new HiveCatalog(hadoopConfiguration);
...
catalog.createTable(tableId, schema, spec);
```

!!! Warning
    When using Tez, you also have to disable vectorization for now (`hive.vectorized.execution.enabled=false`)

#### Table property configuration

Alternatively, the property `engine.hive.enabled` can be set to `true` and added to the table properties when creating the Iceberg table. 
Here is an example of doing it programmatically:

```java
Catalog catalog = ...;
Map<String, String> tableProperties = Maps.newHashMap();
tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
catalog.createTable(tableId, schema, spec, tableProperties);
```

The table level configuration overwrites the global Hadoop configuration.

## Iceberg and Hive catalog compatibility

### Global Hive catalog

From the Hive engine's perspective, there is only 1 global data catalog, which is the Hive metastore defined in the Hadoop configuration in the runtime environment.
On contrast, Iceberg supports multiple different data catalog types such as Hive, Hadoop, AWS Glue, and also allow any custom catalog implementations.
Users might want to read tables in anther catalog through the Hive engine, or perform cross-catalog operations like join.

Iceberg handles this issue in the following way:

1. All tables created by Iceberg's `HiveCatalog` with Hive engine feature enabled are automatically visible by the Hive engine.
2. For Iceberg tables created in other catalogs, the catalog information is registered through Hadoop configuration.
A Hive external table overlay needs to be created in the Hive metastore, 
and the actual catalog name is recorded as a part of the overlay table properties. 
See [CREATE EXTERNAL TABLE](#create-external-table) section for more details.

### Custom Iceberg catalogs

To globally register different catalogs, set the following Hadoop configurations:

| Config Key                                    | Description                                            |
| --------------------------------------------- | ------------------------------------------------------ |
| iceberg.catalog.<catalog_name\>.type          | type of catalog: `hive`,`hadoop` or `custom`             |
| iceberg.catalog.<catalog_name\>.catalog-impl  | catalog implementation, must not be null if type is `custom` |
| iceberg.catalog.<catalog_name\>.<key\>        | any config key and value pairs for the catalog         |

Here are some examples using Hive CLI:

Register a `HiveCatalog` called `another_hive`:

```
SET iceberg.catalog.another_hive.type=hive;
SET iceberg.catalog.another_hive.uri=thrift://example.com:9083;
SET iceberg.catalog.another_hive.clients=10;
SET iceberg.catalog.another_hive.warehouse=hdfs://example.com:8020/warehouse;
```

Register a `HadoopCatalog` called `hadoop`:

```
SET iceberg.catalog.hadoop.type=hadoop;
SET iceberg.catalog.hadoop.warehouse=hdfs://example.com:8020/warehouse;
```

Register an AWS `GlueCatalog` called `glue`:

```
SET iceberg.catalog.glue.type=custom;
SET iceberg.catalog.glue.catalog-impl=org.apache.iceberg.aws.GlueCatalog;
SET iceberg.catalog.glue.warehouse=s3://my-bucket/my/key/prefix;
SET iceberg.catalog.glue.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager;
SET iceberg.catalog.glue.lock.table=myGlueLockTable;
```

## DDL Commands

### CREATE EXTERNAL TABLE

The `CREATE EXTERNAL TABLE` command is used to overlay a Hive table "on top of" an existing Iceberg table. 
Iceberg tables are created using either a [`Catalog`](./javadoc/master/index.html?org/apache/iceberg/catalog/Catalog.html),
or an implementation of the [`Tables`](./javadoc/master/index.html?org/apache/iceberg/Tables.html) interface,
and Hive needs to be configured accordingly to operate on these different types of table.

#### Hive catalog tables

As described before, tables created by the `HiveCatalog` with Hive engine feature enabled are directly visible by the Hive engine, so there is no need to create an overlay.

#### Custom catalog tables

For a table in a registered catalog, specify the catalog name in the statement using table property `iceberg.catalog`.
For example, the SQL below creates an overlay for a table in a `hadoop` type catalog named `hadoop_cat`:

```sql
SET iceberg.catalog.hadoop_cat.type=hadoop;
SET iceberg.catalog.hadoop_cat.warehouse=hdfs://example.com:8020/hadoop_cat;

CREATE EXTERNAL TABLE database_a.table_a
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='hadoop_cat');
```

When `iceberg.catalog` is missing from both table properties and the global Hadoop configuration, `HiveCatalog` will be used as default.

#### Path-based Hadoop tables

Iceberg tables created using `HadoopTables` are stored entirely in a directory in a filesystem like HDFS.
These tables are considered to have no catalog. 
To indicate that, set `iceberg.catalog` property to `location_based_table`. For example:

```sql
CREATE EXTERNAL TABLE table_a 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 'hdfs://some_bucket/some_path/table_a'
TBLPROPERTIES ('iceberg.catalog'='location_based_table');
```

### CREATE TABLE

Hive also supports directly creating a new Iceberg table through `CREATE TABLE` statement. For example:

```sql
CREATE TABLE database_a.table_a (
  id bigint, name string
) PARTITIONED BY (
  dept string
) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';
```

!!! Note
    This creates an unpartitioned HMS table, while the underlying Iceberg table is partitioned.

!!! Note
    Due to the limitation of Hive `PARTITIONED BY` syntax, currently you can only partition by columns, 
    which is translated to Iceberg identity partition transform.
    You cannot partition by other Iceberg partition transforms such as `days(timestamp)`.

The following Hive types have direct Iceberg types mapping:

- boolean
- float
- double
- integer
- long
- decimal
- string
- binary
- date
- timestamp (maps to Iceberg timestamp without timezone)
- timestamplocaltz (Hive 3 only, maps to Iceberg timestamp with timezone)
- struct
- map
- list

The following Hive types are not supported by Iceberg:

- interval_year_month
- interval_day_time
- union

The following Hive types do not have direct Iceberg types mapping, but we can perform auto-conversion:

| Hive type  | Iceberg type |
| ---------- | ------------ |
| byte       | integer      |
| short      | integer      |
| char       | string       |
| varchar    | string       |

You can enable this feature through Hadoop configuration (default not enabled):

| Config key                               | Default                     | Description                                         |
| -----------------------------------------| --------------------------- | --------------------------------------------------- |
| iceberg.mr.schema.auto.conversion        | false                       | if CREATE TABLE should perform type auto-conversion |

### DROP TABLE

Tables can be dropped using the `DROP TABLE` command:

```sql
DROP TABLE [IF EXISTS] table_name [PURGE];
```

You can configure purge behavior through global Hadoop configuration or Hive metastore table properties:

| Config key                  | Default                    | Description                                                     |
| ----------------------------| ---------------------------| --------------------------------------------------------------- |
| external.table.purge        | true                       | if all data and metadata should be purged in a table by default |

Each Iceberg table's default purge behavior can also be configured through Iceberg table properties:

| Property                    | Default                    | Description                                                       |
| ----------------------------| ---------------------------| ----------------------------------------------------------------- |
| gc.enabled                  | true                       | if all data and metadata should be purged in the table by default |

When changing `gc.enabled` on the Iceberg table via `UpdateProperties`, `external.table.purge` is also updated on HMS table accordingly.
When setting `external.table.purge` as a table prop during Hive `CREATE TABLE`, `gc.enabled` is pushed down accordingly to the Iceberg table properties.
This makes sure that the 2 properties are always consistent at table level between Hive and Iceberg.

!!! Warning
    Changing `external.table.purge` via Hive `ALTER TABLE SET TBLPROPERTIES` does not update `gc.enabled` on the Iceberg table. 
    This is a limitation on Hive 3.1.2 because the `HiveMetaHook` doesn't have all the hooks for alter tables yet.

## Querying with SQL

Here are the features highlights for Iceberg Hive read support:

1. **Predicate pushdown**: Pushdown of the Hive SQL `WHERE` clause has been implemented so that these filters are used at the Iceberg `TableScan` level as well as by the Parquet and ORC Readers.
2. **Column projection**: Columns from the Hive SQL `SELECT` clause are projected down to the Iceberg readers to reduce the number of columns read.
3. **Hive query engines**: Both the MapReduce and Tez query execution engines are supported.

### Configurations

Here are the Hadoop configurations that one can adjust for the Hive reader:

| Config key                   | Default                 | Description                                            |
| ---------------------------- | ----------------------- | ------------------------------------------------------ |
| iceberg.mr.reuse.containers  | false                   | if Avro reader should reuse containers                 |
| iceberg.mr.case.sensitive    | true                    | if the query is case-sensitive                         |

### SELECT

You should now be able to issue Hive SQL `SELECT` queries and see the results returned from the underlying Iceberg table, for example:

```sql
SELECT * from table_a;
```

## Writing with SQL

### Configurations

Here are the Hadoop configurations that one can adjust for the Hive writer:

| Config key                                        | Default                                  | Description                                            |
| ------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| iceberg.mr.commit.table.thread.pool.size          | 10                                       | the number of threads of a shared thread pool to execute parallel commits for output tables |
| iceberg.mr.commit.file.thread.pool.size           | 10                                       | the number of threads of a shared thread pool to execute parallel commits for files in each output table |

### INSERT INTO

Hive supports the standard single-table `INSERT INTO` operation:

```sql
INSERT INTO table_a VALUES ('a', 1);
INSERT INTO table_a SELECT ...;
```

Multi-table insert is also supported, but it will not be atomic and are committed one table at a time. Partial changes will be visible during the commit process and failures can leave partial changes committed. Changes within a single table will remain atomic.

Here is an example of inserting into multiple tables at once in Hive SQL:
```sql
FROM customers
    INSERT INTO target1 SELECT customer_id, first_name
    INSERT INTO target2 SELECT last_name, customer_id;
```
