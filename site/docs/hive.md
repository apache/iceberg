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

If the Iceberg storage handler is not in Hive's classpath, then Hive cannot load or update the metadata for an Iceberg table when the storage handler is set.
To avoid the appearance of broken tables in Hive, Iceberg will not add the storage handler to a table unless Hive support is enabled.
The storage handler is kept in sync (added or removed) every time Hive engine support for the table is updated, i.e. turned on or off in the table properties.
There are two ways to enable Hive support: globally in Hadoop Configuration and per-table using a table property.

#### Hadoop configuration

To enable Hive support globally for an application, set `iceberg.engine.hive.enabled=true` in its Hadoop configuration. 
For example, setting this in the `hive-site.xml` loaded by Spark will enable the storage handler for all tables created by Spark.

!!! Warning
    Starting with Apache Iceberg `0.11.0`, when using Hive with Tez you also have to disable vectorization (`hive.vectorized.execution.enabled=false`).

#### Table property configuration

Alternatively, the property `engine.hive.enabled` can be set to `true` and added to the table properties when creating the Iceberg table. 
Here is an example of doing it programmatically:

```java
Catalog catalog = ...;
Map<String, String> tableProperties = Maps.newHashMap();
tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
catalog.createTable(tableId, schema, spec, tableProperties);
```

The table level configuration overrides the global Hadoop configuration.

## Catalog Management

### Global Hive catalog

From the Hive engine's perspective, there is only one global data catalog that is defined in the Hadoop configuration in the runtime environment.
In contrast, Iceberg supports multiple different data catalog types such as Hive, Hadoop, AWS Glue, or custom catalog implementations.
Iceberg also allows loading a table directly based on its path in the file system. Those tables do not belong to any catalog.
Users might want to read these cross-catalog and path-based tables through the Hive engine for use cases like join.

To support this, a table in the Hive metastore can represent three different ways of loading an Iceberg table,
depending on the table's `iceberg.catalog` property:

1. The table will be loaded using a `HiveCatalog` that corresponds to the metastore configured in the Hive environment if no `iceberg.catalog` is set
2. The table will be loaded using a custom catalog if `iceberg.catalog` is set to a catalog name (see below)
3. The table can be loaded directly using the table's root location if `iceberg.catalog` is set to `location_based_table`

For cases 2 and 3 above, users can create an overlay of an Iceberg table in the Hive metastore,
so that different table types can work together in the same Hive environment.
See [CREATE EXTERNAL TABLE](#create-external-table) and [CREATE TABLE](#create-table) for more details.

### Custom Iceberg catalogs

To globally register different catalogs, set the following Hadoop configurations:

| Config Key                                    | Description                                            |
| --------------------------------------------- | ------------------------------------------------------ |
| iceberg.catalog.<catalog_name\>.type          | type of catalog: `hive` or `hadoop`                    |
| iceberg.catalog.<catalog_name\>.catalog-impl  | catalog implementation, must not be null if type is null |
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
    to Hive, the table appears to be unpartitioned although the underlying Iceberg table is partitioned.

!!! Note
    Due to the limitation of Hive `PARTITIONED BY` syntax, if you use Hive `CREATE TABLE`, 
    currently you can only partition by columns, which is translated to Iceberg identity partition transform.
    You cannot partition by other Iceberg partition transforms such as `days(timestamp)`.
    To create table with all partition transforms, you need to create the table with other engines like Spark or Flink.

#### Custom catalog table

You can also create a new table that is managed by a custom catalog. 
For example, the following code creates a table in a custom Hadoop catalog:

```sql
SET iceberg.catalog.hadoop_cat.type=hadoop;
SET iceberg.catalog.hadoop_cat.warehouse=hdfs://example.com:8020/hadoop_cat;

CREATE TABLE database_a.table_a (
  id bigint, name string
) PARTITIONED BY (
  dept string
) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='hadoop_cat');
```

!!! Warning
    If the table to create already exists in the custom catalog, this will create a managed overlay table.
    This means technically you can omit the `EXTERNAL` keyword when creating an overlay table.
    However, this is **not recommended** because creating managed overlay tables could pose a risk
    to the shared data files in case of accidental drop table commands from the Hive side, 
    which would unintentionally remove all the data in the table.

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


## Type compatibility

Hive and Iceberg support different set of types. Iceberg can perform type conversion automatically, but not for all combinations,
so you may want to understand the type conversion in Iceberg in prior to design the types of columns in your tables.
You can enable auto-conversion through Hadoop configuration (not enabled by default):

| Config key                               | Default                     | Description                                         |
| -----------------------------------------| --------------------------- | --------------------------------------------------- |
| iceberg.mr.schema.auto.conversion        | false                       | if Hive should perform type auto-conversion         |

### Hive type to Iceberg type

This type conversion table describes how Hive types are converted to the Iceberg types.
The conversion applies on both creating Iceberg table and writing to Iceberg table via Hive.

| Hive             | Iceberg                 | Notes |
|------------------|-------------------------|-------|
| boolean          | boolean                 |       |
| short            | integer                 | auto-conversion |
| byte             | integer                 | auto-conversion |
| integer          | integer                 |       |
| long             | long                    |       |
| float            | float                   |       |
| double           | double                  |       |
| date             | date                    |       |
| timestamp        | timestamp without timezone |    |
| timestamplocaltz | timestamp with timezone | Hive 3 only |
| interval_year_month |                      | not supported |
| interval_day_time |                        | not supported |
| char             | string                  | auto-conversion |
| varchar          | string                  | auto-conversion |
| string           | string                  |       |
| binary           | binary                  |       |
| decimal          | decimal                 |       |
| struct           | struct                  |       |
| list             | list                    |       |
| map              | map                     |       |
| union            |                         | not supported |
