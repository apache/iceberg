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
Only Hive 2.x versions are currently supported. 

## Enabling Iceberg support in Hive

### Loading runtime jar

To enable Iceberg support in Hive, the `HiveIcebergStorageHandler` and supporting classes need to be made available on Hive's classpath. These are provided by the `iceberg-hive-runtime` jar file. For example, if using the Hive shell, this can be achieved by issuing a statement like so:
```sql
add jar /path/to/iceberg-hive-runtime.jar;
```
There are many others ways to achieve this including adding the jar file to Hive's auxiliary classpath so it is available by default.
Please refer to Hive's documentation for more information.

### Enabling support

#### Hadoop configuration

The value `iceberg.engine.hive.enabled` needs to be set to `true` in the Hadoop configuraiton in the environment.
For example, it can be added to the Hive configuration file on the classpath of the application creating or modifying (altering, inserting etc.) the table by modifying the relevant `hive-site.xml`.
You can also do it programmatically like so:

```java
Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
hadoopConfiguration.set(ConfigProperties.ENGINE_HIVE_ENABLED, "true"); // iceberg.engine.hive.enabled=true
HiveCatalog catalog = new HiveCatalog(hadoopConfiguration);
...
catalog.createTable(tableId, schema, spec);
```

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
| iceberg.catalog.<catalog_name\>.type           | type of catalog: `hive`,`hadoop` or `custom`           |
| iceberg.catalog.<catalog_name\>.catalog-impl   | catalog implementation, must not be null if type is `custom` |
| iceberg.catalog.<catalog_name\>.<key\>         | any config key and value pairs for the catalog         |

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
SET iceberg.catalog.glue.lock.table=myGlueLockTable
```

## DDL Commands

### CREATE EXTERNAL TABLE

The `CREATE EXTERNAL TABLE` command is used to overlay a Hive table "on top of" an existing Iceberg table. 
Iceberg tables are created using either a [`Catalog`](./javadoc/master/index.html?org/apache/iceberg/catalog/Catalog.html),
or an implementation of the [`Tables`](./javadoc/master/index.html?org/apache/iceberg/Tables.html) interface,
and Hive needs to be configured accordingly to operate on these different types of table.

#### Path-based Hadoop tables

Iceberg tables created using `HadoopTables` are stored entirely in a directory in a filesystem like HDFS.
You can use other compute engines or the Java/Python API to create such a table. 

Suppose there is a table `table_a` and the table location is `hdfs://some_path/table_a`. 
Now overlay a Hive table on top of this Iceberg table by issuing Hive DDL like so:

```sql
CREATE EXTERNAL TABLE table_a 
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 'hdfs://some_bucket/some_path/table_a';
```

#### Hadoop catalog tables

Iceberg tables created using `HadoopCatalog` are stored entirely in a directory in a filesystem like HDFS, 
similar to the tables created through `HadoopTables`.
You can use other compute engines or Java/Python API to create such as table. 

Suppose there is a table `table_b` and the table location is `hdfs://some_path/table_b`. 

Now overlay a Hive table on top of this Iceberg table by issuing Hive DDL like so:

```sql
CREATE EXTERNAL TABLE database_a.table_b
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' 
LOCATION 'hdfs://some_path/table_b'
TBLPROPERTIES (
  'iceberg.mr.catalog'='hadoop', 
  'iceberg.mr.catalog.hadoop.warehouse.location'='hdfs://some_bucket/path_to_hadoop_warehouse'
);
```

Note that the Hive database and table name *must* match the values used in the Iceberg `TableIdentifier` when the table was created. 

It is possible to omit either or both of the table properties but instead you will then need to set these when reading from the table.
Generally it is recommended to set them at table creation time, so you can query tables created by different catalogs. 

#### Hive catalog tables

As described before, tables created by the `HiveCatalog` with Hive engine feature enabled are directly visible by the Hive engine, so there is no need to create an overlay.

#### Custom catalog tables

For a registered catalog, simply specify the catalog name in the statement using table property `iceberg.catalog`.
For example, the SQL below creates an overlay for a table in the `glue` catalog.

```sql
CREATE EXTERNAL TABLE database_a.table_c
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='glue');
```

### DROP TABLE

When dropping the Hive table overlay, one can perform the following configurations:

| Config key                  | Default                    | Description                                            |
| ----------------------------| ---------------------------| ------------------------------------------------------ |
| external.table.purge        | true                       | if all data and metadata should be purged in the table  |

## Querying with SQL

Here are the features highlights for Iceberg Hive read support:

1. **Predicate pushdown**: Pushdown of the Hive SQL `WHERE` clause has been implemented so that these filters are used at the Iceberg `TableScan` level as well as by the Parquet and ORC Readers.
2. **Column projection**: Columns from the Hive SQL `SELECT` clause are projected down to the Iceberg readers to reduce the number of columns read.
3. **Hive query engines**: Both the MapReduce and Tez query execution engines are supported.

You should now be able to issue Hive SQL `SELECT` queries and see the results returned from the underlying Iceberg table:

```sql
SELECT * from table_a;
```

## Writing with SQL

### Configurations

Here is a table of Hadoop configurations that one can adjust for the Hive writer:

| Config key                                        | Default                                  | Description                                            |
| ------------------------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| iceberg.mr.output.tables                          | the current table in the job             | a list of tables delimited by `..`                     |
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
    INSERT INTO target2 SELECT last_name, customer_id
```
