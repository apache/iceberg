---
title: "Flink DDL"
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

## DDL commands

###  `CREATE Catalog`

#### Hive catalog

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

#### Hadoop catalog

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

#### REST catalog

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

#### Custom catalog

Flink also supports loading a custom Iceberg `Catalog` implementation by specifying the `catalog-impl` property:

```sql
CREATE CATALOG my_catalog WITH (
  'type'='iceberg',
  'catalog-impl'='com.my.custom.CatalogImpl',
  'my-additional-catalog-config'='my-value'
);
```

#### Create through YAML config

Catalogs can be registered in `sql-client-defaults.yaml` before starting the SQL client.

```yaml
catalogs: 
  - name: my_catalog
    type: iceberg
    catalog-type: hadoop
    warehouse: hdfs://nn:8020/warehouse/path
```

#### Create through SQL Files

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
    data STRING NOT NULL
) WITH ('format-version'='2');
```

Table create commands support the commonly used [Flink create clauses](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/create/) including:

* `PARTITION BY (column1, column2, ...)` to configure partitioning, Flink does not yet support hidden partitioning.
* `COMMENT 'table document'` to set a table description.
* `WITH ('key'='value', ...)` to set [table configuration](configuration.md) which will be stored in Iceberg table properties.

Currently, it does not support computed column and watermark definition etc.

#### `PRIMARY KEY`

Primary key constraint can be declared for a column or a set of columns, which must be unique and do not contain null.
It's required for [`UPSERT` mode](flink-writes.md/#upsert).

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING NOT NULL,
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH ('format-version'='2');
```

#### `PARTITIONED BY`

To create a partition table, use `PARTITIONED BY`:

```sql
CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING NOT NULL
) 
PARTITIONED BY (data) 
WITH ('format-version'='2');
```

Iceberg supports hidden partitioning but Flink doesn't support partitioning by a function on columns. There is no way to support hidden partitions in the Flink DDL.

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
ALTER TABLE `hive_catalog`.`default`.`sample` SET ('write.format.default'='avro');
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
