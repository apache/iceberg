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

Apache Iceberg supports creating flink table directly without creating the explicit flink catalog in flink SQL in [#2666](https://github.com/apache/iceberg/pull/2666). That means we can just create an iceberg table by specifying `'connector'='iceberg'` table option in flink SQL which is similar to usage in the flink official [document](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/overview/).

In flink, the SQL `CREATE TABLE test (..) WITH ('connector'='iceberg', ...)` will create an flink table in current flink catalog (use [GenericInMemoryCatalog](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/catalogs/#genericinmemorycatalog) by default),
which is just mapping to the underlying iceberg table instead of maintaining iceberg table directly in current flink catalog.

To create the table in flink SQL by using SQL syntax `CREATE TABLE test (..) WITH ('connector'='iceberg', ...)`,  flink iceberg connector provides the following table properties:

* `connector`: Use the constant `iceberg`.
* `catalog-name`: User-specified catalog name. It's required because the connector don't have any default value.
* `catalog-type`: Default to use `hive` if don't specify any value. The optional values are:
    * `hive`: The hive metastore catalog.
    * `hadoop`: The hadoop catalog.
    * `custom`: The customized catalog, see [custom catalog](./custom-catalog.md) for more details.
* `catalog-database`: The iceberg database name in the backend catalog, use the current flink database name by default.
* `catalog-table`: The iceberg table name in the backend catalog. Default to use the table name in the flink `CREATE TABLE` sentence.

## Table managed in hive catalog.

Before executing the following SQL, please make sure you've configured the flink SQL client correctly according to the quick start [document](./flink.md).

The following SQL will create an flink table in the current flink catalog, which maps to the iceberg table `default_database.iceberg_table` managed in iceberg catalog.

```sql
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='hive_prod',
    'uri'='thrift://localhost:9083',
    'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```

If you want to create a flink table mapping to a different iceberg table managed in hive catalog (such as `hive_db.hive_iceberg_table` in hive), then you can create flink table as following:

```sql
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='hive_prod',
    'catalog-database'='hive_db',
    'catalog-table'='hive_iceberg_table',
    'uri'='thrift://localhost:9083',
    'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```

!!! Note
    The underlying catalog database (`hive_db` in the above example) will be created automatically if it does not exist when writing records into the flink table.

## Table managed in hadoop catalog

The following SQL will create an flink table in current flink catalog, which maps to the iceberg table `default_database.flink_table` managed in hadoop catalog.

```sql
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='hadoop_prod',
    'catalog-type'='hadoop',
    'warehouse'='hdfs://nn:8020/path/to/warehouse'
);
```

## Table managed in custom catalog

The following SQL will create an flink table in current flink catalog, which maps to the iceberg table `default_database.flink_table` managed in custom catalog.

```sql
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='custom_prod',
    'catalog-type'='custom',
    'catalog-impl'='com.my.custom.CatalogImpl',
     -- More table properties for the customized catalog
    'my-additional-catalog-config'='my-value',
     ...
);
```

Please refer to [AWS](./aws.md#catalogs), [Nessie](./nessie.md), [JDBC](./jdbc.md) catalog for more details.

## A complete example.

Take the hive catalog as an example:

```sql
CREATE TABLE flink_table (
    id   BIGINT,
    data STRING
) WITH (
    'connector'='iceberg',
    'catalog-name'='hive_prod',
    'uri'='thrift://localhost:9083',
    'warehouse'='file:///path/to/warehouse'
);

INSERT INTO flink_table VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC');

SET execution.result-mode=tableau;
SELECT * FROM flink_table;

+----+------+
| id | data |
+----+------+
|  1 |  AAA |
|  2 |  BBB |
|  3 |  CCC |
+----+------+
3 rows in set
```

Please refer to [document](./flink.md) for queries and writes.