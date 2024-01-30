---
title: "Getting Started"
weight: 200
url: getting-started
aliases:
    - "spark/getting-started"
menu:
    main:
        parent: Spark
        identifier: spark_getting_started
        weight: 0
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

# Getting Started

The latest version of Iceberg is [{{% icebergVersion %}}](../../../releases).

Spark is currently the most feature-rich compute engine for Iceberg operations.
We recommend you to get started with Spark to understand Iceberg concepts and features with examples.
You can also view documentations of using Iceberg with other compute engine under the [Multi-Engine Support](https://iceberg.apache.org/multi-engine-support) page.

## Using Iceberg in Spark 3

To use Iceberg in a Spark shell, use the `--packages` option:

```sh
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{% icebergVersion %}}
```

{{< hint info >}}
If you want to include Iceberg in your Spark installation, add the [`iceberg-spark-runtime-3.5_2.12` Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/{{% icebergVersion %}}/iceberg-spark-runtime-3.5_2.12-{{% icebergVersion %}}.jar) to Spark's `jars` folder.
{{< /hint >}}

### Adding catalogs

Iceberg comes with [catalogs](../spark-configuration#catalogs) that enable SQL commands to manage tables and load them by name. Catalogs are configured using properties under `spark.sql.catalog.(catalog_name)`.

This command creates a path-based catalog named `local` for tables under `$PWD/warehouse` and adds support for Iceberg tables to Spark's built-in catalog:

```sh
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{% icebergVersion %}}\
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=$PWD/warehouse
```

### Creating a table

To create your first Iceberg table in Spark, use the `spark-sql` shell or `spark.sql(...)` to run a [`CREATE TABLE`](../spark-ddl#create-table) command:

```sql
-- local is the path-based catalog defined above
CREATE TABLE local.db.table (id bigint, data string) USING iceberg;
```

Iceberg catalogs support the full range of SQL DDL commands, including:

* [`CREATE TABLE ... PARTITIONED BY`](../spark-ddl#create-table)
* [`CREATE TABLE ... AS SELECT`](../spark-ddl#create-table--as-select)
* [`ALTER TABLE`](../spark-ddl#alter-table)
* [`DROP TABLE`](../spark-ddl#drop-table)

### Writing

Once your table is created, insert data using [`INSERT INTO`](../spark-writes#insert-into):

```sql
INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1;
```

Iceberg also adds row-level SQL updates to Spark, [`MERGE INTO`](../spark-writes#merge-into) and [`DELETE FROM`](../spark-writes#delete-from):

```sql
MERGE INTO local.db.target t USING (SELECT * FROM updates) u ON t.id = u.id
WHEN MATCHED THEN UPDATE SET t.count = t.count + u.count
WHEN NOT MATCHED THEN INSERT *;
```

Iceberg supports writing DataFrames using the new [v2 DataFrame write API](../spark-writes#writing-with-dataframes):

```scala
spark.table("source").select("id", "data")
     .writeTo("local.db.table").append()
```

The old `write` API is supported, but _not_ recommended.

### Reading

To read with SQL, use the Iceberg table's name in a `SELECT` query:

```sql
SELECT count(1) as count, data
FROM local.db.table
GROUP BY data;
```

SQL is also the recommended way to [inspect tables](../spark-queries#inspecting-tables). To view all snapshots in a table, use the `snapshots` metadata table:
```sql
SELECT * FROM local.db.table.snapshots;
```
```
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-----+
| committed_at            | snapshot_id    | parent_id | operation | manifest_list                                      | ... |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-----+
| 2019-02-08 03:29:51.215 | 57897183625154 | null      | append    | s3://.../table/metadata/snap-57897183625154-1.avro | ... |
|                         |                |           |           |                                                    | ... |
|                         |                |           |           |                                                    | ... |
| ...                     | ...            | ...       | ...       | ...                                                | ... |
+-------------------------+----------------+-----------+-----------+----------------------------------------------------+-----+
```

[DataFrame reads](../spark-queries#querying-with-dataframes) are supported and can now reference tables by name using `spark.table`:

```scala
val df = spark.table("local.db.table")
df.count()
```

### Next steps

Next, you can learn more about Iceberg tables in Spark:

* [DDL commands](../spark-ddl): `CREATE`, `ALTER`, and `DROP`
* [Querying data](../spark-queries): `SELECT` queries and metadata tables
* [Writing data](../spark-writes): `INSERT INTO` and `MERGE INTO`
* [Maintaining tables](../spark-procedures) with stored procedures
