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

# Spark DDL

To use Iceberg in Spark, first configure [Spark catalogs](./spark-configuration.md).

Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions. Spark 2.4 does not support SQL DDL.

!!! Note
    Spark 2.4 can't create Iceberg tables with DDL, instead use Spark 3.x or the [Iceberg API](./java-api-quickstart.md).

## `CREATE TABLE`

Spark 3.0 can create tables in any Iceberg catalog with the clause `USING iceberg`:

```sql
CREATE TABLE prod.db.sample (
    id bigint COMMENT 'unique id',
    data string)
USING iceberg
```

Iceberg will convert the column type in Spark to corresponding Iceberg type. Please check the section of [type compatibility on creating table](./spark-writes.md#spark-type-to-iceberg-type) for details.

Table create commands, including CTAS and RTAS, support the full range of Spark create clauses, including:

* `PARTITION BY (partition-expressions)` to configure partitioning
* `LOCATION '(fully-qualified-uri)'` to set the table location
* `COMMENT 'table documentation'` to set a table description
* `TBLPROPERTIES ('key'='value', ...)` to set [table configuration](./configuration.md)

Create commands may also set the default format with the `USING` clause. This is only supported for `SparkCatalog` because Spark handles the `USING` clause differently for the built-in catalog.

### `PARTITIONED BY`

To create a partitioned table, use `PARTITIONED BY`:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string)
USING iceberg
PARTITIONED BY (category)
```

The `PARTITIONED BY` clause supports transform expressions to create [hidden partitions](./partitioning.md).

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category)
```

Supported transformations are:

* `years(ts)`: partition by year
* `months(ts)`: partition by month
* `days(ts)` or `date(ts)`: equivalent to dateint partitioning
* `hours(ts)` or `date_hour(ts)`: equivalent to dateint and hour partitioning
* `bucket(N, col)`: partition by hashed value mod N buckets
* `truncate(L, col)`: partition by value truncated to L
    * Strings are truncated to the given length
    * Integers and longs truncate to bins: `truncate(10, i)` produces partitions 0, 10, 20, 30, ...

## `CREATE TABLE ... AS SELECT`

Iceberg supports CTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). CTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

```sql
CREATE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

## `REPLACE TABLE ... AS SELECT`

Iceberg supports RTAS as an atomic operation when using a [`SparkCatalog`](#configuring-catalogs). RTAS is supported, but is not atomic when using [`SparkSessionCatalog`](#replacing-the-session-catalog).

Atomic table replacement creates a new snapshot with the results of the `SELECT` query, but keeps table history.

```sql
REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```
```sql
CREATE OR REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

The schema and partition spec will be replaced if changed. To avoid modifying the table's schema and partitioning, use `INSERT OVERWRITE` instead of `REPLACE TABLE`.
The new table properties in the `REPLACE TABLE` command will be merged with any existing table properties. The existing table properties will be updated if changed else they are preserved.

## `DROP TABLE`

To delete a table, run:

```sql
DROP TABLE prod.db.sample
```


## `ALTER TABLE`

Iceberg has full `ALTER TABLE` support in Spark 3, including:

* Renaming a table
* Setting or removing table properties
* Adding, deleting, and renaming columns
* Adding, deleting, and renaming nested fields
* Reordering top-level columns and nested struct fields
* Widening the type of `int`, `float`, and `decimal` fields
* Making required columns optional

In addition, [SQL extensions](./spark-configuration.md#sql-extensions) can be used to add support for partition evolution and setting a table's write order

### `ALTER TABLE ... RENAME TO`

```sql
ALTER TABLE prod.db.sample RENAME TO prod.db.new_name
```

### `ALTER TABLE ... SET TBLPROPERTIES`

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
    'read.split.target-size'='268435456'
)
```

Iceberg uses table properties to control table behavior. For a list of available properties, see [Table configuration](./configuration.md).

`UNSET` is used to remove properties:

```sql
ALTER TABLE prod.db.sample UNSET TBLPROPERTIES ('read.split.target-size')
```

### `ALTER TABLE ... ADD COLUMN`

To add a column to Iceberg, use the `ADD COLUMNS` clause with `ALTER TABLE`:

```sql
ALTER TABLE prod.db.sample
ADD COLUMNS (
    new_column string comment 'new_column docs'
  )
```

Multiple columns can be added at the same time, separated by commas.

Nested columns should be identified using the full column name:

```sql
-- create a struct column
ALTER TABLE prod.db.sample
ADD COLUMN point struct<x: double, y: double>;

-- add a field to the struct
ALTER TABLE prod.db.sample
ADD COLUMN point.z double
```

In Spark 2.4.4 and later, you can add columns in any position by adding `FIRST` or `AFTER` clauses:

```sql
ALTER TABLE prod.db.sample
ADD COLUMN new_column bigint AFTER other_column
```

```sql
ALTER TABLE prod.db.sample
ADD COLUMN nested.new_column bigint FIRST
```

### `ALTER TABLE ... RENAME COLUMN`

Iceberg allows any field to be renamed. To rename a field, use `RENAME COLUMN`:

```sql
ALTER TABLE prod.db.sample RENAME COLUMN data TO payload
ALTER TABLE prod.db.sample RENAME COLUMN location.lat TO latitude
```

Note that nested rename commands only rename the leaf field. The above command renames `location.lat` to `location.latitude`

### `ALTER TABLE ... ALTER COLUMN`

Alter column is used to widen types, make a field optional, set comments, and reorder fields.

Iceberg allows updating column types if the update is safe. Safe updates are:

* `int` to `bigint`
* `float` to `double`
* `decimal(P,S)` to `decimal(P2,S)` when P2 > P (scale cannot change)

```sql
ALTER TABLE prod.db.sample ALTER COLUMN measurement TYPE double
```

To add or remove columns from a struct, use `ADD COLUMN` or `DROP COLUMN` with a nested column name.

Column comments can also be updated using `ALTER COLUMN`:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN measurement TYPE double COMMENT 'unit is bytes per second'
ALTER TABLE prod.db.sample ALTER COLUMN measurement COMMENT 'unit is kilobytes per second'
```

Iceberg allows reordering top-level columns or columns in a struct using `FIRST` and `AFTER` clauses:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN col FIRST
```
```sql
ALTER TABLE prod.db.sample ALTER COLUMN nested.col AFTER other_col
```

Nullability can be changed using `SET NOT NULL` and `DROP NOT NULL`:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN id DROP NOT NULL
```

!!! Note
    `ALTER COLUMN` is not used to update `struct` types. Use `ADD COLUMN` and `DROP COLUMN` to add or remove struct fields.


### `ALTER TABLE ... DROP COLUMN`

To drop columns, use `ALTER TABLE ... DROP COLUMN`:

```sql
ALTER TABLE prod.db.sample DROP COLUMN id
ALTER TABLE prod.db.sample DROP COLUMN point.z
```

## `ALTER TABLE` SQL extensions

These commands are available in Spark 3.x when using Iceberg [SQL extensions](./spark-configuration.md#sql-extensions).

### `ALTER TABLE ... ADD PARTITION FIELD`

Iceberg supports adding new partition fields to a spec using `ADD PARTITION FIELD`:

```sql
ALTER TABLE prod.db.sample ADD PARTITION FIELD catalog -- identity transform
```

[Partition transforms](#partitioned-by) are also supported:

```sql
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id)
ALTER TABLE prod.db.sample ADD PARTITION FIELD truncate(data, 4)
ALTER TABLE prod.db.sample ADD PARTITION FIELD years(ts)
-- use optional AS keyword to specify a custom name for the partition field 
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id) AS shard
```

Adding a partition field is a metadata operation and does not change any of the existing table data. New data will be written with the new partitioning, but existing data will remain in the old partition layout. Old data files will have null values for the new partition fields in metadata tables.

Dynamic partition overwrite behavior will change when the table's partitioning changes because dynamic overwrite replaces partitions implicitly. To overwrite explicitly, use the new `DataFrameWriterV2` API.

!!! Note
    To migrate from daily to hourly partitioning with transforms, it is not necessary to drop the daily partition field. Keeping the field ensures existing metadata table queries continue to work.

!!! Warning
    **Dynamic partition overwrite behavior will change** when partitioning changes
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.


### `ALTER TABLE ... DROP PARTITION FIELD`

Partition fields can be removed using `DROP PARTITION FIELD`:

```sql
ALTER TABLE prod.db.sample DROP PARTITION FIELD catalog
ALTER TABLE prod.db.sample DROP PARTITION FIELD bucket(16, id)
ALTER TABLE prod.db.sample DROP PARTITION FIELD truncate(data, 4)
ALTER TABLE prod.db.sample DROP PARTITION FIELD years(ts)
ALTER TABLE prod.db.sample DROP PARTITION FIELD shard
```

Note that although the partition is removed, the column will still exist in the table schema.

Dropping a partition field is a metadata operation and does not change any of the existing table data. New data will be written with the new partitioning, but existing data will remain in the old partition layout.

!!! Warning
    **Dynamic partition overwrite behavior will change** when partitioning changes
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.

!!! Warning
    Be careful when dropping a partition field because it will change the schema of metadata tables, like `files`, and may cause metadata queries to fail or produce different results.


### `ALTER TABLE ... WRITE ORDERED BY`

Iceberg tables can be configured with a sort order that is used to automatically sort data that is written to the table in some engines. For example, `MERGE INTO` in Spark will use the table ordering.

To set the write order for a table, use `WRITE ORDERED BY`:

```sql
ALTER TABLE prod.db.sample WRITE ORDERED BY category, id
-- use optional ASC/DEC keyword to specify sort order of each field (default ASC)
ALTER TABLE prod.db.sample WRITE ORDERED BY category ASC, id DESC
-- use optional NULLS FIRST/NULLS LAST keyword to specify null order of each field (default FIRST)
ALTER TABLE prod.db.sample WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST
```

!!! Note
    Table write order does not guarantee data order for queries. It only affects how data is written to the table.

