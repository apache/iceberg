---
title: "DDL"
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

# Spark DDL

To use Iceberg in Spark, first configure [Spark catalogs](spark-configuration.md). Iceberg uses Apache Spark's DataSourceV2 API for data source and catalog implementations.

## `CREATE TABLE`

Spark 3 can create tables in any Iceberg catalog with the clause `USING iceberg`:

```sql
CREATE TABLE prod.db.sample (
    id bigint NOT NULL COMMENT 'unique id',
    data string)
USING iceberg;
```

Iceberg will convert the column type in Spark to corresponding Iceberg type. Please check the section of [type compatibility on creating table](spark-getting-started.md#spark-type-to-iceberg-type) for details.

Table create commands, including CTAS and RTAS, support the full range of Spark create clauses, including:

* `PARTITIONED BY (partition-expressions)` to configure partitioning
* `LOCATION '(fully-qualified-uri)'` to set the table location
* `COMMENT 'table documentation'` to set a table description
* `TBLPROPERTIES ('key'='value', ...)` to set [table configuration](configuration.md)

Create commands may also set the default format with the `USING` clause. This is only supported for `SparkCatalog` because Spark handles the `USING` clause differently for the built-in catalog.

`CREATE TABLE ... LIKE ...` syntax is not supported.

### `PARTITIONED BY`

To create a partitioned table, use `PARTITIONED BY`:

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string)
USING iceberg
PARTITIONED BY (category);
```

The `PARTITIONED BY` clause supports transform expressions to create [hidden partitions](partitioning.md).

```sql
CREATE TABLE prod.db.sample (
    id bigint,
    data string,
    category string,
    ts timestamp)
USING iceberg
PARTITIONED BY (bucket(16, id), days(ts), category);
```

Supported transformations are:

* `year(ts)`: partition by year
* `month(ts)`: partition by month
* `day(ts)` or `date(ts)`: equivalent to dateint partitioning
* `hour(ts)` or `date_hour(ts)`: equivalent to dateint and hour partitioning
* `bucket(N, col)`: partition by hashed value mod N buckets
* `truncate(L, col)`: partition by value truncated to L
    * Strings are truncated to the given length
    * Integers and longs truncate to bins: `truncate(10, i)` produces partitions 0, 10, 20, 30, ...

Note: Old syntax of `years(ts)`, `months(ts)`, `days(ts)` and `hours(ts)` are also supported for compatibility. 

## `CREATE TABLE ... AS SELECT`

Iceberg supports CTAS as an atomic operation when using a [`SparkCatalog`](spark-configuration.md#catalog-configuration). CTAS is supported, but is not atomic when using [`SparkSessionCatalog`](spark-configuration.md#replacing-the-session-catalog).

```sql
CREATE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```

The newly created table won't inherit the partition spec and table properties from the source table in SELECT, you can use PARTITIONED BY and TBLPROPERTIES in CTAS to declare partition spec and table properties for the new table.

```sql
CREATE TABLE prod.db.sample
USING iceberg
PARTITIONED BY (part)
TBLPROPERTIES ('key'='value')
AS SELECT ...
```

## `REPLACE TABLE ... AS SELECT`

Iceberg supports RTAS as an atomic operation when using a [`SparkCatalog`](spark-configuration.md#catalog-configuration). RTAS is supported, but is not atomic when using [`SparkSessionCatalog`](spark-configuration.md#replacing-the-session-catalog).

Atomic table replacement creates a new snapshot with the results of the `SELECT` query, but keeps table history.

```sql
REPLACE TABLE prod.db.sample
USING iceberg
AS SELECT ...
```
```sql
REPLACE TABLE prod.db.sample
USING iceberg
PARTITIONED BY (part)
TBLPROPERTIES ('key'='value')
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

The drop table behavior changed in 0.14.

Prior to 0.14, running `DROP TABLE` would remove the table from the catalog and delete the table contents as well.

From 0.14 onwards, `DROP TABLE` would only remove the table from the catalog.
In order to delete the table contents `DROP TABLE PURGE` should be used.

### `DROP TABLE`

To drop the table from the catalog, run:

```sql
DROP TABLE prod.db.sample;
```

### `DROP TABLE PURGE`

To drop the table from the catalog and delete the table's contents, run:

```sql
DROP TABLE prod.db.sample PURGE;
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

In addition, [SQL extensions](spark-configuration.md#sql-extensions) can be used to add support for partition evolution and setting a table's write order

### `ALTER TABLE ... RENAME TO`

```sql
ALTER TABLE prod.db.sample RENAME TO prod.db.new_name;
```

### `ALTER TABLE ... SET TBLPROPERTIES`

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
    'read.split.target-size'='268435456'
);
```

Iceberg uses table properties to control table behavior. For a list of available properties, see [Table configuration](configuration.md).

`UNSET` is used to remove properties:

```sql
ALTER TABLE prod.db.sample UNSET TBLPROPERTIES ('read.split.target-size');
```

`SET TBLPROPERTIES` can also be used to set the table comment (description):

```sql
ALTER TABLE prod.db.sample SET TBLPROPERTIES (
    'comment' = 'A table comment.'
);
```

### `ALTER TABLE ... ADD COLUMN`

To add a column to Iceberg, use the `ADD COLUMNS` clause with `ALTER TABLE`:

```sql
ALTER TABLE prod.db.sample
ADD COLUMNS (
    new_column string comment 'new_column docs'
);
```

Multiple columns can be added at the same time, separated by commas.

Nested columns should be identified using the full column name:

```sql
-- create a struct column
ALTER TABLE prod.db.sample
ADD COLUMN point struct<x: double, y: double>;

-- add a field to the struct
ALTER TABLE prod.db.sample
ADD COLUMN point.z double;
```

```sql
-- create a nested array column of struct
ALTER TABLE prod.db.sample
ADD COLUMN points array<struct<x: double, y: double>>;

-- add a field to the struct within an array. Using keyword 'element' to access the array's element column.
ALTER TABLE prod.db.sample
ADD COLUMN points.element.z double;
```

```sql
-- create a map column of struct key and struct value
ALTER TABLE prod.db.sample
ADD COLUMN points map<struct<x: int>, struct<a: int>>;

-- add a field to the value struct in a map. Using keyword 'value' to access the map's value column.
ALTER TABLE prod.db.sample
ADD COLUMN points.value.b int;
```

Note: Altering a map 'key' column by adding columns is not allowed. Only map values can be updated.

Add columns in any position by adding `FIRST` or `AFTER` clauses:

```sql
ALTER TABLE prod.db.sample
ADD COLUMN new_column bigint AFTER other_column;
```

```sql
ALTER TABLE prod.db.sample
ADD COLUMN nested.new_column bigint FIRST;
```

### `ALTER TABLE ... RENAME COLUMN`

Iceberg allows any field to be renamed. To rename a field, use `RENAME COLUMN`:

```sql
ALTER TABLE prod.db.sample RENAME COLUMN data TO payload;
ALTER TABLE prod.db.sample RENAME COLUMN location.lat TO latitude;
```

Note that nested rename commands only rename the leaf field. The above command renames `location.lat` to `location.latitude`

### `ALTER TABLE ... ALTER COLUMN`

Alter column is used to widen types, make a field optional, set comments, and reorder fields.

Iceberg allows updating column types if the update is safe. Safe updates are:

* `int` to `bigint`
* `float` to `double`
* `decimal(P,S)` to `decimal(P2,S)` when P2 > P (scale cannot change)

```sql
ALTER TABLE prod.db.sample ALTER COLUMN measurement TYPE double;
```

To add or remove columns from a struct, use `ADD COLUMN` or `DROP COLUMN` with a nested column name.

Column comments can also be updated using `ALTER COLUMN`:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN measurement TYPE double COMMENT 'unit is bytes per second';
ALTER TABLE prod.db.sample ALTER COLUMN measurement COMMENT 'unit is kilobytes per second';
```

Iceberg allows reordering top-level columns or columns in a struct using `FIRST` and `AFTER` clauses:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN col FIRST;
```
```sql
ALTER TABLE prod.db.sample ALTER COLUMN nested.col AFTER other_col;
```

Nullability for a non-nullable column can be changed using `DROP NOT NULL`:

```sql
ALTER TABLE prod.db.sample ALTER COLUMN id DROP NOT NULL;
```

!!! info
    It is not possible to change a nullable column to a non-nullable column with `SET NOT NULL` because Iceberg doesn't know whether there is existing data with null values.

!!! info
    `ALTER COLUMN` is not used to update `struct` types. Use `ADD COLUMN` and `DROP COLUMN` to add or remove struct fields.

### `ALTER TABLE ... DROP COLUMN`

To drop columns, use `ALTER TABLE ... DROP COLUMN`:

```sql
ALTER TABLE prod.db.sample DROP COLUMN id;
ALTER TABLE prod.db.sample DROP COLUMN point.z;
```

## `ALTER TABLE` SQL extensions

These commands are available in Spark 3 when using Iceberg [SQL extensions](spark-configuration.md#sql-extensions).

### `ALTER TABLE ... ADD PARTITION FIELD`

Iceberg supports adding new partition fields to a spec using `ADD PARTITION FIELD`:

```sql
ALTER TABLE prod.db.sample ADD PARTITION FIELD catalog; -- identity transform
```

[Partition transforms](#partitioned-by) are also supported:

```sql
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id);
ALTER TABLE prod.db.sample ADD PARTITION FIELD truncate(4, data);
ALTER TABLE prod.db.sample ADD PARTITION FIELD year(ts);
-- use optional AS keyword to specify a custom name for the partition field 
ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id) AS shard;
```

Adding a partition field is a metadata operation and does not change any of the existing table data. New data will be written with the new partitioning, but existing data will remain in the old partition layout. Old data files will have null values for the new partition fields in metadata tables.

Dynamic partition overwrite behavior will change when the table's partitioning changes because dynamic overwrite replaces partitions implicitly. To overwrite explicitly, use the new `DataFrameWriterV2` API.

!!! note
    To migrate from daily to hourly partitioning with transforms, it is not necessary to drop the daily partition field. Keeping the field ensures existing metadata table queries continue to work.


!!! danger
    **Dynamic partition overwrite behavior will change** when partitioning changes
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.


### `ALTER TABLE ... DROP PARTITION FIELD`

Partition fields can be removed using `DROP PARTITION FIELD`:

```sql
ALTER TABLE prod.db.sample DROP PARTITION FIELD catalog;
ALTER TABLE prod.db.sample DROP PARTITION FIELD bucket(16, id);
ALTER TABLE prod.db.sample DROP PARTITION FIELD truncate(4, data);
ALTER TABLE prod.db.sample DROP PARTITION FIELD year(ts);
ALTER TABLE prod.db.sample DROP PARTITION FIELD shard;
```

Note that although the partition is removed, the column will still exist in the table schema.

Dropping a partition field is a metadata operation and does not change any of the existing table data. New data will be written with the new partitioning, but existing data will remain in the old partition layout.

!!! danger
    **Dynamic partition overwrite behavior will change** when partitioning changes
    For example, if you partition by days and move to partitioning by hours, overwrites will overwrite hourly partitions but not days anymore.


!!! danger
    Be careful when dropping a partition field because it will change the schema of metadata tables, like `files`, and may cause metadata queries to fail or produce different results.


### `ALTER TABLE ... REPLACE PARTITION FIELD`

A partition field can be replaced by a new partition field in a single metadata update by using `REPLACE PARTITION FIELD`:

```sql
ALTER TABLE prod.db.sample REPLACE PARTITION FIELD ts_day WITH day(ts);
-- use optional AS keyword to specify a custom name for the new partition field 
ALTER TABLE prod.db.sample REPLACE PARTITION FIELD ts_day WITH day(ts) AS day_of_ts;
```

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

!!! info
    Table write order does not guarantee data order for queries. It only affects how data is written to the table.


`WRITE ORDERED BY` sets a global ordering where rows are ordered across tasks, like using `ORDER BY` in an `INSERT` command:

```sql
INSERT INTO prod.db.sample
SELECT id, data, category, ts FROM another_table
ORDER BY ts, category
```

To order within each task, not across tasks, use `LOCALLY ORDERED BY`:

```sql
ALTER TABLE prod.db.sample WRITE LOCALLY ORDERED BY category, id
```

To unset the sort order of the table, use `UNORDERED`:

```sql
ALTER TABLE prod.db.sample WRITE UNORDERED
```

### `ALTER TABLE ... WRITE DISTRIBUTED BY PARTITION`

`WRITE DISTRIBUTED BY PARTITION` will request that each partition is handled by one writer, the default implementation is hash distribution.

```sql
ALTER TABLE prod.db.sample WRITE DISTRIBUTED BY PARTITION
```

`DISTRIBUTED BY PARTITION` and `LOCALLY ORDERED BY` may be used together, to distribute by partition and locally order rows within each task.

```sql
ALTER TABLE prod.db.sample WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id
```

### `ALTER TABLE ... SET IDENTIFIER FIELDS`

Iceberg supports setting [identifier fields](https://iceberg.apache.org/spec/#identifier-field-ids) to a spec using `SET IDENTIFIER FIELDS`:
Spark table can support Flink SQL upsert operation if the table has identifier fields.

```sql
ALTER TABLE prod.db.sample SET IDENTIFIER FIELDS id
-- single column
ALTER TABLE prod.db.sample SET IDENTIFIER FIELDS id, data
-- multiple columns
```

Identifier fields must be `NOT NULL` columns when they are created or added. 
The later `ALTER` statement will overwrite the previous setting.

### `ALTER TABLE ... DROP IDENTIFIER FIELDS`

Identifier fields can be removed using `DROP IDENTIFIER FIELDS`:

```sql
ALTER TABLE prod.db.sample DROP IDENTIFIER FIELDS id
-- single column
ALTER TABLE prod.db.sample DROP IDENTIFIER FIELDS id, data
-- multiple columns
```

Note that although the identifier is removed, the column will still exist in the table schema.

### Branching and Tagging DDL

#### `ALTER TABLE ... CREATE BRANCH`

Branches can be created via the `CREATE BRANCH` statement with the following options:
* Do not fail if the branch already exists with `IF NOT EXISTS`
* Update the branch if it already exists with `CREATE OR REPLACE`
* Create at a snapshot
* Create with retention

```sql
-- CREATE audit-branch at current snapshot with default retention.
ALTER TABLE prod.db.sample CREATE BRANCH `audit-branch`

-- CREATE audit-branch at current snapshot with default retention if it doesn't exist.
ALTER TABLE prod.db.sample CREATE BRANCH IF NOT EXISTS `audit-branch`

-- CREATE audit-branch at current snapshot with default retention or REPLACE it if it already exists.
ALTER TABLE prod.db.sample CREATE OR REPLACE BRANCH `audit-branch`

-- CREATE audit-branch at snapshot 1234 with default retention.
ALTER TABLE prod.db.sample CREATE BRANCH `audit-branch`
AS OF VERSION 1234

-- CREATE audit-branch at snapshot 1234, retain audit-branch for 31 days, and retain the latest 31 days. The latest 3 snapshot snapshots, and 2 days worth of snapshots. 
ALTER TABLE prod.db.sample CREATE BRANCH `audit-branch`
AS OF VERSION 1234 RETAIN 30 DAYS 
WITH SNAPSHOT RETENTION 3 SNAPSHOTS 2 DAYS
```

#### `ALTER TABLE ... CREATE TAG`

Tags can be created via the `CREATE TAG` statement with the following options:
* Do not fail if the tag already exists with `IF NOT EXISTS`
* Update the tag if it already exists with `CREATE OR REPLACE`
* Create at a snapshot
* Create with retention

```sql
-- CREATE historical-tag at current snapshot with default retention.
ALTER TABLE prod.db.sample CREATE TAG `historical-tag`

-- CREATE historical-tag at current snapshot with default retention if it doesn't exist.
ALTER TABLE prod.db.sample CREATE TAG IF NOT EXISTS `historical-tag`

-- CREATE historical-tag at current snapshot with default retention or REPLACE it if it already exists.
ALTER TABLE prod.db.sample CREATE OR REPLACE TAG `historical-tag`

-- CREATE historical-tag at snapshot 1234 with default retention.
ALTER TABLE prod.db.sample CREATE TAG `historical-tag` AS OF VERSION 1234

-- CREATE historical-tag at snapshot 1234 and retain it for 1 year. 
ALTER TABLE prod.db.sample CREATE TAG `historical-tag` 
AS OF VERSION 1234 RETAIN 365 DAYS
```

#### `ALTER TABLE ... REPLACE BRANCH`

The snapshot which a branch references can be updated via
the `REPLACE BRANCH` sql. Retention can also be updated in this statement. 

```sql
-- REPLACE audit-branch to reference snapshot 4567 and update the retention to 60 days.
ALTER TABLE prod.db.sample REPLACE BRANCH `audit-branch`
AS OF VERSION 4567 RETAIN 60 DAYS
```

#### `ALTER TABLE ... REPLACE TAG`

The snapshot which a tag references can be updated via
the `REPLACE TAG` sql. Retention can also be updated in this statement.

```sql
-- REPLACE historical-tag to reference snapshot 4567 and update the retention to 60 days.
ALTER TABLE prod.db.sample REPLACE TAG `historical-tag`
AS OF VERSION 4567 RETAIN 60 DAYS
```

#### `ALTER TABLE ... DROP BRANCH`

Branches can be removed via the `DROP BRANCH` sql

```sql
ALTER TABLE prod.db.sample DROP BRANCH `audit-branch`
```

#### `ALTER TABLE ... DROP TAG`

Tags can be removed via the `DROP TAG` sql

```sql
ALTER TABLE prod.db.sample DROP TAG `historical-tag`
```

### Iceberg views in Spark

Iceberg views are a [common representation](../../view-spec.md) of a SQL view that aim to be interpreted across multiple query engines.
This section covers how to create and manage views in Spark using Spark 3.4 and above (earlier versions of Spark are not supported).

!!! note

    All the SQL examples in this section follow the official Spark SQL syntax:

     * [CREATE VIEW](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-view.html#create-view)
     * [ALTER VIEW](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-view.html)
     * [DROP VIEW](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-drop-view.html)
     * [SHOW VIEWS](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-views.html)
     * [SHOW TBLPROPERTIES](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-tblproperties.html)
     * [SHOW CREATE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-create-table.html)


#### Creating a view

Create a simple view without any comments or properties:
```sql
CREATE VIEW <viewName> AS SELECT * FROM <tableName>
```

Using `IF NOT EXISTS` prevents the SQL statement from failing in case the view already exists:
```sql
CREATE VIEW IF NOT EXISTS <viewName> AS SELECT * FROM <tableName>
```

Create a view with a comment, including aliased and commented columns that are different from the source table:
```sql
CREATE VIEW <viewName> (ID COMMENT 'Unique ID', ZIP COMMENT 'Zipcode')
    COMMENT 'View Comment'
    AS SELECT id, zip FROM <tableName>
```

#### Creating a view with properties

Create a view with properties using `TBLPROPERTIES`:
```sql
CREATE VIEW <viewName>
    TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2')
    AS SELECT * FROM <tableName>
```

Display view properties:
```sql
SHOW TBLPROPERTIES <viewName>
```

#### Dropping a view

Drop an existing view:
```sql
DROP VIEW <viewName>
```

Using `IF EXISTS` prevents the SQL statement from failing if the view does not exist:
```sql
DROP VIEW IF EXISTS <viewName>
```

#### Replacing a view

Update a view's schema, its properties, or the underlying SQL statement using `CREATE OR REPLACE`:
```sql
CREATE OR REPLACE <viewName> (updated_id COMMENT 'updated ID')
    TBLPROPERTIES ('key1' = 'new_val1')
    AS SELECT id FROM <tableName>
```

#### Setting and removing view properties

Set the properties of an existing view using `ALTER VIEW ... SET TBLPROPERTIES`:
```sql
ALTER VIEW <viewName> SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2')
```

Remove the properties from an existing view using `ALTER VIEW ... UNSET TBLPROPERTIES`:
```sql
ALTER VIEW <viewName> UNSET TBLPROPERTIES ('key1', 'key2')
```

#### Showing available views

List all views in the currently set namespace (via `USE <namespace>`):
```sql
SHOW VIEWS
```

List all available views in the defined catalog and/or namespace using one of the below variations:
```sql
SHOW VIEWS IN <catalog>
```
```sql
SHOW VIEWS IN <namespace>
```
```sql
SHOW VIEWS IN <catalog>.<namespace>
```

#### Showing the CREATE statement of a view

Show the CREATE statement of a view:
```sql
SHOW CREATE TABLE <viewName>
```

#### Displaying view details

Display additional view details using `DESCRIBE`:

```sql
DESCRIBE [EXTENDED] <viewName>
```
