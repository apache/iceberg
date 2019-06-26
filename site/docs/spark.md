# Spark

Iceberg uses Spark's DataSourceV2 API for data source and catalog implementations. Spark DSv2 is an evolving API with different levels of support in Spark versions.

| Feature support  | Spark 2.4 | Spark 3.0 (unreleased) |
|------------------|-----------|------------------------|
| SQL create table |           | ✔️                     |
| SQL alter table  |           | ✔️                     |
| SQL reads        |           | ✔️                     |
| SQL insert into  |           | ✔️                     |
| DataFrame reads  | ✔️        | ✔️                     |
| DataFrame append | ✔️        | ✔️                     |


## Spark 2.4

To use Iceberg in Spark 2.4, add the `iceberg-runtime` Jar to Spark's `jars` folder.

Spark 2.4 is limited to reading and writing existing Iceberg tables. Use the [Iceberg API](api) to create Iceberg tables.


### Reading an Iceberg table

To read an Iceberg table, use the `iceberg` format in `DataFrameReader`:

```scala
spark.read.format("iceberg").load("db.table")
```

Iceberg tables identified by HDFS path are also supported:

```scala
spark.read.format("iceberg").load("hdfs://nn:8020/path/to/table")
```


### Time travel

To select a specific table snapshot or the snapshot at some time, Iceberg supports two Spark read options:

* `snapshot-id` selects a specific table snapshot
* `as-of-timestamp` selects the current snapshot at a timestamp, in milliseconds

```scala
// time travel to October 26, 1986 at 01:21:00
spark.read
    .format("iceberg")
    .option("as-of-timestamp", "499162860000")
    .load("db.table")
```

```scala
// time travel to snapshot with ID 10963874102873L
spark.read
    .format("iceberg")
    .option("snapshot-id", 10963874102873L)
    .load("db.table")
```


### Querying with SQL

To run SQL `SELECT` statements on Iceberg tables in 2.4, register the DataFrame as a temporary table:

```scala
val df = spark.read.format("iceberg").load("db.table")
df.createOrReplaceTempView("table")

spark.sql("""select count(1) from table""").show()
```


### Appending to an Iceberg table

To append a dataframe to an Iceberg table, use the `iceberg` format with `DataFrameReader`:

```scala
spark.write
    .format("iceberg")
    .save("db.table")
```
