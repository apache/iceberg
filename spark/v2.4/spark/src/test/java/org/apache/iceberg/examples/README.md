# Iceberg Java API Examples (with Spark)

## About
Welcome! :smile:

If you've stumbled across this module, hopefully you're looking for some guidance on how to get started with the [Apache Iceberg](https://iceberg.apache.org/) table format. This set of classes collects code examples of how to use the Iceberg Java API with Spark, along with some extra detail here in the README.

The examples are structured as JUnit tests that you can download and run locally if you want to mess around with Iceberg yourself. 

## Using Iceberg 
### Maven
If you'd like to try out Iceberg in your own project using Spark, you can use the `iceberg-spark-runtime` dependency:
```xml
   <dependency>
     <groupId>org.apache.iceberg</groupId>
     <artifactId>iceberg-spark-runtime</artifactId>
     <version>${iceberg.version}</version>
   </dependency>
```

You'll also need `spark-sql`:
```xml
  <dependency> 
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.4.4</version>
  </dependency>
```

### Gradle
To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:
```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:0.8.0-incubating'
}
```

## Key features investigated
The following section will break down the different areas of Iceberg explored in the examples, with links to the code and extra information that could be useful for new users. 

### Writing data to tables
There are multiple ways of creating tables with Iceberg, including using the Hive Metastore to keep track of tables ([HiveCatalog](https://iceberg.apache.org/java-api-quickstart/#using-a-hive-catalog)), or using HDFS / your local file system ([HadoopTables](https://iceberg.apache.org/java-api-quickstart/#using-hadoop-tables)) to store the tables. However, it should be noted that directory tables (such as those using `HadoopTables`)  don’t support all catalog operations, like rename and therefore use the `Tables` interface instead of the `Catalog` interface.
It should be noted that `HadoopTables` _shouldn’t_ be used with file systems that do not support atomic rename as Iceberg depends on this to synchronize concurrent commits. 
To limit complexity, these examples create tables on your local file system using the `HadoopTables` class.

To create an Iceberg `Table` you will need to use the Iceberg API to create a `Schema` and `PartitionSpec` which you use with a Spark `DataFrameWriter`.

Code examples can be found [here](ReadAndWriteTablesTest.java).

#### A quick look at file structures
It could be interesting to note that when writing partitioned data, Iceberg will layout your files in a similar manner to Hive:

``` 
├── data
│   ├── published_month=2017-09
│   │   └── 00000-1-5cbc72f6-7c1a-45e4-bb26-bc30deaca247-00002.parquet
│   ├── published_month=2018-09
│   │   └── 00000-1-5cbc72f6-7c1a-45e4-bb26-bc30deaca247-00001.parquet
│   ├── published_month=2018-11
│   │   └── 00000-1-5cbc72f6-7c1a-45e4-bb26-bc30deaca247-00000.parquet
│   └── published_month=null
│       └── 00000-1-5cbc72f6-7c1a-45e4-bb26-bc30deaca247-00003.parquet
└── metadata
    └── version-hint.text
```
**WARNING** 
It should be noted that it is not possible to just drag-and-drop data files into an Iceberg table like the one shown above and expect to see your data in the table. 
Each file is tracked individually and is managed by Iceberg, and so must be written into the table using the Iceberg API. 

### Reading data from tables
Reading Iceberg tables is fairly simple using the Spark `DataFrameReader`.

Code examples can be found [here](ReadAndWriteTablesTest.java).

### A look at the metadata
This section looks a little bit closer at the metadata produced by Iceberg tables. Consider an example where you've written some data to a table. Your files will look something like this:

``` 
├── data
│   └── ...
└── metadata
    ├── 51accd1d-39c7-4a6e-8f35-9e05f7c67864-m0.avro
    ├── snap-1335014336004891572-1-51accd1d-39c7-4a6e-8f35-9e05f7c67864.avro
    ├── v1.metadata.json
    ├── v2.metadata.json
    └── version-hint.text
```

The metadata for your table is kept in json files and each commit to a table will produce a new metadata file. For tables using a metastore for the metadata, the file used is whichever file the metastore points at. For `HadoopTables`, the file used will be the latest version available. Look [here](https://iceberg.apache.org/spec/#table-metadata) for more information on metadata.

The metadata file will contain things like the table location, the schema and the partition spec:

```json
{
  "format-version" : 1,
  "table-uuid" : "f31aa6d7-acc3-4365-b737-4ef028a60bc1",
  "location" : "/var/folders/sg/ypkyhl2s0p18qcd10ddpkn0c0000gn/T/temp5216691795982307214",
  "last-updated-ms" : 1572972868185,
  "last-column-id" : 2,
  "schema" : {
    "type" : "struct",
    "fields" : [ {
      ...
    } ]
  },
  "partition-spec" : [ {
    ...
  } ],
  "default-spec-id" : 0,
  "partition-specs" : [ {
    ...
    } ]
  } ],
  "last-partition-id" : 1000,
  "properties" : { },
  "current-snapshot-id" : -1,
  "snapshots" : [ ],
  "snapshot-log" : [ ]
}
```

When you then add your first chunk of data, you get a new version of the metadata (`v2.metadata.json`) that is the same as the first version except for the snapshot section at the bottom, which gets updated to:

```json
"current-snapshot-id" : 8405273199394950821,
  "snapshots" : [ {
    "snapshot-id" : 8405273199394950821,
    "timestamp-ms" : 1572972873293,
    "summary" : {
      "operation" : "append",
      "spark.app.id" : "local-1572972867758",
      "added-data-files" : "4",
      "added-records" : "4",
      "changed-partition-count" : "4",
      "total-records" : "4",
      "total-data-files" : "4"
    },
    "manifest-list" : "/var/folders/sg/ypkyhl2s0p18qcd10ddpkn0c0000gn/T/temp5216691795982307214/metadata/snap-8405273199394950821-1-5706fc75-31e1-404e-aa23-b493387e2e32.avro"
  } ],
  "snapshot-log" : [ {
    "timestamp-ms" : 1572972873293,
    "snapshot-id" : 8405273199394950821
  } ]
```

Here you get information on the data you have just written to the table, such as `added-records` and `added-data-files` as well as where the manifest list is located. 


### Snapshot based functionality
Iceberg uses [snapshots](https://iceberg.apache.org/terms/#snapshot) as part of its implementation, and provides a lot of useful functionality from this, such as **time travel**.

- Iceberg creates a new snapshot for all table operations that modify the table, such as appends and overwrites.
- You are able to access the whole list of snapshots generated for a table.
- Iceberg will store all snapshots generated until you delete the snapshots using the `ExpireSnapshots` API. Currently, this must be called by the user.
    - **NOTE**: A VACUUM operation with Spark is in the works for a future release to make this process easier. 
    - You can delete all snapshots earlier than a certain timestamp.
    - You can delete snaphots based on `SnapshotID` values.
- You can read data from an old snapshot using the `SnapshotID` or a timestamp value ([time travel](https://iceberg.apache.org/spark/#time-travel)).
- You can roll back your data to an earlier snapshot.

Code examples can be found [here](SnapshotFunctionalityTest.java).

### Table schema evolution
Iceberg provides support to handle schema evolution of your tables over time:

1. Add a new column
    1. The new column is always added at the end of the table (**NOTE**: This is fixed in Spark 3 which has implemented AFTER and FIRST operations).
    1. You are only able to add a column at the end of the schema, not somewhere in the middle. 
    1. Any rows using the earlier schema return a `null` value for this new column. You cannot use an alternative default value.
    1. This column automatically becomes an `optional` column, meaning adding data to this column isn't enforced for each future write. 
1. Delete a column
    1. When you delete a column, that column will no longer be available in any of your previous snapshots. So, use this with caution :sweat_smile: 
1. Update a column
    1. Certain type promotions can be made (such as `int` -> `long`). For a definitive list, see the [official documentation](https://iceberg.apache.org/spec/#schemas-and-data-types).
1. Rename a column
    1. When you rename a column, it will appear renamed in all earlier versions of snapshots. 

Code examples can be found [here](SchemaEvolutionTest.java).

### Optimistic concurrency
[Optimistic concurrency](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) is when a system assumes that multiple writers can write to the same table without interfering with each other. This is usually used in environments where there is low data contention. It means that locking of the table isn't used, allowing multiple writers to  write to the table at the same time. 

However, this means you need to occasionally deal with concurrent writer conflicts. This is when multiple writers start writing to a table at the same time, but one finishes first and commits an update. Then when the second writer tries to commit it has to throw an error because the table isn't in the same state as it was when it started writing.

Iceberg deals with this by attempting retries of the write based on the new metadata. This can happen if the files the first write changed aren't touched by the second write, then it's deemed safe to commit the second update. 

[This test](ConcurrencyTest.java) looks to experiment with how optimistic concurrency works. For more information on conflict resolution, look [here](https://iceberg.apache.org/spec/#table-metadata) and for information on write concurrency, look [here](https://iceberg.apache.org/reliability/#concurrent-write-operations).

By default, Iceberg has set the `commit.retry.num-retries` property to **4**. You can edit this default by creating an `UpdateProperties` object and assigning a new number to that property:

```java
  table.updateProperties().set("commit.retry.num-retries", "1").commit();
```

You can find more information on other table properties you can configure [here](https://iceberg.apache.org/configuration/#table-properties).
