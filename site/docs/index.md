![](img/Iceberg-logo.png)


**Apache Iceberg is a new table format for large, slow-moving tabular data.** It is designed to improve on the de-facto standard table layout built into Hive, Presto, and Spark.


### Iceberg Overview

Iceberg tracks individual data files in a table instead of directories. This allows writers to create data files in-place and only adds files to the table in an explicit commit.

Table state is maintained in metadata files. All changes to table state create a new metadata file and replace the old metadata with an atomic operation. The table metadata file tracks the table schema, partitioning config, other properties, and snapshots of the table contents.

The atomic transitions from one table metadata file to the next provide snapshot isolation. Readers use the latest table state (snapshot) that was current when they load the table metadata and are not affected by changes until they refresh and pick up a new metadata location.

A *snapshot* is a complete set of data files in the table at some point in time. Snapshots are listed in the metadata file, but the files in a snapshot are stored across a separate set of *manifest* files.

Data files in snapshots are stored in one or more manifest files that contain a row for each data file in the table, its partition data, and its metrics. A snapshot is the union of all files in its manifests. Manifest files can be shared between snapshots to avoid rewriting metadata that is slow-changing.


### Design benefits

This design addresses specific problems with the hive layout: file listing is no longer used to plan jobs and files are created in place without renaming.

This also provides improved guarantees and performance:

* **Snapshot isolation**: Readers always use a consistent snapshot of the table, without needing to hold a lock. All table updates are atomic.
* **O(1) RPCs to plan**: Instead of listing O(n) directories in a table to plan a job, reading a snapshot requires O(1) RPC calls.
* **Distributed planning**: File pruning and predicate push-down is distributed to jobs, removing the metastore as a bottleneck.
* **Version history and rollback**: Table snapshots are kept as history and tables can roll back if a job produces bad data.
* **Finer granularity partitioning**: Distributed planning and O(1) RPC calls remove the current barriers to finer-grained partitioning.
* **Safe file-level operations**. By supporting atomic changes, Iceberg enables new use cases, like safely compacting small files and safely appending late data to tables.


### Why a new table format?

The central metastore can be a scale bottleneck and the file system doesn't---and shouldn't---provide transactions to isolate concurrent reads and writes.

There are several problems with the current format:

* **There is no specification**: Implementations don’t handle all cases consistently. For example, bucketing in Hive and Spark use different hash functions and are not compatible. Hive uses a locking scheme to make cross-partition changes safe, but no other implementations use it.
* **Table data is tracked in two stores**: a central metastore, for partitions, and the file system, for files. This makes atomic changes to a table’s contents impossible. It also requires job planning to make listing calls that are O(n) with the number of partitions. These listing calls are expensive, especially when using a remote object store.
* **Operations depend on file rename**: Most output committers depend on rename operations to implement guarantees and reduce the amount of time tables only have partial data from a write. But rename is not a metadata-only operation in S3 and will copy data. The [new S3 committers][HADOOP-13786] that use multipart upload make this better, but can’t entirely solve the problem and put a lot of load on the file system during job commit.

The current format's dependence on listing and rename cannot be changed, so a new format is needed.

[HADOOP-13786]: https://issues.apache.org/jira/browse/HADOOP-13786


### Other design goals

In addition to changes in how table contents are tracked, Iceberg's design improves a few other areas:

* **Reliable types**: Iceberg provides a core set of types, tested to work consistently across all of the supported data formats. Types include date, timestamp, and decimal, as well as nested combinations of map, list, and struct.
* **Schema evolution**: Columns are tracked by ID to fully support add, drop, and rename across all columns and nested fields.
* **Hidden partitioning**: Partitioning is built into Iceberg as table configuration; it can plan efficient queries without extra partition predicates.
* **Stats-based split filtering**: Stats for the columns in each data file are used to eliminate splits before creating tasks, boosting performance for highly selective queries.
* **Metrics**: The format includes cost-based optimization metrics stored with data files for better job planning.
* **Unmodified partition data**: The Hive layout stores partition data escaped in strings. Iceberg stores partition data without modification.
* **Portable spec**: Tables are not tied to Java. Iceberg has a clear specification for other implementations.


