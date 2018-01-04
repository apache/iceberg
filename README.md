## Iceberg

Iceberg is a new table format for storing large, slow-moving tabular data. It is designed to improve on the de-facto standard table layout built into Hive, Presto, and Spark.


## Status

Iceberg is under active development at Netflix.

The core Java library that tracks table snapshots and metadata is complete, but still evolving. Current work is focused on integrating Iceberg into Spark and Presto.

The [Iceberg format specification][iceberg-spec] is being actively updated and is open for comment. Until the specification is complete and released, it carries no compatibility guarantees. The spec is currently evolving as the Java reference implementation changes.

[iceberg-spec]: https://docs.google.com/document/d/1Q-zL5lSCle6NEEdyfiYsXYzX_Q8Qf0ctMyGBKslOswA/edit?usp=sharing 


## Collaboration

We welcome collaboration on both the Iceberg library and specification. The draft spec is open for comments.

For other discussion, please open issues on the [Iceberg github page][iceberg-github].

[iceberg-github]: https://github.com/Netflix/iceberg


### Building

Iceberg is built using Gradle 4.4.

Iceberg is organized as modules:

* `iceberg-common` contains utility classes used in other modules
* `iceberg-api` contains the public Iceberg API
* `iceberg-avro` contains helpers for working with Avro files with Iceberg schemas
* `iceberg-core` contains implementations of the Iceberg API, **this is what most applications should depend on**
* `iceberg-parquet` is an optional module for working with tables backed by Parquet files
* `iceberg-spark` is an implementation of Spark's Datasource V2 API for Iceberg

Current [Java API javadocs][iceberg-javadocs] are available.

The current implementation requires the following **unreleased dependencies**:

* `iceberg-spark` requires Spark 2.3.0-SNAPSHOT for the [Datasource V2 API][spark-15689]
* `iceberg-parquet` requires Parquet 1.9.1-SNAPSHOT built from master ([9191fbd][parquet-9191fbd] or later)

[spark-15689]: https://issues.apache.org/jira/browse/SPARK-15689
[parquet-9191fbd]: https://github.com/apache/parquet-mr/commit/9191fbd
[parquet-mr-428]: https://github.com/apache/parquet-mr/pull/428
[iceberg-javadocs]: https://netflix.github.io/iceberg/current/javadoc/


## About Iceberg

### Overview

Iceberg tracks individual data files in a table instead of directories. This allows writers to create data files in-place and only adds files to the table in an explicit commit.

Table state is maintained in metadata files. All changes to table state create a new metadata file and replace the old metadata with an atomic operation. The table metadata file tracks the table schema, partitioning config, other properties, and snapshots of the table contents. Each snapshot is a complete set of data files in the table at some point in time. Snapshots are listed in the metadata file, but the files in a snapshot are stored in separate manifest files.

The atomic transitions from one table metadata file to the next provide snapshot isolation. Readers use the snapshot that was current when they load the table metadata and are not affected by changes until they refresh and pick up a new metadata location.

Data files in snapshots are stored in one or more manifest files that contain a row for each data file in the table, its partition data, and its metrics. A snapshot is the union of all files in its manifests. Manifest files can be shared between snapshots to avoid rewriting metadata that is slow-changing.


### Design benefits

This design addresses specific problems with the hive layout: file listing is no longer used to plan jobs and files are created in place without renaming.

This also provides improved guarantees and performance:

* **Snapshot isolation**: Readers always use a consistent snapshot of the table, without needing to hold a lock. All table updates are atomic.
* **O(1) RPCs to plan**: Instead of listing O(n) directories in a table to plan a job, reading a snapshot requires O(1) RPC calls.
* **Distributed planning**: File pruning and predicate push-down is distributed to jobs, removing the metastore as a bottleneck.
* **Version history and rollback**: Table snapshots are kept as history and tables can roll back if a job produces bad data.
* **Finer granularity partitioning**: Distributed planning and O(1) RPC calls remove the current barriers to finer-grained partitioning.
* **Enables safe file-level operations**. By supporting atomic changes, Iceberg enables new use cases, like safely compacting small files and safely appending late data to tables.


### Why a new table format?

There are several problems with the current format:

* **There is no specification.** Implementations don’t handle all cases consistently. For example, bucketing in Hive and Spark use different hash functions and are not compatible. Hive uses a locking scheme to make cross-partition changes safe, but no other implementations use it.
* **The metastore only tracks partitions.** Files within partitions are discovered by listing partition paths. Listing partitions to plan a read is expensive, especially when using S3. This also makes atomic changes to a table’s contents impossible. Netflix has developed custom Metastore extensions to swap partition locations, but these are slow because it is expensive to make thousands of updates in a database transaction.
* **Operations depend on file rename.** Most output committers depend on rename operations to implement guarantees and reduce the amount of time tables only have partial data from a write. But rename is not a metadata-only operation in S3 and will copy data. The [new S3 committers][HADOOP-13786] that use multipart upload make this better, but can’t entirely solve the problem and put a lot of load on the S3 index during job commit.

Table data is tracked in both a central metastore, for partitions, and the file system, for files. The central metastore can be a scale bottleneck and the file system doesn't---and shouldn't---provide transactions to isolate concurrent reads and writes. The current table layout cannot be patched to fix its major problems.

[HADOOP-13786]: https://issues.apache.org/jira/browse/HADOOP-13786


### Other design goals

In addition to changes in how table contents are tracked, Iceberg's design improves a few other areas:

* **Schema evolution**: Columns are tracked by ID to support add/drop/rename.
* **Reliable types**: Iceberg uses a core set of types, tested to work consistently across all of the supported data formats.
* **Metrics**: The format includes cost-based optimization metrics stored with data files for better job planning.
* **Invisible partitioning**: Partitioning is built into Iceberg as table configuration; it can plan efficient queries without extra partition predicates.
* **Unmodified partition data**: The Hive layout stores partition data escaped in strings. Iceberg stores partition data without modification.
* **Portable spec**: Tables are not tied to Java. Iceberg has a clear specification for other implementations.


