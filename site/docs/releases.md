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

## Downloads

The latest version of Iceberg is [{{ versions.iceberg }}](https://github.com/apache/iceberg/releases/tag/apache-iceberg-{{ versions.iceberg }}).

* [{{ versions.iceberg }} source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-{{ versions.iceberg }}/apache-iceberg-{{ versions.iceberg }}.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-{{ versions.iceberg }}/apache-iceberg-{{ versions.iceberg }}.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-{{ versions.iceberg }}/apache-iceberg-{{ versions.iceberg }}.tar.gz.sha512)
* [{{ versions.iceberg }} Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/{{ versions.iceberg }}/iceberg-spark3-runtime-{{ versions.iceberg }}.jar)
* [{{ versions.iceberg }} Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/{{ versions.iceberg }}/iceberg-spark-runtime-{{ versions.iceberg }}.jar)
* [{{ versions.iceberg }} Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/{{ versions.iceberg }}/iceberg-flink-runtime-{{ versions.iceberg }}.jar)
* [{{ versions.iceberg }} Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{ versions.iceberg }}/iceberg-hive-runtime-{{ versions.iceberg }}.jar)

To use Iceberg in Spark, download the runtime JAR and add it to the jars folder of your Spark install. Use iceberg-spark3-runtime for Spark 3, and iceberg-spark-runtime for Spark 2.4.

To use Iceberg in Hive, download the iceberg-hive-runtime JAR and add it to Hive using `ADD JAR`.

### Gradle

To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:

```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:{{ versions.iceberg }}'
}
```

You may also want to include `iceberg-parquet` for Parquet file support.

### Maven

To add a dependency on Iceberg in Maven, add the following to your `pom.xml`:

```
<dependencies>
  ...
  <dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-core</artifactId>
    <version>{{ versions.iceberg }}</version>
  </dependency>
  ...
</dependencies>
```
## 0.12.0 Release Notes

Apache Iceberg 0.12.0 was released on August 15, 2021. It consists of 395 commits authored by 74 contributors over a 139 day period.

**High-level features:**

* **Core**
    * Allow Iceberg schemas to specify one or more columns as row identifers [[\#2465](https://github.com/apache/iceberg/pull/2465)]. Note that this is a prerequisite for supporting upserts in Flink.
    * Added JDBC [[\#1870](https://github.com/apache/iceberg/pull/1870)] and DynamoDB [[\#2688](https://github.com/apache/iceberg/pull/2688)] catalog implementations.
    * Added predicate pushdown for partitions and files metadata tables [[\#2358](https://github.com/apache/iceberg/pull/2358), [\#2926](https://github.com/apache/iceberg/pull/2926)].
    * Added a new, more flexible compaction action for Spark that can support different strategies such as bin packing and sorting. [[\#2501](https://github.com/apache/iceberg/pull/2501), [\#2609](https://github.com/apache/iceberg/pull/2609)].
    * Added the ability to upgrade to v2 or create a v2 table using the table property format-version=2  [[\#2887](https://github.com/apache/iceberg/pull/2887)].
    * Added support for nulls in StructLike collections [[\#2929](https://github.com/apache/iceberg/pull/2929)].
    * Added `key_metadata` field to manifest lists for encryption [[\#2675](https://github.com/apache/iceberg/pull/2675)].
* **Flink**
    * Added support for SQL primary keys [[\#2410](https://github.com/apache/iceberg/pull/2410)].
* **Hive**
    * Added the ability to set the catalog at the table level in the Hive Metastore. This makes it possible to write queries that reference tables from multiple catalogs [[\#2129](https://github.com/apache/iceberg/pull/2129)].
    * As a result of [[\#2129](https://github.com/apache/iceberg/pull/2129)], deprecated the configuration property `iceberg.mr.catalog` which was previously used to configure the Iceberg catalog in MapReduce and Hive [[\#2565](https://github.com/apache/iceberg/pull/2565)].
    * Added table-level JVM lock on commits[[\#2547](https://github.com/apache/iceberg/pull/2547)].
    * Added support for Hive's vectorized ORC reader [[\#2613](https://github.com/apache/iceberg/pull/2613)].
* **Spark**
    * Added `SET` and `DROP IDENTIFIER FIELDS` clauses to `ALTER TABLE` so people don't have to look up the DDL [[\#2560](https://github.com/apache/iceberg/pull/2560)].
    * Added support for `ALTER TABLE REPLACE PARTITION FIELD` DDL [[\#2365](https://github.com/apache/iceberg/pull/2365)].
    * Added support for micro-batch streaming reads for structured streaming in Spark3 [[\#2660](https://github.com/apache/iceberg/pull/2660)].
    * Improved the performance of importing a Hive table by not loading all partitions from Hive and instead pushing the partition filter to the Metastore [[\#2777](https://github.com/apache/iceberg/pull/2777)].
    * Added support for `UPDATE` statements in Spark [[\#2193](https://github.com/apache/iceberg/pull/2193), [\#2206](https://github.com/apache/iceberg/pull/2206)].
    * Added support for Spark 3.1 [[\#2512]()].
    * Added `RemoveReachableFiles` action [[\#2415](https://github.com/apache/iceberg/pull/2415)].
    * Added `add_files` stored procedure [[\#2210](https://github.com/apache/iceberg/pull/2210)].
    * Refactored Actions API and added a new entry point.
    * Added support for Hadoop configuration overrides [[\#2922](https://github.com/apache/iceberg/pull/2922)].
    * Added support for the `TIMESTAMP WITHOUT TIMEZONE` type in Spark [[\#2757](https://github.com/apache/iceberg/pull/2757)].
    * Added validation that files referenced by row-level deletes are not concurrently rewritten [[\#2308](https://github.com/apache/iceberg/pull/2308)].


**Important bug fixes:**

* **Core**
    * Fixed string bucketing with non-BMP characters [[\#2849](https://github.com/apache/iceberg/pull/2849)].
    * Fixed Parquet dictionary filtering with fixed-length byte arrays and decimals [[\#2551](https://github.com/apache/iceberg/pull/2551)].
    * Fixed a problem with the configuration of HiveCatalog [[\#2550](https://github.com/apache/iceberg/pull/2550)].
    * Fixed partition field IDs in table replacement [[\#2906](https://github.com/apache/iceberg/pull/2906)].
* **Hive**
    * Enabled dropping HMS tables even if the metadata on disk gets corrupted [[\#2583](https://github.com/apache/iceberg/pull/2583)].
* **Parquet**
    * Fixed Parquet row group filters when types are promoted from `int` to `long` or from `float` to `double` [[\#2232](https://github.com/apache/iceberg/pull/2232)]
* **Spark**
    * Fixed `MERGE INTO` in Spark when used with `SinglePartition` partitioning [[\#2584](https://github.com/apache/iceberg/pull/2584)].
    * Fixed nested struct pruning in Spark [[\#2877](https://github.com/apache/iceberg/pull/2877)].
    * Fixed NaN handling for float and double metrics [[\#2464](https://github.com/apache/iceberg/pull/2464)].
    * Fixed Kryo serialization for data and delete files [[\#2343](https://github.com/apache/iceberg/pull/2343)].

**Other notable changes:**

* The Iceberg Community [voted to approve](https://mail-archives.apache.org/mod_mbox/iceberg-dev/202107.mbox/%3cCAMwmD1-k1gnShK=wQ0PD88it6cg9mY7Y1hKHjDZ7L-jcDzpyZA@mail.gmail.com%3e) version 2 of the Apache Iceberg Format Specification. The differences between version 1 and 2 of the specification are documented [here](https://iceberg.apache.org/spec/#version-2).
* Bugfixes and stability improvements for NessieCatalog.
* Improvements and fixes for Iceberg's Python library.
* Added a vectorized reader for Apache Arrow [[\#2286](https://github.com/apache/iceberg/pull/2286)].
* The following Iceberg dependencies were upgraded:
    * Hive 2.3.8 [[\#2110](https://github.com/apache/iceberg/pull/2110)].
    * Avro 1.10.1 [[\#1648](https://github.com/apache/iceberg/pull/1648)].
    * Parquet 1.12.0 [[\#2441](https://github.com/apache/iceberg/pull/2441)].


## Past releases

### 0.11.1

* Git tag: [0.11.1](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.11.1)
* [0.11.1 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.11.1/apache-iceberg-0.11.1.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.11.1/apache-iceberg-0.11.1.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.11.1/apache-iceberg-0.11.1.tar.gz.sha512)
* [0.11.1 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.11.1/iceberg-spark3-runtime-0.11.1.jar)
* [0.11.1 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.11.1/iceberg-spark-runtime-0.11.1.jar)
* [0.11.1 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.11.1/iceberg-flink-runtime-0.11.1.jar)
* [0.11.1 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.11.1/iceberg-hive-runtime-0.11.1.jar)

Important bug fixes:

* [\#2367](https://github.com/apache/iceberg/pull/2367) prohibits deleting data files when tables are dropped if GC is disabled.
* [\#2196](https://github.com/apache/iceberg/pull/2196) fixes data loss after compaction when large files are split into multiple parts and only some parts are combined with other files.
* [\#2232](https://github.com/apache/iceberg/pull/2232) fixes row group filters with promoted types in Parquet.
* [\#2267](https://github.com/apache/iceberg/pull/2267) avoids listing non-Iceberg tables in Glue.
* [\#2254](https://github.com/apache/iceberg/pull/2254) fixes predicate pushdown for Date in Hive.
* [\#2126](https://github.com/apache/iceberg/pull/2126) fixes writing of Date, Decimal, Time, UUID types in Hive.
* [\#2241](https://github.com/apache/iceberg/pull/2241) fixes vectorized ORC reads with metadata columns in Spark.
* [\#2154](https://github.com/apache/iceberg/pull/2154) refreshes the relation cache in DELETE and MERGE operations in Spark.

### 0.11.0

* Git tag: [0.11.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.11.0)
* [0.11.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz.sha512)
* [0.11.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.11.0/iceberg-spark3-runtime-0.11.0.jar)
* [0.11.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.11.0/iceberg-spark-runtime-0.11.0.jar)
* [0.11.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.11.0/iceberg-flink-runtime-0.11.0.jar)
* [0.11.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.11.0/iceberg-hive-runtime-0.11.0.jar)

High-level features:

* **Core API** now supports partition spec and sort order evolution
* **Spark 3** now supports the following SQL extensions:
    * MERGE INTO (experimental)
    * DELETE FROM (experimental)
    * ALTER TABLE ... ADD/DROP PARTITION
    * ALTER TABLE ... WRITE ORDERED BY
    * Invoke stored procedures using CALL
* **Flink** now supports streaming reads, CDC writes (experimental), and filter pushdown
* **AWS module** is added to support better integration with AWS, with [AWS Glue catalog](https://aws.amazon.com/glue/) support and dedicated S3 FileIO implementation
* **Nessie module** is added to support integration with [project Nessie](https://projectnessie.org/)

Important bug fixes:

* [\#1981](https://github.com/apache/iceberg/pull/1981) fixes bug that date and timestamp transforms were producing incorrect values for dates and times before 1970. Before the fix, negative values were incorrectly transformed by date and timestamp transforms to 1 larger than the correct value. For example, `day(1969-12-31 10:00:00)` produced 0 instead of -1. The fix is backwards compatible, which means predicate projection can still work with the incorrectly transformed partitions written using older versions.
* [\#2091](https://github.com/apache/iceberg/pull/2091) fixes `ClassCastException` for type promotion `int` to `long` and `float` to `double` during Parquet vectorized read. Now Arrow vector is created by looking at Parquet file schema instead of Iceberg schema for `int` and `float` fields.
* [\#1998](https://github.com/apache/iceberg/pull/1998) fixes bug in `HiveTableOperation` that `unlock` is not called if new metadata cannot be deleted. Now it is guaranteed that `unlock` is always called for Hive catalog users.
* [\#1979](https://github.com/apache/iceberg/pull/1979) fixes table listing failure in Hadoop catalog when user does not have permission to some tables. Now the tables with no permission are ignored in listing.
* [\#1798](https://github.com/apache/iceberg/pull/1798) fixes scan task failure when encountering duplicate entries of data files. Spark and Flink readers can now ignore duplicated entries in data files for each scan task.
* [\#1785](https://github.com/apache/iceberg/pull/1785) fixes invalidation of metadata tables in `CachingCatalog`. When a table is dropped, all the metadata tables associated with it are also invalidated in the cache.
* [\#1960](https://github.com/apache/iceberg/pull/1960) fixes bug that ORC writer does not read metrics config and always use the default. Now customized metrics config is respected.

Other notable changes:

* NaN counts are now supported in metadata
* Shared catalog properties are added in core library to standardize catalog level configurations
* Spark and Flink now support dynamically loading customized `Catalog` and `FileIO` implementations
* Spark 2 now supports loading tables from other catalogs, like Spark 3
* Spark 3 now supports catalog names in DataFrameReader when using Iceberg as a format
* Flink now uses the number of Iceberg read splits as its job parallelism to improve performance and save resource.
* Hive (experimental) now supports INSERT INTO, case insensitive query, projection pushdown, create DDL with schema and auto type conversion
* ORC now supports reading tinyint, smallint, char, varchar types
* Avro to Iceberg schema conversion now preserves field docs



### 0.10.0

* Git tag: [0.10.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.10.0)
* [0.10.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.sha512)
* [0.10.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.10.0/iceberg-spark3-runtime-0.10.0.jar)
* [0.10.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.10.0/iceberg-spark-runtime-0.10.0.jar)
* [0.10.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.10.0/iceberg-flink-runtime-0.10.0.jar)
* [0.10.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.10.0/iceberg-hive-runtime-0.10.0.jar)

High-level features:

* **Format v2 support** for building row-level operations (`MERGE INTO`) in processing engines
    * Note: format v2 is not yet finalized and does not have a forward-compatibility guarantee
* **Flink integration** for writing to Iceberg tables and reading from Iceberg tables (reading supports batch mode only)
* **Hive integration** for reading from Iceberg tables, with filter pushdown (experimental; configuration may change)

Important bug fixes:

* [\#1706](https://github.com/apache/iceberg/pull/1706) fixes non-vectorized ORC reads in Spark that incorrectly skipped rows
* [\#1536](https://github.com/apache/iceberg/pull/1536) fixes ORC conversion of `notIn` and `notEqual` to match null values
* [\#1722](https://github.com/apache/iceberg/pull/1722) fixes `Expressions.notNull` returning an `isNull` predicate; API only, method was not used by processing engines
* [\#1736](https://github.com/apache/iceberg/pull/1736) fixes `IllegalArgumentException` in vectorized Spark reads with negative decimal values
* [\#1666](https://github.com/apache/iceberg/pull/1666) fixes file lengths returned by the ORC writer, using compressed size rather than uncompressed size
* [\#1674](https://github.com/apache/iceberg/pull/1674) removes catalog expiration in HiveCatalogs
* [\#1545](https://github.com/apache/iceberg/pull/1545) automatically refreshes tables in Spark when not caching table instances

Other notable changes:

* The `iceberg-hive` module has been renamed to `iceberg-hive-metastore` to avoid confusion
* Spark 3 is based on 3.0.1 that includes the fix for [SPARK-32168](https://issues.apache.org/jira/browse/SPARK-32168)
* Hadoop tables will recover from version hint corruption
* Tables can be configured with a required sort order
* Data file locations can be customized with a dynamically loaded `LocationProvider`
* ORC file imports can apply a name mapping for stats


A more exhaustive list of changes is available under the [0.10.0 release milestone](https://github.com/apache/iceberg/milestone/10?closed=1).

### 0.9.1

* Git tag: [0.9.1](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.9.1)
* [0.9.1 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.9.1/apache-iceberg-0.9.1.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.9.1/apache-iceberg-0.9.1.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.9.1/apache-iceberg-0.9.1.tar.gz.sha512)
* [0.9.1 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.9.1/iceberg-spark3-runtime-0.9.1.jar)
* [0.9.1 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.9.1/iceberg-spark-runtime-0.9.1.jar)

### 0.9.0

* Git tag: [0.9.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.9.0)
* [0.9.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.9.0/apache-iceberg-0.9.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.9.0/apache-iceberg-0.9.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.9.0/apache-iceberg-0.9.0.tar.gz.sha512)
* [0.9.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.9.0/iceberg-spark3-runtime-0.9.0.jar)
* [0.9.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.9.0/iceberg-spark-runtime-0.9.0.jar)

### 0.8.0

* Git tag: [apache-iceberg-0.8.0-incubating](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.8.0-incubating)
* [0.8.0-incubating source tar.gz](https://www.apache.org/dyn/closer.cgi/incubator/iceberg/apache-iceberg-0.8.0-incubating/apache-iceberg-0.8.0-incubating.tar.gz) -- [signature](https://downloads.apache.org/incubator/iceberg/apache-iceberg-0.8.0-incubating/apache-iceberg-0.8.0-incubating.tar.gz.asc) -- [sha512](https://downloads.apache.org/incubator/iceberg/apache-iceberg-0.8.0-incubating/apache-iceberg-0.8.0-incubating.tar.gz.sha512)
* [0.8.0-incubating Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.8.0-incubating/iceberg-spark-runtime-0.8.0-incubating.jar)


### 0.7.0

* Git tag: [apache-iceberg-0.7.0-incubating](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.7.0-incubating)
* [0.7.0-incubating source tar.gz](https://www.apache.org/dyn/closer.cgi/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz) -- [signature](https://dist.apache.org/repos/dist/release/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz.asc) -- [sha512](https://dist.apache.org/repos/dist/release/incubator/iceberg/apache-iceberg-0.7.0-incubating/apache-iceberg-0.7.0-incubating.tar.gz.sha512)
* [0.7.0-incubating Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.7.0-incubating/iceberg-spark-runtime-0.7.0-incubating.jar)

