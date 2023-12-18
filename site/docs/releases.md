---
title: "Releases"
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

## Downloads

The latest version of Iceberg is [{{ icebergVersion }}](https://github.com/apache/iceberg/releases/tag/apache-iceberg-{{ icebergVersion }}).

* [{{ icebergVersion }} source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-{{ icebergVersion }}/apache-iceberg-{{ icebergVersion }}.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-{{ icebergVersion }}/apache-iceberg-{{ icebergVersion }}.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-{{ icebergVersion }}/apache-iceberg-{{ icebergVersion }}.tar.gz.sha512)
* [{{ icebergVersion }} Spark 3.4\_2.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.4_2.12-{{ icebergVersion }}.jar) -- [3.4\_2.13](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.4_2.13/{{ icebergVersion }}/iceberg-spark-runtime-3.4_2.13-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Spark 3.3\_2.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.3_2.12-{{ icebergVersion }}.jar) -- [3.3\_2.13](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.13/{{ icebergVersion }}/iceberg-spark-runtime-3.3_2.13-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Spark 3.2\_2.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.2_2.12-{{ icebergVersion }}.jar) -- [3.2\_2.13](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.13/{{ icebergVersion }}/iceberg-spark-runtime-3.2_2.13-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Spark 3.1 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/{{ icebergVersion }}/iceberg-spark-runtime-3.1_2.12-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Flink 1.17 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.17/{{ icebergVersion }}/iceberg-flink-runtime-1.17-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Flink 1.16 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.16/{{ icebergVersion }}/iceberg-flink-runtime-1.16-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Flink 1.15 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.15/{{ icebergVersion }}/iceberg-flink-runtime-1.15-{{ icebergVersion }}.jar)
* [{{ icebergVersion }} Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/{{ icebergVersion }}/iceberg-hive-runtime-{{ icebergVersion }}.jar)

To use Iceberg in Spark or Flink, download the runtime JAR for your engine version and add it to the jars folder of your installation.

To use Iceberg in Hive 2 or Hive 3, download the Hive runtime JAR and add it to Hive using `ADD JAR`.

### Gradle

To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:

```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:{{ icebergVersion }}'
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
    <version>{{ icebergVersion }}</version>
  </dependency>
  ...
</dependencies>
```

## 1.3.1 release

Apache Iceberg 1.3.1 was released on July 25, 2023.
The 1.3.1 release addresses various issues identified in the 1.3.0 release.

* Core
  - Table Metadata parser now accepts null for fields: current-snapshot-id, properties, and snapshots ([\#8064](https://github.com/apache/iceberg/pull/8064))
* Hive
  - Fix HiveCatalog deleting metadata on failures in checking lock status ([\#7931](https://github.com/apache/iceberg/pull/7931))
* Spark
  - Fix RewritePositionDeleteFiles failure for certain partition types ([\#8059](https://github.com/apache/iceberg/pull/8059))
  - Fix RewriteDataFiles concurrency edge-case on commit timeouts ([\#7933](https://github.com/apache/iceberg/pull/7933))
  - Fix partition-level DELETE operations for WAP branches ([\#7900](https://github.com/apache/iceberg/pull/7900))
* Flink
  - FlinkCatalog creation no longer creates the default database ([\#8039](https://github.com/apache/iceberg/pull/8039))

## Past releases

### 1.3.0 release

Apache Iceberg 1.3.0 was released on May 30th, 2023.
The 1.3.0 release adds a variety of new features and bug fixes.

* Core
  - Expose file and data sequence numbers in ContentFile ([\#7555](https://github.com/apache/iceberg/pull/7555))
  - Improve bit density in object storage layout ([\#7128](https://github.com/apache/iceberg/pull/7128))
  - Store split offsets for delete files ([\#7011](https://github.com/apache/iceberg/pull/7011))
  - Readable metrics in entries metadata table ([\#7539](https://github.com/apache/iceberg/pull/7539))
  - Delete file stats in partitions metadata table ([\#6661](https://github.com/apache/iceberg/pull/6661))
  - Optimized vectorized reads for Parquet Decimal ([\#3249](https://github.com/apache/iceberg/pull/3249))
  - Vectorized reads for Parquet INT96 timestamps in imported data ([\#6962](https://github.com/apache/iceberg/pull/6962))
  - Support selected vector with ORC row and batch readers ([\#7197](https://github.com/apache/iceberg/pull/7197))
  - Clean up expired metastore clients ([\#7310](https://github.com/apache/iceberg/pull/7310))
  - Support for deleting old partition spec columns in V1 tables ([\#7398](https://github.com/apache/iceberg/pull/7398))
* Spark
  - Initial support for Spark 3.4
  - Removed integration for Spark 2.4
  - Support for storage-partitioned joins with mismatching keys in Spark 3.4 (MERGE commands) ([\#7424](https://github.com/apache/iceberg/pull/7424))
  - Support for TimestampNTZ in Spark 3.4 ([\#7553](https://github.com/apache/iceberg/pull/7553))
  - Ability to handle skew during writes in Spark 3.4 ([\#7520](https://github.com/apache/iceberg/pull/7520))
  - Ability to coalesce small tasks during writes in Spark 3.4 ([\#7532](https://github.com/apache/iceberg/pull/7532))
  - Distribution and ordering enhancements in Spark 3.4 ([\#7637](https://github.com/apache/iceberg/pull/7637))
  - Action for rewriting position deletes ([\#7389](https://github.com/apache/iceberg/pull/7389))
  - Procedure for rewriting position deletes ([\#7572](https://github.com/apache/iceberg/pull/7572))
  - Avoid local sort for MERGE cardinality check ([\#7558](https://github.com/apache/iceberg/pull/7558))
  - Support for rate limits in Structured Streaming ([\#4479](https://github.com/apache/iceberg/pull/4479))
  - Read and write support for UUIDs ([\#7399](https://github.com/apache/iceberg/pull/7399))
  - Concurrent compaction is enabled by default ([\#6907](https://github.com/apache/iceberg/pull/6907))
  - Support for metadata columns in changelog tables ([\#7152](https://github.com/apache/iceberg/pull/7152))
  - Add file group failure info for data compaction ([\#7361](https://github.com/apache/iceberg/pull/7361))
* Flink
  - Initial support for Flink 1.17
  - Removed integration for Flink 1.14
  - Data statistics operator to collect traffic distribution for guiding smart shuffling ([\#6382](https://github.com/apache/iceberg/pull/6382))
  - Data statistics operator sends local data statistics to coordinator and receives aggregated data statistics from coordinator for smart shuffling ([\#7269](https://github.com/apache/iceberg/pull/7269))
  - Exposed write parallelism in SQL hints ([\#7039](https://github.com/apache/iceberg/pull/7039))
  - Row-level filtering ([\#7109](https://github.com/apache/iceberg/pull/7109))
  - Use starting sequence number by default when rewriting data files ([\#7218](https://github.com/apache/iceberg/pull/7218))
  - Config for max allowed consecutive planning failures in IcebergSource before failing the job ([\#7571](https://github.com/apache/iceberg/pull/7571))
* Vendor Integrations
  - AWS: Use Apache HTTP client as default AWS HTTP client ([\#7119](https://github.com/apache/iceberg/pull/7119))
  - AWS: Prevent token refresh scheduling on every sign request ([\#7270](https://github.com/apache/iceberg/pull/7270))
  - AWS: Disable local credentials if remote signing is enabled ([\#7230](https://github.com/apache/iceberg/pull/7230))
* Dependencies
  - Bump Arrow to 12.0.0
  - Bump ORC to 1.8.3
  - Bump Parquet to 1.13.1
  - Bump Nessie to 0.59.0

### 1.2.1 release

Apache Iceberg 1.2.1 was released on April 11th, 2023.
The 1.2.1 release is a patch release to address various issues identified in the prior release.
Here is an overview:

* CORE
  - REST: fix previous locations for refs-only load [\#7284](https://github.com/apache/iceberg/pull/7284)
  - Parse snapshot-id as long in remove-statistics update [\#7235](https://github.com/apache/iceberg/pull/7235)
* Spark
  - Broadcast table instead of file IO in rewrite manifests [\#7263](https://github.com/apache/iceberg/pull/7263)
  - Revert "Spark: Add "Iceberg" prefix to SparkTable name string for SparkUI [\#7273](https://github.com/apache/iceberg/pull/7273)
* AWS
  - Make AuthSession cache static [\#7289](https://github.com/apache/iceberg/pull/7289)
  - Abort S3 input stream on close if not EOS [\#7262](https://github.com/apache/iceberg/pull/7262)
  - Disable local credentials if remote signing is enabled [\#7230](https://github.com/apache/iceberg/pull/7230)
  - Prevent token refresh scheduling on every sign request [\#7270](https://github.com/apache/iceberg/pull/7270)
  - S3 Credentials provider support in DefaultAwsClientFactory [\#7066](https://github.com/apache/iceberg/pull/7066)

### 1.2.0 release

Apache Iceberg 1.2.0 was released on March 20th, 2023. 
The 1.2.0 release adds a variety of new features and bug fixes.
Here is an overview:

* Core
  - Added AES GCM encrpytion stream spec ([\#5432](https://github.com/apache/iceberg/pull/5432))
  - Added support for Delta Lake to Iceberg table conversion ([\#6449](https://github.com/apache/iceberg/pull/6449), [\#6880](https://github.com/apache/iceberg/pull/6880))
  - Added support for `position_deletes` metadata table ([\#6365](https://github.com/apache/iceberg/pull/6365), [\#6716](https://github.com/apache/iceberg/pull/6716))
  - Added support for scan and commit metrics reporter that is pluggable through catalog ([\#6404](https://github.com/apache/iceberg/pull/6404), [\#6246](https://github.com/apache/iceberg/pull/6246), [\#6410](https://github.com/apache/iceberg/pull/6410)) 
  - Added support for branch commit for all operations ([\#4926](https://github.com/apache/iceberg/pull/4926), [\#5010](https://github.com/apache/iceberg/pull/5010))
  - Added `FileIO` support for ORC readers and writers ([\#6293](https://github.com/apache/iceberg/pull/6293))
  - Updated all actions to leverage bulk delete whenever possible ([\#6682](https://github.com/apache/iceberg/pull/6682))
  - Updated snapshot ID definition in Puffin spec to support statistics file reuse ([\#6272](https://github.com/apache/iceberg/pull/6267))
  - Added human-readable metrics information in `files` metadata table ([\#5376](https://github.com/apache/iceberg/pull/5376))
  - Fixed incorrect Parquet row group skipping when min and max values are `NaN` ([\#6517](https://github.com/apache/iceberg/pull/6517))
  - Fixed a bug that location provider could generate paths with double slash (`//`) which is not compatible in a Hadoop file system ([\#6777](https://github.com/apache/iceberg/pull/6777))
  - Fixed metadata table time travel failure for tables that performed schema evolution ([\#6980](https://github.com/apache/iceberg/pull/6980))
* Spark
  - Added time range query support for changelog table ([\#6350](https://github.com/apache/iceberg/pull/6350))
  - Added changelog view procedure for v1 table ([\#6012](https://github.com/apache/iceberg/pull/6012))
  - Added support for storage partition joins to improve read and write performance ([\#6371](https://github.com/apache/iceberg/pull/6371))
  - Updated default Arrow environment settings to improve read performance ([\#6550](https://github.com/apache/iceberg/pull/6550))
  - Added aggregate pushdown support for `min`, `max` and `count` to improve read performance ([\#6622](https://github.com/apache/iceberg/pull/6622))
  - Updated default distribution mode settings to improve write performance ([\#6828](https://github.com/apache/iceberg/pull/6828), [\#6838](https://github.com/apache/iceberg/pull/6838))
  - Updated DELETE to perform metadata-only update whenever possible to improve write performance ([\#6899](https://github.com/apache/iceberg/pull/6899))
  - Improved predicate pushdown support for write operations ([\#6636](https://github.com/apache/iceberg/pull/6633))
  - Added support for reading a branch or tag through table identifier and `VERSION AS OF` (a.k.a. `FOR SYSTEM_VERSION AS OF`) SQL syntax ([\#6717](https://github.com/apache/iceberg/pull/6717), [\#6575](https://github.com/apache/iceberg/pull/6575))
  - Added support for writing to a branch through identifier or through write-audit-publish (WAP) workflow settings ([\#6965](https://github.com/apache/iceberg/pull/6965), [\#7050](https://github.com/apache/iceberg/pull/7050))
  - Added DDL SQL extensions to create, replace and drop a branch or tag ([\#6638](https://github.com/apache/iceberg/pull/6638), [\#6637](https://github.com/apache/iceberg/pull/6637), [\#6752](https://github.com/apache/iceberg/pull/6752), [\#6807](https://github.com/apache/iceberg/pull/6807))
  - Added UDFs for `years`, `months`, `days` and `hours` transforms ([\#6207](https://github.com/apache/iceberg/pull/6207), [\#6261](https://github.com/apache/iceberg/pull/6261), [\#6300](https://github.com/apache/iceberg/pull/6300), [\#6339](https://github.com/apache/iceberg/pull/6339))
  - Added partition related stats for `add_files` procedure result ([\#6797](https://github.com/apache/iceberg/pull/6797))
  - Fixed a bug that `rewrite_manifests` procedure produced a new manifest even when there was no rewrite performed ([\#6659](https://github.com/apache/iceberg/pull/6695))
  - Fixed a bug that statistics files were not cleaned up in `expire_snapshots` procedure ([\#6090](https://github.com/apache/iceberg/pull/6090))
* Flink
  - Added support for metadata tables ([\#6222](https://github.com/apache/iceberg/pull/6222))
  - Added support for read options in Flink source ([\#5967](https://github.com/apache/iceberg/pull/5967))
  - Added support for reading and writing Avro `GenericRecord` ([\#6557](https://github.com/apache/iceberg/pull/6557), [\#6584](https://github.com/apache/iceberg/pull/6584))
  - Added support for reading a branch or tag and write to a branch ([\#6660](https://github.com/apache/iceberg/pull/6660), [\#5029](https://github.com/apache/iceberg/pull/5029))
  - Added throttling support for streaming read ([\#6299](https://github.com/apache/iceberg/pull/6299))
  - Added support for multiple sinks for the same table in the same job ([\#6528](https://github.com/apache/iceberg/pull/6528))
  - Fixed a bug that metrics config was not applied to equality and position deletes ([\#6271](https://github.com/apache/iceberg/pull/6271), [\#6313](https://github.com/apache/iceberg/pull/6313))
* Vendor Integrations
  - Added Snowflake catalog integration ([\#6428](https://github.com/apache/iceberg/pull/6428))
  - Added AWS sigV4 authentication support for REST catalog ([\#6951](https://github.com/apache/iceberg/pull/6951))
  - Added support for AWS S3 remote signing ([\#6169](https://github.com/apache/iceberg/pull/6169), [\#6835](https://github.com/apache/iceberg/pull/6835), [\#7080](https://github.com/apache/iceberg/pull/7080))
  - Updated AWS Glue catalog to skip table version archive by default ([\#6919](https://github.com/apache/iceberg/pull/6916))
  - Updated AWS Glue catalog to not require a warehouse location ([\#6586](https://github.com/apache/iceberg/pull/6586))
  - Fixed a bug that a bucket-only AWS S3 location such as `s3://my-bucket` could not be parsed ([\#6352](https://github.com/apache/iceberg/pull/6352))
  - Fixed a bug that unnecessary HTTP client dependencies had to be included to use any AWS integration ([\#6746](https://github.com/apache/iceberg/pull/6746))
  - Fixed a bug that AWS Glue catalog did not respect custom catalog ID when determining default warehouse location ([\#6223](https://github.com/apache/iceberg/pull/6223))
  - Fixes a bug that AWS DynamoDB catalog namespace listing result was incomplete ([\#6823](https://github.com/apache/iceberg/pull/6823))
* Dependencies
  - Upgraded ORC to 1.8.1 ([\#6349](https://github.com/apache/iceberg/pull/6349))
  - Upgraded Jackson to 2.14.1 ([\#6168](https://github.com/apache/iceberg/pull/6168))
  - Upgraded AWS SDK V2 to 2.20.18 ([\#7003](https://github.com/apache/iceberg/pull/7003))
  - Upgraded Nessie to 0.50.0 ([\#6875](https://github.com/apache/iceberg/pull/6875))

For more details, please visit [Github](https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.2.0).


### 1.1.0 release

Apache Iceberg 1.1.0 was released on November 28th, 2022. 
The 1.1.0 release deprecates various pre-1.0.0 methods, 
and adds a variety of new features. 
Here is an overview:

* Core
  - Puffin statistics have been [added to the Table API](https://github.com/apache/iceberg/pull/4945)
  - Support for [Table scan reporting](https://github.com/apache/iceberg/pull/5268), which enables collection of statistics of the table scans.
  - [Add file sequence number to ManifestEntry](https://github.com/apache/iceberg/pull/6002)
  - [Support register table](https://github.com/apache/iceberg/pull/5037) for all the catalogs (previously it was only for Hive)
  - [Support performing merge appends and delete files on branches](https://github.com/apache/iceberg/pull/5618)
  - [Improved Expire Snapshots FileCleanupStrategy](https://github.com/apache/iceberg/pull/5669)
  - [SnapshotProducer supports branch writes](https://github.com/apache/iceberg/pull/4926)
* Spark
  - [Support for aggregate expressions](https://github.com/apache/iceberg/pull/5961)
  - [SparkChangelogTable for querying changelogs](https://github.com/apache/iceberg/pull/5740)
  - Dropped support for Apache Spark 3.0
* Flink
  - [FLIP-27 reader is supported in SQL](https://github.com/apache/iceberg/pull/5318)
  - Added support for Flink 1.16, dropped support for Flink 1.13
* Dependencies
  - [AWS SDK: 2.17.257](https://github.com/apache/iceberg/pull/5612)
  - [Nessie: 0.44](https://github.com/apache/iceberg/pull/6008)
  - [Apache ORC: 1.8.0](https://github.com/apache/iceberg/pull/5699) (Also, supports [setting bloom filters on row groups](https://github.com/apache/iceberg/pull/5313/files))

For more details, please visit [Github](https://github.com/apache/iceberg/releases/tag/apache-iceberg-1.1.0).

### 1.0.0 release

The 1.0.0 release officially guarantees the stability of the Iceberg API.

Iceberg's API has been largely stable since very early releases and has been integrated with many processing engines, but was still released under a 0.y.z version number indicating that breaking changes may happen. From 1.0.0 forward, the project will follow semver in the public API module, iceberg-api.

This release removes deprecated APIs that are no longer part of the API. To make transitioning to the new release easier, it is based on the 0.14.1 release with only important bug fixes:

* Increase metrics limit to 100 columns ([#5933](https://github.com/apache/iceberg/pull/5933))
* Bump Spark patch versions for CVE-2022-33891 ([#5292](https://github.com/apache/iceberg/pull/5292))
* Exclude Scala from Spark runtime Jars ([#5884](https://github.com/apache/iceberg/pull/5884))

### 0.14.1 release

This release includes all bug fixes from the 0.14.x patch releases.

#### Notable bug fixes

* API
  - API: Fix ID assignment in schema merging ([#5395](https://github.com/apache/iceberg/pull/5395))
* Core
  - Core: Fix snapshot log with intermediate transaction snapshots ([#5568](https://github.com/apache/iceberg/pull/5568))
  - Core: Fix exception handling in BaseTaskWriter ([#5683](https://github.com/apache/iceberg/pull/5683))
  - Core: Support deleting tables without metadata files ([#5510](https://github.com/apache/iceberg/pull/5510))
  - Core: Add CommitStateUnknownException handling to REST ([#5694](https://github.com/apache/iceberg/pull/5694))
* Spark
  - Spark: Fix stats in rewrite metadata action ([#5691](https://github.com/apache/iceberg/pull/5691))
* File Formats
  - Parquet: Close zstd input stream early to avoid memory pressure ([#5681](https://github.com/apache/iceberg/pull/5681))
* Vendor Integrations
  - Core, AWS: Fix Kryo serialization failure for FileIO ([#5437](https://github.com/apache/iceberg/pull/5437))
  - AWS: S3OutputStream - failure to close should persist on subsequent close calls ([#5311](https://github.com/apache/iceberg/pull/5311))

### 0.14.0 release

Apache Iceberg 0.14.0 was released on 16 July 2022.

#### Highlights

* Added several [performance improvements](#performance-improvements) for scan planning and Spark queries
* Added a common REST catalog client that uses change-based commits to resolve commit conflicts on the service side
* Added support for Spark 3.3, including `AS OF` syntax for SQL time travel queries
* Added support for Scala 2.13 with Spark 3.2 or later
* Added merge-on-read support for MERGE and UPDATE queries in Spark 3.2 or later
* Added support to rewrite partitions using zorder
* Added support for Flink 1.15 and dropped support for Flink 1.12
* Added a spec and implementation for Puffin, a format for large stats and index blobs, like [Theta sketches](https://datasketches.apache.org/docs/Theta/InverseEstimate.html) or bloom filters
* Added new interfaces for consuming data incrementally (both append and changelog scans)
* Added support for bulk operations and ranged reads to FileIO interfaces
* Added more metadata tables to show delete files in the metadata tree

#### High-level features

* API
  - Added IcebergBuild to expose Iceberg version and build information
  - Added binary compatibility checking to the build ([#4638](https://github.com/apache/iceberg/pull/4638), [#4798](https://github.com/apache/iceberg/pull/4798))
  - Added a new IncrementalAppendScan interface and planner implementation ([#4580](https://github.com/apache/iceberg/pull/4580))
  - Added a new IncrementalChangelogScan interface ([#4870](https://github.com/apache/iceberg/pull/4870))
  - Refactored the ScanTask hierarchy to create new task types for changelog scans ([#5077](https://github.com/apache/iceberg/pull/5077))
  - Added expression sanitizer ([#4672](https://github.com/apache/iceberg/pull/4672))
  - Added utility to check expression equivalence ([#4947](https://github.com/apache/iceberg/pull/4947))
  - Added support for serializing FileIO instances using initialization properties ([#5178](https://github.com/apache/iceberg/pull/5178))
  - Updated Snapshot methods to accept a FileIO to read metadata files, deprecated old methods ([#4873](https://github.com/apache/iceberg/pull/4873))
  - Added optional interfaces to FileIO, for batch deletes ([#4052](https://github.com/apache/iceberg/pull/4052)), prefix operations ([#5096](https://github.com/apache/iceberg/pull/5096)), and ranged reads ([#4608](https://github.com/apache/iceberg/pull/4608))
* Core
  - Added a common client for REST-based catalog services that uses a change-based protocol ([#4320](https://github.com/apache/iceberg/pull/4320), [#4319](https://github.com/apache/iceberg/pull/4319))
  - Added Puffin, a file format for statistics and index payloads or sketches ([#4944](https://github.com/apache/iceberg/pull/4944), [#4537](https://github.com/apache/iceberg/pull/4537))
  - Added snapshot references to track tags and branches ([#4019](https://github.com/apache/iceberg/pull/4019))
  - ManageSnapshots now supports multiple operations using transactions, and added branch and tag operations ([#4128](https://github.com/apache/iceberg/pull/4128), [#4071](https://github.com/apache/iceberg/pull/4071))
  - ReplacePartitions and OverwriteFiles now support serializable isolation ([#2925](https://github.com/apache/iceberg/pull/2925), [#4052](https://github.com/apache/iceberg/pull/4052))
  - Added new metadata tables: `data_files` ([#4336](https://github.com/apache/iceberg/pull/4336)), `delete_files` ([#4243](https://github.com/apache/iceberg/pull/4243)), `all_delete_files`, and `all_files` ([#4694](https://github.com/apache/iceberg/pull/4694))
  - Added deleted files to the `files` metadata table ([#4336](https://github.com/apache/iceberg/pull/4336)) and delete file counts to the `manifests` table ([#4764](https://github.com/apache/iceberg/pull/4764))
  - Added support for predicate pushdown for the `all_data_files` metadata table ([#4382](https://github.com/apache/iceberg/pull/4382)) and the `all_manifests` table ([#4736](https://github.com/apache/iceberg/pull/4736))
  - Added support for catalogs to default table properties on creation ([#4011](https://github.com/apache/iceberg/pull/4011))
  - Updated sort order construction to ensure all partition fields are added to avoid partition closed failures ([#5131](https://github.com/apache/iceberg/pull/5131))
* Spark
  - Spark 3.3 is now supported ([#5056](https://github.com/apache/iceberg/pull/5056))
  - Added SQL time travel using `AS OF` syntax in Spark 3.3 ([#5156](https://github.com/apache/iceberg/pull/5156))
  - Scala 2.13 is now supported for Spark 3.2 and 3.3 ([#4009](https://github.com/apache/iceberg/pull/4009))
  - Added support for the `mergeSchema` option for DataFrame writes ([#4154](https://github.com/apache/iceberg/pull/4154))
  - MERGE and UPDATE queries now support the lazy / merge-on-read strategy ([#3984](https://github.com/apache/iceberg/pull/3984), [#4047](https://github.com/apache/iceberg/pull/4047))
  - Added zorder rewrite strategy to the `rewrite_data_files` stored procedure and action ([#3983](https://github.com/apache/iceberg/pull/3983), [#4902](https://github.com/apache/iceberg/pull/4902))
  - Added a `register_table` stored procedure to create tables from metadata JSON files ([#4810](https://github.com/apache/iceberg/pull/4810))
  - Added a `publish_changes` stored procedure to publish staged commits by ID ([#4715](https://github.com/apache/iceberg/pull/4715))
  - Added `CommitMetadata` helper class to set snapshot summary properties from SQL ([#4956](https://github.com/apache/iceberg/pull/4956))
  - Added support to supply a file listing to remove orphan data files procedure and action ([#4503](https://github.com/apache/iceberg/pull/4503))
  - Added FileIO metrics to the Spark UI ([#4030](https://github.com/apache/iceberg/pull/4030), [#4050](https://github.com/apache/iceberg/pull/4050))
  - DROP TABLE now supports the PURGE flag ([#3056](https://github.com/apache/iceberg/pull/3056))
  - Added support for custom isolation level for dynamic partition overwrites ([#2925](https://github.com/apache/iceberg/pull/2925)) and filter overwrites ([#4293](https://github.com/apache/iceberg/pull/4293))
  - Schema identifier fields are now shown in table properties ([#4475](https://github.com/apache/iceberg/pull/4475))
  - Abort cleanup now supports parallel execution ([#4704](https://github.com/apache/iceberg/pull/4704))
* Flink
  - Flink 1.15 is now supported ([#4553](https://github.com/apache/iceberg/pull/4553))
  - Flink 1.12 support was removed ([#4551](https://github.com/apache/iceberg/pull/4551))
  - Added a FLIP-27 source and builder to 1.14 and 1.15 ([#5109](https://github.com/apache/iceberg/pull/5109))
  - Added an option to set the monitor interval ([#4887](https://github.com/apache/iceberg/pull/4887)) and an option to limit the number of snapshots in a streaming read planning operation ([#4943](https://github.com/apache/iceberg/pull/4943))
  - Added support for write options, like `write-format` to Flink sink builder ([#3998](https://github.com/apache/iceberg/pull/3998))
  - Added support for task locality when reading from HDFS ([#3817](https://github.com/apache/iceberg/pull/3817))
  - Use Hadoop configuration files from `hadoop-conf-dir` property ([#4622](https://github.com/apache/iceberg/pull/4622))
* Vendor integrations
  - Added Dell ECS integration ([#3376](https://github.com/apache/iceberg/pull/3376), [#4221](https://github.com/apache/iceberg/pull/4221))
  - JDBC catalog now supports namespace properties ([#3275](https://github.com/apache/iceberg/pull/3275))
  - AWS Glue catalog supports native Glue locking ([#4166](https://github.com/apache/iceberg/pull/4166))
  - AWS S3FileIO supports using S3 access points ([#4334](https://github.com/apache/iceberg/pull/4334)), bulk operations ([#4052](https://github.com/apache/iceberg/pull/4052), [#5096](https://github.com/apache/iceberg/pull/5096)), ranged reads ([#4608](https://github.com/apache/iceberg/pull/4608)), and tagging at write time or in place of deletes ([#4259](https://github.com/apache/iceberg/pull/4259), [#4342](https://github.com/apache/iceberg/pull/4342))
  - AWS GlueCatalog supports passing LakeFormation credentials ([#4280](https://github.com/apache/iceberg/pull/4280)) 
  - AWS DynamoDB catalog and lock supports overriding the DynamoDB endpoint ([#4726](https://github.com/apache/iceberg/pull/4726))
  - Nessie now supports namespaces and namespace properties ([#4385](https://github.com/apache/iceberg/pull/4385), [#4610](https://github.com/apache/iceberg/pull/4610))
  - Nessie now passes most common catalog tests ([#4392](https://github.com/apache/iceberg/pull/4392))
* Parquet
  - Added support for row group skipping using Parquet bloom filters ([#4938](https://github.com/apache/iceberg/pull/4938))
  - Added table configuration options for writing Parquet bloom filters ([#5035](https://github.com/apache/iceberg/pull/5035))
* ORC
  - Support file rolling at a target file size ([#4419](https://github.com/apache/iceberg/pull/4419))
  - Support table compression settings, `write.orc.compression-codec` and `write.orc.compression-strategy` ([#4273](https://github.com/apache/iceberg/pull/4273))

#### Performance improvements

* Core
  - Fixed manifest file handling in scan planning to open manifests in the planning threadpool ([#5206](https://github.com/apache/iceberg/pull/5206))
  - Avoided an extra S3 HEAD request by passing file length when opening manifest files ([#5207](https://github.com/apache/iceberg/pull/5207))
  - Refactored Arrow vectorized readers to avoid extra dictionary copies ([#5137](https://github.com/apache/iceberg/pull/5137))
  - Improved Arrow decimal handling to improve decimal performance ([#5168](https://github.com/apache/iceberg/pull/5168), [#5198](https://github.com/apache/iceberg/pull/5198))
  - Added support for Avro files with Zstd compression ([#4083](https://github.com/apache/iceberg/pull/4083))
  - Column metrics are now disabled by default after the first 32 columns ([#3959](https://github.com/apache/iceberg/pull/3959), [#5215](https://github.com/apache/iceberg/pull/5215))
  - Updated delete filters to copy row wrappers to avoid expensive type analysis ([#5249](https://github.com/apache/iceberg/pull/5249))
  - Snapshot expiration supports parallel execution ([#4148](https://github.com/apache/iceberg/pull/4148))
  - Manifest updates can use a custom thread pool ([#4146](https://github.com/apache/iceberg/pull/4146))
* Spark
  - Parquet vectorized reads are enabled by default ([#4196](https://github.com/apache/iceberg/pull/4196))
  - Scan statistics now adjust row counts for split data files ([#4446](https://github.com/apache/iceberg/pull/4446))
  - Implemented `SupportsReportStatistics` in `ScanBuilder` to work around SPARK-38962 ([#5136](https://github.com/apache/iceberg/pull/5136))
  - Updated Spark tables to avoid expensive (and inaccurate) size estimation ([#5225](https://github.com/apache/iceberg/pull/5225))
* Flink
  - Operators will now use a worker pool per job ([#4177](https://github.com/apache/iceberg/pull/4177))
  - Fixed `ClassCastException` thrown when reading arrays from Parquet ([#4432](https://github.com/apache/iceberg/pull/4432))
* Hive
  - Added vectorized Parquet reads for Hive 3 ([#3980](https://github.com/apache/iceberg/pull/3980))
  - Improved generic reader performance using copy instead of create ([#4218](https://github.com/apache/iceberg/pull/4218))

#### Notable bug fixes

This release includes all bug fixes from the 0.13.x patch releases.

* Core
  - Fixed an exception thrown when metadata-only deletes encounter delete files that are partially matched ([#4304](https://github.com/apache/iceberg/pull/4304))
  - Fixed transaction retries for changes without validations, like schema updates, that could ignore an update ([#4464](https://github.com/apache/iceberg/pull/4464))
  - Fixed failures when reading metadata tables with evolved partition specs ([#4520](https://github.com/apache/iceberg/pull/4520), [#4560](https://github.com/apache/iceberg/pull/4560))
  - Fixed delete files dropped when a manifest is rewritten following a format version upgrade ([#4514](https://github.com/apache/iceberg/pull/4514))
  - Fixed missing metadata files resulting from an OOM during commit cleanup ([#4673](https://github.com/apache/iceberg/pull/4673))
  - Updated logging to use sanitized expressions to avoid leaking values ([#4672](https://github.com/apache/iceberg/pull/4672))
* Spark
  - Fixed Spark to skip calling abort when CommitStateUnknownException is thrown ([#4687](https://github.com/apache/iceberg/pull/4687))
  - Fixed MERGE commands with mixed case identifiers ([#4848](https://github.com/apache/iceberg/pull/4848))
* Flink
  - Fixed table property update failures when tables have a primary key ([#4561](https://github.com/apache/iceberg/pull/4561))
* Integrations
  - JDBC catalog behavior has been updated to pass common catalog tests ([#4220](https://github.com/apache/iceberg/pull/4220), [#4231](https://github.com/apache/iceberg/pull/4231))

#### Dependency changes

* Updated Apache Avro to 1.10.2 (previously 1.10.1)
* Updated Apache Parquet to 1.12.3 (previously 1.12.2)
* Updated Apache ORC to 1.7.5 (previously 1.7.2)
* Updated Apache Arrow to 7.0.0 (previously 6.0.0)
* Updated AWS SDK to 2.17.131 (previously 2.15.7)
* Updated Nessie to 0.30.0 (previously 0.18.0)
* Updated Caffeine to 2.9.3 (previously 2.8.4)

### 0.13.2

Apache Iceberg 0.13.2 was released on June 15th, 2022.

* Git tag: [0.13.2](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.13.2)
* [0.13.2 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.13.2/apache-iceberg-0.13.2.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.13.2/apache-iceberg-0.13.2.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.13.2/apache-iceberg-0.13.2.tar.gz.sha512)
* [0.13.2 Spark 3.2 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/0.13.2/iceberg-spark-runtime-3.2_2.12-0.13.2.jar)
* [0.13.2 Spark 3.1 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/0.13.2/iceberg-spark-runtime-3.1_2.12-0.13.2.jar)
* [0.13.2 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.13.2/iceberg-spark3-runtime-0.13.2.jar)
* [0.13.2 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.13.2/iceberg-spark-runtime-0.13.2.jar)
* [0.13.2 Flink 1.14 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.14/0.13.2/iceberg-flink-runtime-1.14-0.13.2.jar)
* [0.13.2 Flink 1.13 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.13/0.13.2/iceberg-flink-runtime-1.13-0.13.2.jar)
* [0.13.2 Flink 1.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.12/0.13.2/iceberg-flink-runtime-1.12-0.13.2.jar)
* [0.13.2 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.13.2/iceberg-hive-runtime-0.13.2.jar)

**Important bug fixes and changes:**

* **Core**
  * [\#4673](https://github.com/apache/iceberg/pull/4673) fixes table corruption from OOM during commit cleanup
  * [\#4514](https://github.com/apache/iceberg/pull/4514) row delta delete files were dropped in sequential commits after table format updated to v2
  * [\#4464](https://github.com/apache/iceberg/pull/4464) fixes an issue were conflicting transactions have been ignored during a commit
  * [\#4520](https://github.com/apache/iceberg/pull/4520) fixes an issue with wrong table predicate filtering with evolved partition specs
* **Spark**
  * [\#4663](https://github.com/apache/iceberg/pull/4663) fixes NPEs in Spark value converter
  * [\#4687](https://github.com/apache/iceberg/pull/4687) fixes an issue with incorrect aborts when non-runtime exceptions were thrown in Spark
* **Flink**
  * Note that there's a correctness issue when using upsert mode in Flink 1.12. Given that Flink 1.12 is deprecated, it was decided to not fix this bug but rather log a warning (see also [\#4754](https://github.com/apache/iceberg/pull/4754)).
* **Nessie**
  * [\#4509](https://github.com/apache/iceberg/pull/4509) fixes a NPE that occurred when accessing refreshed tables in NessieCatalog


A more exhaustive list of changes is available under the [0.13.2 release milestone](https://github.com/apache/iceberg/milestone/18?closed=1).

### 0.13.1

Apache Iceberg 0.13.1 was released on February 14th, 2022.

* Git tag: [0.13.1](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.13.1)
* [0.13.1 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.13.1/apache-iceberg-0.13.1.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.13.1/apache-iceberg-0.13.1.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.13.1/apache-iceberg-0.13.1.tar.gz.sha512)
* [0.13.1 Spark 3.2 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/0.13.1/iceberg-spark-runtime-3.2_2.12-0.13.1.jar)
* [0.13.1 Spark 3.1 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/0.13.1/iceberg-spark-runtime-3.1_2.12-0.13.1.jar)
* [0.13.1 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.13.1/iceberg-spark3-runtime-0.13.1.jar)
* [0.13.1 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.13.1/iceberg-spark-runtime-0.13.1.jar)
* [0.13.1 Flink 1.14 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.14/0.13.1/iceberg-flink-runtime-1.14-0.13.1.jar)
* [0.13.1 Flink 1.13 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.13/0.13.1/iceberg-flink-runtime-1.13-0.13.1.jar)
* [0.13.1 Flink 1.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.12/0.13.1/iceberg-flink-runtime-1.12-0.13.1.jar)
* [0.13.1 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.13.1/iceberg-hive-runtime-0.13.1.jar)

**Important bug fixes:**

* **Spark**
  * [\#4023](https://github.com/apache/iceberg/pull/4023) fixes predicate pushdown in row-level operations for merge conditions in Spark 3.2. 
  Prior to the fix, filters would not be extracted and targeted merge conditions were not pushed down leading to degraded performance
  for these targeted merge operations.
  * [\#4024](https://github.com/apache/iceberg/pull/4024) fixes table creation in the root namespace of a Hadoop Catalog.

* **Flink**
  * [\#3986](https://github.com/apache/iceberg/pull/3986) fixes manifest location collisions when there are multiple committers
  in the same Flink job.


### 0.13.0

Apache Iceberg 0.13.0 was released on February 4th, 2022.

* Git tag: [0.13.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.13.0)
* [0.13.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.13.0/apache-iceberg-0.13.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.13.0/apache-iceberg-0.13.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.13.0/apache-iceberg-0.13.0.tar.gz.sha512)
* [0.13.0 Spark 3.2 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/0.13.0/iceberg-spark-runtime-3.2_2.12-0.13.0.jar)
* [0.13.0 Spark 3.1 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.1_2.12/0.13.0/iceberg-spark-runtime-3.1_2.12-0.13.0.jar)
* [0.13.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.13.0/iceberg-spark3-runtime-0.13.0.jar)
* [0.13.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.13.0/iceberg-spark-runtime-0.13.0.jar)
* [0.13.0 Flink 1.14 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.14/0.13.0/iceberg-flink-runtime-1.14-0.13.0.jar)
* [0.13.0 Flink 1.13 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.13/0.13.0/iceberg-flink-runtime-1.13-0.13.0.jar)
* [0.13.0 Flink 1.12 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.12/0.13.0/iceberg-flink-runtime-1.12-0.13.0.jar)
* [0.13.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.13.0/iceberg-hive-runtime-0.13.0.jar)

**High-level features:**

* **Core**
  * Catalog caching now supports cache expiration through catalog property `cache.expiration-interval-ms` [[\#3543](https://github.com/apache/iceberg/pull/3543)]
  * Catalog now supports registration of Iceberg table from a given metadata file location [[\#3851](https://github.com/apache/iceberg/pull/3851)]
  * Hadoop catalog can be used with S3 and other file systems safely by using a lock manager [[\#3663](https://github.com/apache/iceberg/pull/3663)]
* **Vendor Integrations**
  * Google Cloud Storage (GCS) `FileIO` is supported with optimized read and write using GCS streaming transfer [[\#3711](https://github.com/apache/iceberg/pull/3711)]
  * Aliyun Object Storage Service (OSS) `FileIO` is supported [[\#3553](https://github.com/apache/iceberg/pull/3553)]
  * Any S3-compatible storage (e.g. MinIO) can now be accessed through AWS `S3FileIO` with custom endpoint and credential configurations [[\#3656](https://github.com/apache/iceberg/pull/3656)] [[\#3658](https://github.com/apache/iceberg/pull/3658)]
  * AWS `S3FileIO` now supports server-side checksum validation [[\#3813](https://github.com/apache/iceberg/pull/3813)]
  * AWS `GlueCatalog` now displays more table information including table location, description [[\#3467](https://github.com/apache/iceberg/pull/3467)] and columns [[\#3888](https://github.com/apache/iceberg/pull/3888)]
  * Using multiple `FileIO`s based on file path scheme is supported by configuring a `ResolvingFileIO` [[\#3593](https://github.com/apache/iceberg/pull/3593)]
* **Spark**
  * Spark 3.2 is supported [[\#3335](https://github.com/apache/iceberg/pull/3335)] with merge-on-read `DELETE` [[\#3970](https://github.com/apache/iceberg/pull/3970)]
  * `RewriteDataFiles` action now supports sort-based table optimization [[\#2829](https://github.com/apache/iceberg/pull/2829)] and merge-on-read delete compaction [[\#3454](https://github.com/apache/iceberg/pull/3454)]. The corresponding Spark call procedure `rewrite_data_files` is also supported [[\#3375](https://github.com/apache/iceberg/pull/3375)]
  * Time travel queries now use snapshot schema instead of the table's latest schema [[\#3722](https://github.com/apache/iceberg/pull/3722)]
  * Spark vectorized reads now support row-level deletes [[\#3557](https://github.com/apache/iceberg/pull/3557)] [[\#3287](https://github.com/apache/iceberg/pull/3287)]
  * `add_files` procedure now skips duplicated files by default (can be turned off with the `check_duplicate_files` flag) [[\#2895](https://github.com/apache/iceberg/issues/2779)], skips folder without file [[\#2895](https://github.com/apache/iceberg/issues/3455)] and partitions with `null` values [[\#2895](https://github.com/apache/iceberg/issues/3778)] instead of throwing exception, and supports partition pruning for faster table import [[\#3745](https://github.com/apache/iceberg/issues/3745)]
* **Flink**
  * Flink 1.13 and 1.14 are supported [[\#3116](https://github.com/apache/iceberg/pull/3116)] [[\#3434](https://github.com/apache/iceberg/pull/3434)]
  * Flink connector support is supported [[\#2666](https://github.com/apache/iceberg/pull/2666)]
  * Upsert write option is supported [[\#2863](https://github.com/apache/iceberg/pull/2863)]
* **Hive**
  * Table listing in Hive catalog can now skip non-Iceberg tables by disabling flag `list-all-tables` [[\#3908](https://github.com/apache/iceberg/pull/3908)]
  * Hive tables imported to Iceberg can now be read by `IcebergInputFormat` [[\#3312](https://github.com/apache/iceberg/pull/3312)]
* **File Formats**
  * ORC now supports writing delete file [[\#3248](https://github.com/apache/iceberg/pull/3248)] [[\#3250](https://github.com/apache/iceberg/pull/3250)] [[\#3366](https://github.com/apache/iceberg/pull/3366)]

**Important bug fixes:**

* **Core**
  * Iceberg new data file root path is configured through `write.data.path` going forward. `write.folder-storage.path` and `write.object-storage.path` are deprecated [[\#3094](https://github.com/apache/iceberg/pull/3094)]
  * Catalog commit status is `UNKNOWN` instead of `FAILURE` when new metadata location cannot be found in snapshot history [[\#3717](https://github.com/apache/iceberg/pull/3717)]
  * Dropping table now also deletes old metadata files instead of leaving them strained [[\#3622](https://github.com/apache/iceberg/pull/3622)]
  * `history` and `snapshots` metadata tables can now query tables with no current snapshot instead of returning empty [[\#3812](https://github.com/apache/iceberg/pull/3812)]
* **Vendor Integrations**
  * Using cloud service integrations such as AWS `GlueCatalog` and `S3FileIO` no longer fail when missing Hadoop dependencies in the execution environment [[\#3590](https://github.com/apache/iceberg/pull/3590)]
  * AWS clients are now auto-closed when related `FileIO` or `Catalog` is closed. There is no need to close the AWS clients separately [[\#2878](https://github.com/apache/iceberg/pull/2878)]
* **Spark**
  * For Spark >= 3.1, `REFRESH TABLE` can now be used with Spark session catalog instead of throwing exception [[\#3072](https://github.com/apache/iceberg/pull/3072)]
  * Insert overwrite mode now skips partition with 0 record instead of failing the write operation [[\#2895](https://github.com/apache/iceberg/issues/2895)]
  * Spark snapshot expiration action now supports custom `FileIO` instead of just `HadoopFileIO` [[\#3089](https://github.com/apache/iceberg/pull/3089)]
  * `REPLACE TABLE AS SELECT` can now work with tables with columns that have changed partition transform. Each old partition field of the same column is converted to a void transform with a different name [[\#3421](https://github.com/apache/iceberg/issues/3421)]
  * Spark SQL filters containing binary or fixed literals can now be pushed down instead of throwing exception [[\#3728](https://github.com/apache/iceberg/pull/3728)]
* **Flink**
  * A `ValidationException` will be thrown if a user configures both `catalog-type` and `catalog-impl`. Previously it chose to use `catalog-type`. The new behavior brings Flink consistent with Spark and Hive [[\#3308](https://github.com/apache/iceberg/issues/3308)]
  * Changelog tables can now be queried without `RowData` serialization issues [[\#3240](https://github.com/apache/iceberg/pull/3240)]
  * `java.sql.Time` data type can now be written without data overflow problem [[\#3740](https://github.com/apache/iceberg/pull/3740)]
  * Avro position delete files can now be read without encountering `NullPointerException` [[\#3540](https://github.com/apache/iceberg/pull/3540)]
* **Hive**
  * Hive catalog can now be initialized with a `null` Hadoop configuration instead of throwing exception [[\#3252](https://github.com/apache/iceberg/pull/3252)]
  * Table creation can now succeed instead of throwing exception when some columns do not have comments [[\#3531](https://github.com/apache/iceberg/pull/3531)]
* **File Formats**
  * Parquet file writing issue is fixed for string data with over 16 unparseable chars (e.g. high/low surrogates) [[\#3760](https://github.com/apache/iceberg/pull/3760)]
  * ORC vectorized read is now configured using `read.orc.vectorization.batch-size` instead of `read.parquet.vectorization.batch-size`  [[\#3133](https://github.com/apache/iceberg/pull/3133)]

**Other notable changes:**

* The community has finalized the long-term strategy of Spark, Flink and Hive support. See [Multi-Engine Support](multi-engine-support.md) page for more details.

### 0.12.1

Apache Iceberg 0.12.1 was released on November 8th, 2021.

* Git tag: [0.12.1](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.12.1)
* [0.12.1 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.12.1/apache-iceberg-0.12.1.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.12.1/apache-iceberg-0.12.1.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.12.1/apache-iceberg-0.12.1.tar.gz.sha512)
* [0.12.1 Spark 3.x runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.12.1/iceberg-spark3-runtime-0.12.1.jar)
* [0.12.1 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.12.1/iceberg-spark-runtime-0.12.1.jar)
* [0.12.1 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.12.1/iceberg-flink-runtime-0.12.1.jar)
* [0.12.1 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.12.1/iceberg-hive-runtime-0.12.1.jar)

Important bug fixes and changes:

* [\#3264](https://github.com/apache/iceberg/pull/3258) fixes validation failures that occurred after snapshot expiration when writing Flink CDC streams to Iceberg tables.
* [\#3264](https://github.com/apache/iceberg/pull/3264) fixes reading projected map columns from Parquet files written before Parquet 1.11.1.
* [\#3195](https://github.com/apache/iceberg/pull/3195) allows validating that commits that produce row-level deltas don't conflict with concurrently added files. Ensures users can maintain serializable isolation for update and delete operations, including merge operations.
* [\#3199](https://github.com/apache/iceberg/pull/3199) allows validating that commits that overwrite files don't conflict with concurrently added files. Ensures users can maintain serializable isolation for overwrite operations.
* [\#3135](https://github.com/apache/iceberg/pull/3135) fixes equality-deletes using `DATE`, `TIMESTAMP`, and `TIME` types.
* [\#3078](https://github.com/apache/iceberg/pull/3078) prevents the JDBC catalog from overwriting the `jdbc.user` property if any property called user exists in the environment.
* [\#3035](https://github.com/apache/iceberg/pull/3035) fixes drop namespace calls with the DyanmoDB catalog.
* [\#3273](https://github.com/apache/iceberg/pull/3273) fixes importing Avro files via `add_files` by correctly setting the number of records.
* [\#3332](https://github.com/apache/iceberg/pull/3332) fixes importing ORC files with float or double columns in `add_files`.

A more exhaustive list of changes is available under the [0.12.1 release milestone](https://github.com/apache/iceberg/milestone/15?closed=1).

### 0.12.0

Apache Iceberg 0.12.0 was released on August 15, 2021. It consists of 395 commits authored by 74 contributors over a 139 day period.

* Git tag: [0.12.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.12.0)
* [0.12.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.12.0/apache-iceberg-0.12.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.12.0/apache-iceberg-0.12.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.12.0/apache-iceberg-0.12.0.tar.gz.sha512)
* [0.12.0 Spark 3.x runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.12.0/iceberg-spark3-runtime-0.12.0.jar)
* [0.12.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.12.0/iceberg-spark-runtime-0.12.0.jar)
* [0.12.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.12.0/iceberg-flink-runtime-0.12.0.jar)
* [0.12.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.12.0/iceberg-hive-runtime-0.12.0.jar)

**High-level features:**

* **Core**
    * Allow Iceberg schemas to specify one or more columns as row identifiers [[\#2465](https://github.com/apache/iceberg/pull/2465)]. Note that this is a prerequisite for supporting upserts in Flink.
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
    * Added support for Spark 3.1 [[\#2512](https://github.com/apache/iceberg/pull/2512)].
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

* The Iceberg Community [voted to approve](https://mail-archives.apache.org/mod_mbox/iceberg-dev/202107.mbox/%3cCAMwmD1-k1gnShK=wQ0PD88it6cg9mY7Y1hKHjDZ7L-jcDzpyZA@mail.gmail.com%3e) version 2 of the Apache Iceberg Format Specification. The differences between version 1 and 2 of the specification are documented [here](spec.md#version-2).
* Bugfixes and stability improvements for NessieCatalog.
* Improvements and fixes for Iceberg's Python library.
* Added a vectorized reader for Apache Arrow [[\#2286](https://github.com/apache/iceberg/pull/2286)].
* The following Iceberg dependencies were upgraded:
    * Hive 2.3.8 [[\#2110](https://github.com/apache/iceberg/pull/2110)].
    * Avro 1.10.1 [[\#1648](https://github.com/apache/iceberg/pull/1648)].
    * Parquet 1.12.0 [[\#2441](https://github.com/apache/iceberg/pull/2441)].

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

