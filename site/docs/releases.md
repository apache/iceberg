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

The latest version of Iceberg is [0.11.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.11.0).

* [0.11.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.11.0/apache-iceberg-0.11.0.tar.gz.sha512)
* [0.11.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.11.0/iceberg-spark3-runtime-0.11.0.jar)
* [0.11.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.11.0/iceberg-spark-runtime-0.11.0.jar)
* [0.11.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.11.0/iceberg-flink-runtime-0.11.0.jar)
* [0.11.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.11.0/iceberg-hive-runtime-0.11.0.jar)

To use Iceberg in Spark, download the runtime Jar and add it to the jars folder of your Spark install. Use iceberg-spark3-runtime for Spark 3, and iceberg-spark-runtime for Spark 2.4.

To use Iceberg in Hive, download the iceberg-hive-runtime Jar and add it to Hive using `ADD JAR`.

### Gradle

To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:

```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:0.11.0'
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
    <version>0.11.0</version>
  </dependency>
  ...
</dependencies>
```

## 0.11.0 release notes

High-level features:

* **Core API** now supports partition spec and sort order evolution
* **Spark 3** now supports the following SQL extensions:
    * MERGE INTO (experimental)
    * DELETE FROM (experimental)
    * ALTER TABLE ... ADD/DROP PARTITION
    * ALTER TABLE ... WRITE ORDERED BY
    * invoke stored procedures using CALL
* **Flink** now supports streaming reads, CDC writes (experimental), and filter pushdown
* **AWS module** is added to support better integration with AWS, with [AWS Glue catalog](https://aws.amazon.com/glue/) support and dedicated S3 FileIO implementation
* **Nessie module** is added to support integration with [project Nessie](https://projectnessie.org/)

Important bug fixes:

* [\#2091](https://github.com/apache/iceberg/pull/2091) fixes Parquet vectorized reads when column types are promoted
* [\#1991](https://github.com/apache/iceberg/pull/1991) fixes Avro schema conversions to preserve field docs
* [\#1981](https://github.com/apache/iceberg/pull/1981) fixes date and timestamp transforms
* [\#1962](https://github.com/apache/iceberg/pull/1962) fixes Parquet vectorized position reader
* [\#1811](https://github.com/apache/iceberg/pull/1811) makes refreshing Spark cache optional
* [\#1798](https://github.com/apache/iceberg/pull/1798) fixes read failure when encountering duplicate entries of data files
* [\#1785](https://github.com/apache/iceberg/pull/1785) fixes invalidation of metadata tables in CachingCatalog
* [\#1784](https://github.com/apache/iceberg/pull/1784) fixes resolving of SparkSession table's metadata tables

Other notable changes:

* NaN counter is added to format v2 metrics
* Shared catalog properties are added in core library to standardize catalog level configurations
* Spark and Flink now supports dynamically loading customized `Catalog` and `FileIO` implementations
* Spark now supports loading tables with file paths via HadoopTables
* Spark 2 now supports loading tables from other catalogs, like Spark 3
* Spark 3 now supports catalog names in DataFrameReader when using Iceberg as a format
* Hive now supports INSERT INTO, case insensitive query, projection pushdown, create DDL with schema and auto type conversion
* ORC now supports reading tinyint, smallint, char, varchar types
* Hadoop catalog now supports role-based access of table listing

## Past releases

### 0.10.0

* Git tag: [0.10.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.10.0)
* [0.10.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.sha512)
* [0.10.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.10.0/iceberg-spark3-runtime-0.10.0.jar)
* [0.10.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.10.0/iceberg-spark-runtime-0.10.0.jar)
* [0.10.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.10.0/iceberg-flink-runtime-0.10.0.jar)
* [0.10.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.10.0/iceberg-hive-runtime-0.10.0.jar)

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

