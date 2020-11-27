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

The latest version of Iceberg is [0.10.0](https://github.com/apache/iceberg/releases/tag/apache-iceberg-0.10.0).

* [0.10.0 source tar.gz](https://www.apache.org/dyn/closer.cgi/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz) -- [signature](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.asc) -- [sha512](https://downloads.apache.org/iceberg/apache-iceberg-0.10.0/apache-iceberg-0.10.0.tar.gz.sha512)
* [0.10.0 Spark 3.0 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark3-runtime/0.10.0/iceberg-spark3-runtime-0.10.0.jar)
* [0.10.0 Spark 2.4 runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime/0.10.0/iceberg-spark-runtime-0.10.0.jar)
* [0.10.0 Flink runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime/0.10.0/iceberg-flink-runtime-0.10.0.jar)
* [0.10.0 Hive runtime Jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-hive-runtime/0.10.0/iceberg-hive-runtime-0.10.0.jar)

To use Iceberg in Spark, download the runtime Jar and add it to the jars folder of your Spark install. Use iceberg-spark3-runtime for Spark 3, and iceberg-spark-runtime for Spark 2.4.

To use Iceberg in Hive, download the iceberg-hive-runtime Jar and add it to Hive using `ADD JAR`.

### Gradle

To add a dependency on Iceberg in Gradle, add the following to `build.gradle`:

```
dependencies {
  compile 'org.apache.iceberg:iceberg-core:0.10.0'
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
    <version>0.10.0</version>
  </dependency>
  ...
</dependencies>
```

## 0.10.0 release notes

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

## Past releases

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

