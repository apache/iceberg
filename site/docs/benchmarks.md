---
title: "Benchmarks"
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

## Available Benchmarks and how to run them

Benchmarks are located under `<module>/src/jmh`. It is generally better to run only the benchmarks you are investigating instead of the full suite.
Also note that JMH benchmarks run in the same JVM as the system under test, so results may vary between runs.

## Running Benchmarks on GitHub

It is possible to run one or more benchmarks via the **JMH Benchmarks** GitHub Actions workflow on your own fork of the Iceberg repository. This workflow takes the following inputs:

* The repository name to run against, such as `apache/iceberg` or `<user>/iceberg`
* The branch name to benchmark, such as `main` or `my-feature-branch`
* A comma-separated list of double-quoted benchmark names, such as `"IcebergSourceFlatParquetDataReadBenchmark", "IcebergSourceFlatParquetDataFilterBenchmark", "IcebergSourceNestedListParquetDataWriteBenchmark"`

Benchmark results will be uploaded once **all** benchmarks are done.

GitHub-hosted runners have limited and shared resources, so treat these results as directional signals for understanding code changes rather than as production-grade measurements.

## Running Benchmarks locally

JMH writes human-readable output to `build/reports/jmh/human-readable-output.txt` and JSON output to `build/reports/jmh/results.json` by default. Override them with `-PjmhOutputPath=<path>` and `-PjmhJsonOutputPath=<path>` if needed.

The default versions in this repository are:

* Spark `4.1`
* Flink `2.1`
* Scala `2.12`

Core and data benchmarks can be run directly. Spark and Flink benchmarks use versioned Gradle modules.
Spark `4.1` benchmarks use `2.13` module names, while older Spark modules follow the configured `scalaVersion`.

### Command templates

#### Core and data modules

```bash
./gradlew :iceberg-core:jmh -PjmhIncludeRegex=<BenchmarkName>
./gradlew :iceberg-data:jmh -PjmhIncludeRegex=<BenchmarkName>
```

#### Spark modules

Run benchmarks in the default Spark module:

```bash
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

Run benchmarks in the default Spark extensions module:

```bash
./gradlew :iceberg-spark:iceberg-spark-extensions-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

To use another supported Spark version, set the version properties and update the module name accordingly. For example:

```bash
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
  -PjmhIncludeRegex=IcebergSourceFlatParquetDataReadBenchmark
```

#### Flink modules

Run benchmarks in the default Flink module:

```bash
./gradlew :iceberg-flink:iceberg-flink-2.1:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

To use another supported Flink version, set the version property and update the module name. For example:

```bash
./gradlew -DflinkVersions=2.0 \
  :iceberg-flink:iceberg-flink-2.0:jmh \
  -PjmhIncludeRegex=MapRangePartitionerBenchmark
```

### Core benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-core:jmh -PjmhIncludeRegex=<BenchmarkName>
```

* `AppendBenchmark`: Append data files to a table.
* `ManifestReadBenchmark`: Read table manifests.
* `ManifestWriteBenchmark`: Write table manifests.
* `MetricsConfigBenchmark`: Evaluate metrics configuration lookups.
* `ReplaceDeleteFilesBenchmark`: Replace delete files during a commit.
* `RewriteDataFilesBenchmark`: Rewrite data files in table maintenance flows.
* `RoaringPositionBitmapBenchmark`: Measure roaring bitmap operations used for position deletes.
* `CountersBenchmark`: Measure metrics counter updates.
* `ZOrderByteUtilsBenchmark`: Measure Z-order byte utility operations.

### Data benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-data:jmh -PjmhIncludeRegex=<BenchmarkName>
```

* `GenericOrcReaderBenchmark`: Read ORC data through the generic Iceberg reader.
* `GenericParquetReaderBenchmark`: Read Parquet data through the generic Iceberg reader.
* `PartitionStatsHandlerBenchmark`: Read and write partition statistics metadata.

### Spark action benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

* `DeleteOrphanFilesBenchmark`: Execute the delete-orphan-files action.
* `IcebergSortCompactionBenchmark`: Execute sort-based compaction.

### Spark Parquet reader and writer benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

* `SparkParquetReadersFlatDataBenchmark`: Read flat Parquet data with Spark Parquet readers.
* `SparkParquetReadersNestedDataBenchmark`: Read nested Parquet data with Spark Parquet readers.
* `SparkParquetWritersFlatDataBenchmark`: Write flat Parquet data with Spark Parquet writers.
* `SparkParquetWritersNestedDataBenchmark`: Write nested Parquet data with Spark Parquet writers.

### Spark source benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

* `AvroWritersBenchmark`: Write Avro data through Iceberg writer implementations.
* `DVReaderBenchmark`: Read data with deletion vectors.
* `DVWriterBenchmark`: Write deletion vectors.
* `IcebergSourceFlatAvroDataReadBenchmark`: Read flat Avro data through the Spark Iceberg source.
* `IcebergSourceFlatORCDataReadBenchmark`: Read flat ORC data through the Spark Iceberg source.
* `IcebergSourceFlatParquetDataFilterBenchmark`: Evaluate file skipping for clustered flat Parquet data.
* `IcebergSourceFlatParquetDataReadBenchmark`: Read flat Parquet data through the Spark Iceberg source.
* `IcebergSourceFlatParquetDataWriteBenchmark`: Write flat Parquet data through the Spark Iceberg source.
* `IcebergSourceNestedAvroDataReadBenchmark`: Read nested Avro data through the Spark Iceberg source.
* `IcebergSourceNestedListORCDataWriteBenchmark`: Write nested-list ORC data through the Spark Iceberg source.
* `IcebergSourceNestedListParquetDataWriteBenchmark`: Write nested-list Parquet data through the Spark Iceberg source.
* `IcebergSourceNestedORCDataReadBenchmark`: Read nested ORC data through the Spark Iceberg source.
* `IcebergSourceNestedParquetDataFilterBenchmark`: Evaluate file skipping for clustered nested Parquet data.
* `IcebergSourceNestedParquetDataReadBenchmark`: Read nested Parquet data through the Spark Iceberg source.
* `IcebergSourceNestedParquetDataWriteBenchmark`: Write nested Parquet data through the Spark Iceberg source.
* `IcebergSourceParquetEqDeleteBenchmark`: Read Parquet data with equality deletes applied.
* `IcebergSourceParquetMultiDeleteFileBenchmark`: Read Parquet data with many delete files applied.
* `IcebergSourceParquetPosDeleteBenchmark`: Read Parquet data with position deletes applied.
* `IcebergSourceParquetWithUnrelatedDeleteBenchmark`: Read Parquet data when unrelated delete files are present.
* `ParquetWritersBenchmark`: Write Parquet data through Iceberg writer implementations.
* `VectorizedReadDictionaryEncodedFlatParquetDataBenchmark`: Vectorized reads of dictionary-encoded flat Parquet data.
* `VectorizedReadFlatParquetDataBenchmark`: Vectorized reads of flat Parquet data.
* `VectorizedReadParquetDecimalBenchmark`: Vectorized reads of Parquet decimal columns.

### Spark extensions benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-spark:iceberg-spark-extensions-4.1_2.13:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

* `DeleteFileIndexBenchmark`: Build and query delete file indexes.
* `MergeCardinalityCheckBenchmark`: Perform merge cardinality checks.
* `PlanningBenchmark`: Plan Spark scans over Iceberg tables.
* `TaskGroupPlanningBenchmark`: Plan Spark task groups for distributed execution.
* `UpdateProjectionBenchmark`: Apply projection logic for Spark update operations.

### Flink benchmarks

Run benchmarks in this group with:

```bash
./gradlew :iceberg-flink:iceberg-flink-2.1:jmh \
  -PjmhIncludeRegex=<BenchmarkName>
```

* `DynamicRecordSerializerDeserializerBenchmark`: Serialize and deserialize dynamic Flink records.
* `MapRangePartitionerBenchmark`: Partition Flink shuffle data with the map-based range partitioner.
* `SketchRangePartitionerBenchmark`: Partition Flink shuffle data with the sketch-based range partitioner.
* `StatisticsRecordSerializerBenchmark`: Serialize shuffle statistics records in Flink.
