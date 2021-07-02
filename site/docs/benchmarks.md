<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

## Available Benchmarks and how to run them

Benchmarks are located under `<project-name>/jmh`. It is generally favorable to only run the tests of interest rather than running all available benchmarks.
Also note that JMH benchmarks run within the same JVM as the system-under-test, so results might vary between runs.


### IcebergSourceNestedListParquetDataWriteBenchmark
A benchmark that evaluates the performance of writing nested Parquet data using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedListParquetDataWriteBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-list-parquet-data-write-benchmark-result.txt`

### SparkParquetReadersNestedDataBenchmark
A benchmark that evaluates the performance of reading nested Parquet data using Iceberg and Spark Parquet readers. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=SparkParquetReadersNestedDataBenchmark -PjmhOutputPath=benchmark/spark-parquet-readers-nested-data-benchmark-result.txt`

### SparkParquetWritersFlatDataBenchmark
A benchmark that evaluates the performance of writing Parquet data with a flat schema using Iceberg and Spark Parquet writers. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=SparkParquetWritersFlatDataBenchmark -PjmhOutputPath=benchmark/spark-parquet-writers-flat-data-benchmark-result.txt`

### IcebergSourceFlatORCDataReadBenchmark
A benchmark that evaluates the performance of reading ORC data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceFlatORCDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-flat-orc-data-read-benchmark-result.txt`

### SparkParquetReadersFlatDataBenchmark
A benchmark that evaluates the performance of reading Parquet data with a flat schema using Iceberg and Spark Parquet readers. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=SparkParquetReadersFlatDataBenchmark -PjmhOutputPath=benchmark/spark-parquet-readers-flat-data-benchmark-result.txt`

### VectorizedReadDictionaryEncodedFlatParquetDataBenchmark
A benchmark to compare performance of reading Parquet dictionary encoded data with a flat schema using vectorized Iceberg read path and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=VectorizedReadDictionaryEncodedFlatParquetDataBenchmark -PjmhOutputPath=benchmark/vectorized-read-dict-encoded-flat-parquet-data-result.txt`

### IcebergSourceNestedListORCDataWriteBenchmark
A benchmark that evaluates the performance of writing nested Parquet data using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedListORCDataWriteBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-list-orc-data-write-benchmark-result.txt`

### VectorizedReadFlatParquetDataBenchmark
A benchmark to compare performance of reading Parquet data with a flat schema using vectorized Iceberg read path and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=VectorizedReadFlatParquetDataBenchmark -PjmhOutputPath=benchmark/vectorized-read-flat-parquet-data-result.txt`

### IcebergSourceFlatParquetDataWriteBenchmark
A benchmark that evaluates the performance of writing Parquet data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceFlatParquetDataWriteBenchmark -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-write-benchmark-result.txt`

### IcebergSourceNestedAvroDataReadBenchmark
A benchmark that evaluates the performance of reading Avro data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedAvroDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-avro-data-read-benchmark-result.txt`

### IcebergSourceFlatAvroDataReadBenchmark
A benchmark that evaluates the performance of reading Avro data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceFlatAvroDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-flat-avro-data-read-benchmark-result.txt`

### IcebergSourceNestedParquetDataWriteBenchmark
A benchmark that evaluates the performance of writing nested Parquet data using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedParquetDataWriteBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-parquet-data-write-benchmark-result.txt`

### IcebergSourceNestedParquetDataReadBenchmark
* A benchmark that evaluates the performance of reading nested Parquet data using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

` ./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedParquetDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-parquet-data-read-benchmark-result.txt`

### IcebergSourceNestedORCDataReadBenchmark
A benchmark that evaluates the performance of reading ORC data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedORCDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-orc-data-read-benchmark-result.txt`

### IcebergSourceFlatParquetDataReadBenchmark
A benchmark that evaluates the performance of reading Parquet data with a flat schema using Iceberg and the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceFlatParquetDataReadBenchmark -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-read-benchmark-result.txt`

### IcebergSourceFlatParquetDataFilterBenchmark
A benchmark that evaluates the file skipping capabilities in the Spark data source for Iceberg. This class uses a dataset with a flat schema, where the records are clustered according to the
column used in the filter predicate. The performance is compared to the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:

`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceFlatParquetDataFilterBenchmark -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-filter-benchmark-result.txt`

### IcebergSourceNestedParquetDataFilterBenchmark
A benchmark that evaluates the file skipping capabilities in the Spark data source for Iceberg. This class uses a dataset with nested data, where the records are clustered according to the
column used in the filter predicate. The performance is compared to the built-in file source in Spark. To run this benchmark for either spark-2 or spark-3:
`./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=IcebergSourceNestedParquetDataFilterBenchmark -PjmhOutputPath=benchmark/iceberg-source-nested-parquet-data-filter-benchmark-result.txt`

### SparkParquetWritersNestedDataBenchmark
* A benchmark that evaluates the performance of writing nested Parquet data using Iceberg and Spark Parquet writers. To run this benchmark for either spark-2 or spark-3:
  `./gradlew :iceberg-spark[2|3]:jmh -PjmhIncludeRegex=SparkParquetWritersNestedDataBenchmark -PjmhOutputPath=benchmark/spark-parquet-writers-nested-data-benchmark-result.txt`
