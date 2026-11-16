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

# Vectorized vs row-by-row Parquet reads of list columns

## Reader-level comparison: `VectorizedListReadBenchmark` (no Spark)

It reads two list columns (`list<int>` and `list<string>`) from a
Parquet file with the Arrow vectorized reader and compares that with the generic
row-by-row reader. Both shapes hold about 2.5M list elements in total. `FIXED_768` uses lists of
exactly 768 elements, `RANDOM` generates a list with length between 0 and 19.

This is **not** an apples-to-apples comparison, because the vectorized reader creates an Arrow vector
while the row-by-row reader's results are Record objects.

### Setup
- Apple M4 Pro (14 cores), 48 GB RAM, JDK 17.0.17, JMH 1.37, `-Xmx32g`
- 1 fork, 3 warmup + 10 measurement iterations, `SingleShotTime`, 1 thread
- Table: 1 Parquet file total element count ~ 2.5M
- Batch size: 5000
- Row numbers:
  - `RANDOM`: 263,157
  - `FIXED_768`: 3,255
- Schema: two list columns(`list<int>` and `list<string>`)

### Results

| List shape | Row-by-row | Vectorized | Vectorized vs row-by-row |
|---|---|---|---|
| FIXED_768 | 0.315 ± 0.012 s | 0.375 ± 0.007 s | ~19% slower (1.19x) |
| RANDOM | 0.337 ± 0.019 s | 0.396 ± 0.029 s | ~17% slower (1.17x) |

- It's clear that the vectorized reader in this form didn't give any performance benefit.
The main reason is that the reading of repetition levels are read sequentially together with the values, and the cost of
copying data between different arrow vectors (from the elements vector to the List vector's data vector).
- It's possible to do significantly faster read of fixed lists as demonstrated by other libraries (Spark's own Parquet
reader, Hardwood), but it requires a different approach than the current one.

## Preventing the fallback to row-by-row reads in Spark: `VectorizedReadListFallbackBenchmark`

There is a scenario where the vectorized reader can provide real performance benefit. The `SparkBatch` requires all
columns to be supported for vectorized reads to allow vectorization.
In a use case where there are many columns that already support vectorized reads,
it would mean a significant performance penalty to disable vectorization just because of a single list column.
It can prevent the fallback to the row-by-row reader now.

This benchmark compares the performance of reading a table with a wide schema (one list and 999 int columns).
It measures the difference between the speedup of reading all int columns vectorized
and the slowdown of reading the list column we saw in the previous benchmark.

### Setup

- Apple M4 Pro (14 cores), 48 GB RAM, JDK 17.0.17, JMH 1.37, `-Xmx32g`, Spark local mode
- 1 fork, 3 warmup + 5 measurement iterations, `SingleShotTime`, 1 thread
- Table: 1 Parquet file 10,000 rows
- Batch size: 5000
- Schema: 999 int columns and 1 list column (`list<int>`)

### Results (seconds per read, lower is better)

| Schema | Row-by-row | Vectorized | Vectorized vs row-by-row |
|---|---|---|---|
| Single list column | 0.149 ± 0.020 | 0.144 ± 0.026 | ~3% faster, within noise |
| Wide | 0.530 ± 0.109 | 0.389 ± 0.088 | ~1.36x faster (27% less time) |

- **Wide schema:** vectorized is faster. The 999 int columns now stay on the batch
  path, because a projected list no longer forces the whole scan to row-by-row.
- **Single list column:** the expectation that vectorized would be slower was **not** observed.
  This is likely because the data is small and the fixed Spark overhead dominates the read time.

## Reproduce

```
SPARK_LOCAL_IP=localhost ./gradlew -DsparkVersions=4.1 -DscalaVersion=2.13 \
    :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
    -PjmhIncludeRegex=VectorizedReadListFallbackBenchmark \
    -PjmhOutputPath=benchmark/list-fallback-results.txt

./gradlew :iceberg-data:jmh -PjmhIncludeRegex=VectorizedListReadBenchmark \
    -PjmhOutputPath=benchmark/vectorized-list-results.txt
```
