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

# Iceberg on the GPU for Spark 4.2

This module plugs the GPU merge-on-read scan of `iceberg-tornadovm` (TornadoVM + cuDF, NVIDIA GPUs
only) into unmodified Spark SQL. It does three things, and in each case anything it cannot do
exactly is left to Spark:

| What | How it hooks in | When it is used |
|---|---|---|
| **Reads**: a Spark task's data files are read on the GPU with their position, deletion-vector and equality deletes applied and Spark's pushed filters evaluated, and come back as `ColumnarBatch`es | `PartitionReaderFactoryProvider` (`spark.sql.iceberg.partition-reader-factory-provider=tornadovm`) | Parquet files read whole; columns are int, bigint, float, double, date, timestamp, string or decimal up to 18 digits, and at least one is not a string; a task of at least `spark.iceberg.gpu.minRows` rows |
| **Grouped aggregates**: an `Aggregate` over filters and projections of one Iceberg table runs as a single GPU scan, and only the groups come back | planner strategy (`GpuAggregateExtensions`) | SUM / AVG / COUNT of products of double columns and `1 - c` / `1 + c`; integer and decimal sums the files' bounds prove exact; up to 4 group columns or Iceberg's `bucket(n, bigint)`; value columns null-free by metrics |
| **Compaction**: `rewrite_data_files` (bin-pack) rewrites each file group on the GPU, and Iceberg's action plans and commits as usual | `FileRewriteRunnerProvider` (`spark.sql.iceberg.rewrite-runner-provider=tornadovm`) | unsorted tables, partitioned or not (one partition spec), whose columns are int, bigint, float, double, date, timestamp, string or decimal up to 18 digits |

The two providers are the engine-neutral extension points this branch adds to Iceberg's Spark 4.2
module (commit "Spark 4.2: Add pluggable partition reader factory and rewrite runner providers",
proposed to Apache Iceberg). Both are off unless a provider is named, and this module is the
provider called `tornadovm`; `spark-submit-gpu` names it.

All three work on tables on local disk or on an object store (S3, GCS, ADLS: anything Iceberg's
`FileIO` reads), with merge-on-read deletes of every kind, and on evolved schemas: columns added
after a file was written read as nulls, and int→long and float→double promotions are widened on
the device.

Answers match Spark's. The tests compare every path with the GPU on and off. On TPC-DS SF10 after
merge-on-read maintenance, all 102 runnable queries return identical results on Spark, Spark + GPU
and Spark RAPIDS.

## Where it helps, and where it does not

The GPU wins when the work stays on the device:

- **Aggregations**: TPC-H Q1 and Q6 on SF10 run 3–5× faster than Spark and 2–8× faster than Spark
  RAPIDS.
- **Selective filters**: only matching rows cross PCIe.
- **Expensive deletes**: equality deletes, many delete files.
- **Compaction**: compare `tornadovm/run.sh gpu <warehouse> rewrite 3 <table>` with `tornadovm/run.sh spark` on the same table.

Reads are decided per Spark task, by the kinds of deletes it applies
(`spark.iceberg.gpu.read.policy`, default `auto`): the GPU reads task groups with **equality
deletes**, and Iceberg's own reader keeps everything else. Paired runs of the same scans on both
readers (`spark.iceberg.gpu.scanLog`, `tornadovm/scanfit.py`) showed why:

- Without deletes, and with position deletes or deletion vectors, Iceberg's CPU reader is faster:
  the GPU won 29 of 690 TPC-DS SF10 scans (186.8 s against 60.8 s of scan time summed), and a full
  scan of 100M rows took 380-510 ms on the CPU against 475-570 ms on the GPU.
- With equality deletes the CPU reader probes every row: the GPU read 100M rows in ~500 ms against
  0.7 s (10K deleted keys) and 2.8-3.3 s (1M keys); with 10M keys the CPU reader ran out of memory.

`always` reads every task group the GPU can on it, `never` none (as `spark.iceberg.gpu.read.enabled=false`).

## Requirements

- An NVIDIA GPU (compute capability 7.0 or newer), CUDA 13 driver, Linux x86_64.
- JDK 21. The device scan uses Java's foreign-memory API, which needs `--enable-preview` on 21.
- Spark 4.2 (Scala 2.13), **local mode**: the GPU work runs in the driver JVM.
- TornadoVM 8.1.0 (`sdk install tornadovm 8.1.0-jdk21-cuda`) and libcudf 26.08 with the CUDA 13
  runtime (`source tornadovm/cudf-env.sh`): the steps are in [../README.md](../README.md#set-up).

Check all of it, and whether your tables qualify, with:

```bash
tornadovm/preflight.sh /data/wh/db/sales        # table directories are optional
```

## Quick start

```bash
# Set up as in ../README.md (TORNADOVM_HOME, JAVA_HOME, source tornadovm/cudf-env.sh), then:
export SPARK_HOME=/opt/spark-4.2.1-bin-hadoop3   # https://spark.apache.org/downloads.html
./gradlew -DsparkVersions=4.2 :iceberg-tornadovm-spark:sparkGpuBundle
tornadovm/spark/build/spark-gpu/bin/spark-submit-gpu --driver-memory 16g \
    tornadovm/spark/examples/quickstart.py /tmp/quickstart-wh
```

[examples/quickstart.py](examples/quickstart.py) creates a v3 merge-on-read table of 20M rows,
deletes every 20th row (deletion vectors), runs a grouped aggregate on the GPU and on Spark's CPU
path and compares the answers, then compacts the table with `rewrite_data_files`. On an RTX 4090:

```
aggregate: GPU 0.09 s (on the GPU: True), Spark 0.21 s, same answer: True
... INFO GpuFileRewriteRunnerProvider: Rewriting local.db.sales on the GPU
... Rewriting 8 files (GPU-BIN-PACK, file group 1/1) in local.db.sales
compaction: 0.54 s
rows after compaction: 19000000 (expected 19000000)
```

In your own job, nothing changes but the launcher: run it with `spark-submit-gpu` instead of
`spark-submit`. Queries, `CALL ... rewrite_data_files` and the catalog configuration stay as they
are, and whatever the GPU cannot do exactly runs on Spark. To see what ran where, look for
`GpuIcebergAggregate` in a query's executed plan, `GPU-BIN-PACK` in compaction logs, and set
`spark.iceberg.gpu.scanLog` for reads.

## Build and run

```
export TORNADOVM_HOME=...        # the SDK above
./gradlew -DsparkVersions=4.2 :iceberg-tornadovm-spark:sparkGpuBundle
build=tornadovm/spark/build/spark-gpu
SPARK_HOME=... $build/bin/spark-submit-gpu --driver-memory 24g my_job.py
```

`sparkGpuBundle` writes `build/spark-gpu/`:

- `jars/`: this module, Iceberg's modules (with the SQL extensions, for `CALL`), and the Parquet,
  Avro, Arrow and ORC versions they need. Everything a Spark distribution already ships is left out.
- `tornado-modules/` and `tornado-argfile`: TornadoVM's module path, without the jars that would
  clash with Spark's. TornadoVM's own argfile puts its whole jar directory on the module path, and
  Java then resolves every jar there as an automatic module, including JMH, log4j and junit.
- `bin/spark-submit-gpu`: `spark-submit` in local mode, with the class path, the driver JVM options
  and the GPU settings filled in. Its arguments are `spark-submit`'s.

Compaction then runs as it always does, on the GPU when the table qualifies:

```sql
CALL local.system.rewrite_data_files(table => 'db.sales')
```

## Configuration

| Spark setting | Default | |
|---|---|---|
| `spark.iceberg.gpu.enabled` | `false` | turns all three on (`spark-submit-gpu` sets it) |
| `spark.iceberg.gpu.read.enabled` | `true` | reads on the GPU (`false` keeps aggregates and compaction on it) |
| `spark.iceberg.gpu.read.policy` | `auto` | which reads: `auto` task groups with equality deletes, `always`, `never` |
| `spark.iceberg.gpu.scanLog` | unset | a directory: per-scan timings of both readers, for `tornadovm/scanfit.py` |
| `spark.iceberg.gpu.minRows` | `1000000` | fewest rows a Spark task must read for the GPU to read it; smaller tasks are faster on Spark's reader |
| `spark.iceberg.gpu.aggregate.enabled` | `true` | the aggregate strategy |
| `spark.iceberg.gpu.aggregate.nullable` | `false` | aggregates of columns that may hold nulls (SUM and AVG skip nulls, COUNT(column) counts the rest). Off by default: on TPC-DS it moves many small scalar subqueries to one device scan each, slower than Spark (q9 1.5 s to 4.3 s) |
| `spark.iceberg.gpu.aggregate.star` | `false` | star aggregates: an aggregate over inner joins of one Iceberg fact table with small dimensions, the joins done on the device by key lookup. TPC-DS SF10: 20 star queries 1.37x geomean, the whole suite 1.08x |
| `spark.iceberg.gpu.aggregate.star.maxGroups` | `4194304` | most finest groups a star may produce (the product of its dimensions' attribute tuples); larger ones run on Spark |
| `spark.iceberg.gpu.aggregate.star.maxDimensionRows` | `10000000` | largest table a star treats as a dimension |
| `spark.iceberg.gpu.aggregate.probe` | `false` | probe aggregates: an aggregate over joins of two fact tables -- the larger with its dimensions on the device, the other side a plan Spark runs and collects, joined on equalities (and one comparison) between the two. TPC-DS q72 11.8 to 1.1 s |
| `spark.iceberg.gpu.aggregate.probe.maxBuildRows` | `20000000` | most rows the collected side may have; more and Spark runs the aggregate |
| `spark.iceberg.gpu.rewrite.enabled` | `true` | compaction on the GPU |

| JVM property (driver) | Default | |
|---|---|---|
| `tornado.device.memory` | `20GB` (`GPU_DEVICE_MEMORY`) | TornadoVM's share of device memory. Leave a few GB for cuDF's own allocations |
| `iceberg.tornadovm.streams` | `4` | concurrent device streams |
| `iceberg.tornadovm.rewriteStreams` | the streams | concurrent streams of a compaction's file group. Each holds a batch's read, gathered and encoded columns at once |
| `iceberg.tornadovm.rewriteBatchBytes` | 768 MB | most decoded bytes in one compaction batch; wide tables (e.g. with strings) get fewer rows per batch. A batch that runs out of device memory is retried at half the rows on one stream |
| `iceberg.tornadovm.cacheDir` | `java.io.tmpdir`/`iceberg-tornadovm-cache` | local copies of object-store files |
| `iceberg.tornadovm.cacheBytes` | 64 GB | the cache's size; least recently used files go first |
| `iceberg.tornadovm.fetchThreads` | `16` | parallel fetches from the object store |
| `iceberg.tornadovm.aggregateShapes` | `6` | aggregate query shapes (group columns, values, a star's joins) whose device regions are kept JVM-wide; past this the least recently used idle ones are freed |
| `iceberg.tornadovm.keptScans` | `8` | device scans (table snapshot and filter) the aggregate strategy keeps |
| `iceberg.tornadovm.eagerZeroes` | `false` | allocate regions' host buffers zero-filled up front instead of as lazily mapped pages (which need `--enable-native-access=ALL-UNNAMED`; without it the scan falls back to this) |
| `iceberg.tornadovm.sharedPools` | `6` | table and projection shapes that keep device buffers. Past this, the least recently used idle ones are freed |
| `iceberg.tornadovm.spark.debug` | `false` | per-task timings on stderr |
| `iceberg.tornadovm.debug` | `false` | per-batch and per-compaction-group sizes on stderr |

## Limits

- Local mode only.
- No rewrites of sorted tables, of tables whose partition spec evolved, or of decimals over 18
  digits.
- Object-store files are fetched whole into a local cache before cuDF reads them (and a
  compaction's files are uploaded after it writes them); the cache needs local disk.
- Columns added with a default value (v3), required added columns and added string columns are
  declined.
- Nested types are not read.
- Reads take whole Parquet files only, as Iceberg plans unsplit files. A file split inside a task is
  read by Spark.
- Strings longer than 8 bytes cannot be group keys or filter terms on the device. Such an aggregate
  runs on Spark, after a failed first attempt on the device.
