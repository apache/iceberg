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


# iceberg-tornadovm: Iceberg merge-on-read and maintenance on the GPU

Iceberg's per-row work, run on an NVIDIA GPU from plain Java: merge-on-read scans that apply
position deletes, deletion vectors and equality deletes, compaction, and the conversion of equality
deletes into deletion vectors. Iceberg still does the planning and the commits. The GPU only
replaces the work in between.

It is a library on Iceberg's public APIs. It needs no engine and no change to Iceberg. The GPU code
is ordinary Java methods that [TornadoVM](https://github.com/beehive-lab/TornadoVM) compiles to
CUDA, plus [cuDF](https://github.com/rapidsai/cudf) for Parquet. Every operation has a CPU twin
(`CpuMergeOnReadScan`), the same algorithm on all cores, for comparison and as the reference.

## What it does

```java
try (DeviceMergeOnReadScan scan = DeviceMergeOnReadScan.of(table)) {
  // Scans: every delete applied on the device; only the answer comes back.
  AggregateResult byK = scan.aggregate("k", "v1", "v2");            // GROUP BY k: COUNT(*), SUM(v1), SUM(v2)
  AggregateResult buckets = scan.aggregateByBucket("id", 16, "v1");   // GROUP BY bucket(16, id)
  long live = scan.materialize(List.of("id", "k", "v1"), rows -> { ... }); // live rows, to the host

  // Maintenance: each is one Iceberg commit, validated against the snapshot it planned.
  scan.convertEqualityDeletes();       // equality deletes -> a deletion vector per data file (v3),
                                       //   or a file-scoped position-delete file (v2)
  scan.deleteKeys("id", keys);         // DELETE ... WHERE id IN (keys), as deletion vectors
  scan.computeTableStats("id", "k");   // NDV theta sketches in a Puffin file, as compute_table_stats
  scan.rewriteDataFiles();             // compaction: deletes applied, files rewritten
  scan.rewriteDataFilesSorted();       // compaction in the table's sort order, sort_order_id set
  scan.rewriteDataFilesZOrder("k", "v1");
}
// Filters: Iceberg prunes files, the device applies the residual.
DeviceMergeOnReadScan.of(table, Expressions.and(lessThan("id", 25_000_000L), greaterThan("v2", 0.5)));
```

**`convertEqualityDeletes()`** reads only the equality key columns of the data files that
equality deletes apply to. It applies every delete as a scan would, marks the rows merge-on-read
drops, and writes one deletion vector per data file, with that file's earlier deletion vector or
file-scoped position deletes folded in. One `RowDelta` adds the vectors and removes the
equality-delete files and the deletes they replace, validated against the planned snapshot.
These are the rules Flink's `ConvertEqualityDeletes` maintenance task follows (apache/iceberg
#16858, #16874, #17189), here without a Flink job. Iceberg v4 forbids writing new equality
deletes (#17783), so this is the conversion v4 tables need. On a v2 table each data file gets one
file-scoped position-delete file instead.

**`deleteKeys(column, keys)`** deletes by key without writing an equality-delete file: what a CDC
writer does for keys it has seen before (Spark and Flink write deletion vectors only for positions
they already know, #11561, #14197). Iceberg prunes files by the column's bounds (`IN` up to
`iceberg.tornadovm.deleteKeys.inLimit` keys, 1000 by default, otherwise the key range); the keys
stay in memory; the device finds their rows. The `RowDelta` also validates that no data file added
since could hold one of the keys.

**`computeTableStats(columns...)`** writes what Spark's `compute_table_stats` writes: an
`apache-datasketches-theta-v1` blob a column (MurmurHash3 seed 9001 over Iceberg's serialization,
4096 entries) with the `ndv` property, in a Puffin file set on the snapshot. The device hashes and
sorts; the host merges the smallest hashes.

**`rewriteDataFilesSorted()`** is `rewrite_data_files(strategy => 'sort')` with the table's sort
order: identity fields, either direction and null order, and the order's id on every new file
(`sort_order_id`, as Spark's writes set it since #15150 and #15832). Each output file is sorted;
files are not sorted against each other, as with Spark's file groups. For selective compaction,
`rewriteGroup(tasks, targetFileBytes)` and `rewriteGroupSorted(...)` rewrite a file group that any
planner chose (a `where`, a delete-file threshold) and leave the commit to the caller.

## How a batch runs

1. **Plan.** `table.newScan().planFiles()` decides which delete files apply to which data file,
   by sequence number. Consecutive files that share their equality deletes form a batch of up to
   `iceberg.tornadovm.batch-rows` rows (8M by default).
2. **Prepare on the host,** ahead of the GPU: Parquet footers, and deletion vectors through
   `BaseDeleteLoader`.
3. **Run on the GPU,** in one TornadoVM execution plan a batch:
   - cuDF reads the batch's data files, its position-delete files and its equality-delete keys;
   - TornadoVM kernels fold position deletes, deletion vectors, equality hits and Iceberg's null
     rules into one live mask;
   - then one consumer runs: a cuDF group-by, a gather of live rows, a Parquet write, or the
     positions of dropped rows. A group-by on one int/long key whose Parquet footer bounds span
     at most 4096 values (a store, a month, a status code) skips cuDF's hash table: rows add
     into 32 copies of a slot array with atomics, and a second kernel sums the copies.
   Nothing returns to the host between these steps. `iceberg.tornadovm.streams` batches (4 by
   default) run at once, each on its own CUDA stream.
4. **Commit** through Iceberg: `RewriteFiles` for compaction, `RowDelta` for deletion vectors.

**Object stores** (S3, GCS, ADLS) go through the table's `FileIO`. cuDF opens files by path, so a
scan fetches only the column chunks it reads, each with one ranged read, into a sparse local copy
that cuDF opens as the original file. Delete files and rewrites fetch whole files. The cache is
`iceberg.tornadovm.cacheDir`, up to `iceberg.tornadovm.cacheBytes` (64 GB by default), least
recently used first.

## What the device takes

| | Supported | Refused with an error |
|---|---|---|
| Files | Parquet, whole files; local or through `FileIO` | other formats, split files |
| Types | int, long, date, timestamp, float, double, decimal (precision <= 18), string | nested types, decimal > 18, `time` in writes |
| Deletes | v2 position deletes, v3 deletion vectors, equality deletes on int/long keys (one or several columns) | equality deletes on string keys |
| Tables | v2 and v3, partitioned or not, evolved schemas (added columns read as null) | |
| Conversion, delete by key | v3 (deletion vectors) and v2 (file-scoped position deletes); int/long keys | partition-scoped position delete files |
| Sorted rewrite | up to 4 identity sort fields on int, long, date, timestamp, double | transforms (bucket, truncate, ...) in the sort order |

Anything refused fails with `UnsupportedOperationException`. Nothing is answered wrongly.

## Iceberg APIs it uses

All public, the same ones Iceberg's own engines use. Planning: `Table.newScan()`, `FileScanTask`,
`DeleteFile`. Commits: `RowDelta`, `RewriteFiles`. Deletes: `BaseDeleteLoader`,
`PositionDeleteIndex`, `BaseDVFileWriter`, `ContentFileUtil`. Writing: `OutputFileFactory`,
`ParquetUtil.fileMetrics`, `MetricsConfig`. It is not a File Format API model, because that API
reads one file at a time and applies deletes above it (#12774, #13433). Nothing here needs to live
in Iceberg's tree (compare #15674).

## Correctness

The tests check every device and CPU result against `IcebergGenerics`, Iceberg's own merge-on-read
reader:
- **`TestDeviceMergeOnRead`:** v2 position deletes, v3 deletion vectors, equality deletes before
  and after an append (the sequence-number rules), several key columns, nulls in keys and values,
  filters, strings, decimals, compaction with v3 row lineage, z-order, sorted rewrites (checked
  with Iceberg's `SortOrderComparators`), conversion to deletion vectors and to v2 position deletes
  (GPU and CPU twin give the same vectors), delete by key, and NDV sketches (checked against
  DataSketches' own update sketch).
- **`TestDeviceEvolutionAndStorage`:** evolved schemas, object-store tables, partitioned compaction
  and partitioned conversion.

The benchmarks print each answer's row count and checksum, and every engine must agree.

## Set up

You need an NVIDIA GPU (compute capability 7.0+) with a CUDA 13 driver, Linux x86_64 with glibc
2.34 or newer, JDK 21 and Python 3 (only to unpack NVIDIA's wheels).

```bash
# 1. JDK 21 and TornadoVM 8.1.0 (the CUDA SDK ships the cuDF shim)
sdk install java 21.0.2-open
sdk install tornadovm 8.1.0-jdk21-cuda
export JAVA_HOME=~/.sdkman/candidates/java/21.0.2-open
export TORNADOVM_HOME=~/.sdkman/candidates/tornadovm/8.1.0-jdk21-cuda

# 2. libcudf 26.08 and the CUDA 13 runtime from NVIDIA's wheels (about 1.5 GB, once; no conda,
#    no CUDA toolkit). Sets CUDF_HOME, RMM_HOME and LD_LIBRARY_PATH; source it in every shell.
source tornadovm/cudf-env.sh

# 3. Check the JDK, SDK, shim, libcudf and GPU (OK / WARN / FAIL a line)
tornadovm/preflight.sh
```

Without SDKMAN, unpack `tornadovm-8.1.0-jdk21-cuda-linux-amd64.tar.gz` from TornadoVM's release
page and point `TORNADOVM_HOME` at it. A TornadoVM built from source against CUDA 12 works too
(`make BACKEND=cuda` with `CUDF_HOME` set), with `CUDF_PACKAGE=libcudf-cu12 source
tornadovm/cudf-env.sh`.

## Quick start

The module is part of the build only when `TORNADOVM_HOME` is set.

```bash
./gradlew :iceberg-tornadovm:test                     # 38 tests, each checked against Iceberg's reader

tornadovm/run.sh gen /tmp/wh 4 1000000                # 4M rows, one table per delete layout
tornadovm/run.sh gpu /tmp/wh q2 3 none pos05 dv05 eq1m
```

`run.sh gpu` prints one CSV line a run: the table, the query, the time, the live rows and a
checksum (`run.sh cpu` runs the CPU twin, which must print the same answers):

```
table,engine,query,run,warmup,millis,live_rows,checksum,breakdown,cpu_seconds
pos05,gpu,q2,1,no,32.4,3801069,7.604363e+06,"total 32.2 ms | plan 4.5, ...
eq1m,gpu,q2,1,no,24.2,3115737,6.233407e+06,"total 24.0 ms | plan 3.4, ...
```

From your own Java code, put `iceberg-tornadovm` on the class path, start the JVM with TornadoVM's
argfile (`@$TORNADOVM_HOME/tornado-argfile`, or its `tornado-argfile.template` with
`${TORNADOVM_HOME}` expanded) and `--enable-native-access=ALL-UNNAMED`, and use
`DeviceMergeOnReadScan` as in [What it does](#what-it-does). For Spark SQL, see
[spark/README.md](spark/README.md).

The benchmarks at full size (100M rows a table) and the maintenance operations:

```bash
tornadovm/run.sh gen /data/wh 64 1562500                    # 100M rows, a table per delete layout
tornadovm/run.sh gpu /data/wh q2 6 none pos05 dv05 eq1m     # and `cpu` for the CPU twin
tornadovm/run.sh gpu /data/wh sorted 4 pos05 eq1m             # sorted rewrite; `spark` for Spark's
MAIN=org.apache.iceberg.tornadovm.bench.ConvertBench tornadovm/run.sh gpu /data/wh gpu 4 eq1m eq10m
MAIN=org.apache.iceberg.tornadovm.bench.NewOpsBench tornadovm/run.sh gpu /data/wh deletekeys 3 none 1000 1000000
MAIN=org.apache.iceberg.tornadovm.bench.NewOpsBench tornadovm/run.sh gpu /data/wh convertv2 3 eq1m eq10m
MAIN=org.apache.iceberg.tornadovm.bench.StatsBench tornadovm/run.sh gpu /data/wh id,k,v1,v2 3 none eq1m
MAIN=org.apache.iceberg.tornadovm.bench.TpcdsBench tornadovm/run.sh gpu /data/tpcds agg 4 ss_pos ss_dv ss_eq
```

TPC-DS `store_sales` in three delete layouts comes from `tpcds_partitioned_setup.py` (Spark) and
`AddPartitionedEqualityDeletes`; `TpcdsBench` runs sales by store (`agg`), equality-delete
removal (`convert`) and sorted compaction (`sorted`), and `tpcds_partitioned_bench.py` the same
through Spark. Maintenance runs report the bytes they write and are rolled back.

The Spark baselines run the same operations through Spark 3.5 and Iceberg's procedures:
`BENCH_SCRIPT=spark_delete_keys.py` (`MERGE ... WHEN MATCHED THEN DELETE`) and
`BENCH_SCRIPT=stats_bench.py` (`compute_table_stats`) with `tornadovm/run.sh spark`.

| Property | Default | |
|---|---|---|
| `iceberg.tornadovm.batch-rows` | 8M | rows a batch |
| `iceberg.tornadovm.streams` | 4 | batches in flight, one CUDA stream each |
| `iceberg.tornadovm.deleteKeys.inLimit` | 1000 | up to this many keys prune by `IN`, more by range |
| `iceberg.tornadovm.positionWriters` | 0 (auto: one per 32k positions, up to a core each) | v2 position-delete writers |
| `iceberg.tornadovm.rewriteBatchBytes` | 768 MB | decoded bytes a compaction batch holds on each stream |
| `iceberg.tornadovm.write-codec` | the table's `write.parquet.compression-codec` | `zstd`, `snappy` or `none` for the device writer |
| `iceberg.tornadovm.ablate` | none | switches off a metadata-driven choice, for measuring (`Ablation`); `dense`: always cuDF's hash group-by |
| `LIBCUDF_HOST_DECOMPRESSION` (env) | `HYBRID` in `run.sh` | the largest ZSTD pages (a big dictionary page) decompress on host threads; `OFF` keeps all on the GPU |
| `tornado.cuda.sched` | `yield` | `blocking`: waiting threads sleep; less host CPU, a little slower (TornadoVM #1277) |


## Beyond the core

- `org.apache.iceberg.tornadovm.join`: star aggregates and probe joins over the scan's grouped
  pipeline. Optional; nothing in the core depends on it.
- `tornadovm/spark`: a Spark integration (reader, aggregates, rewrite runner). Separate and
  optional ([spark/README.md](spark/README.md)).
