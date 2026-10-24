# SCALAR Index — Performance Benchmark

How we measure the benefit of the SCALAR secondary index (query with index vs. without), what the
synthetic dataset looks like, and how to run and read the results.

## Goal

Quantify how much less data a query reads once a SCALAR index exists, versus a plain full scan. The
headline number — the fraction of data skipped for a point lookup — is directly comparable to a
Bloom-filter (BF) skipping index's file-skip number, since this build's SCALAR pruning is
file-level (see *Caveats*).

## Artifacts

| File | Runs in | Use |
|------|---------|-----|
| `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestScalarIndexBenchmark.java` | Embedded Spark via Gradle | **Primary.** No Spark install needed; exercises the local SCALAR build. |
| `scripts/benchmark_scalar_index.scala` | `spark-shell` / `spark-submit` | Alternative, for an external Spark pointed at a locally-built runtime jar. |

Both are intentionally **uncommitted / local-only**: the Gradle test writes a million rows and is
far too slow for the normal CI suite, and `inputFiles()`-based measurement in the shell script is
unreliable on Spark 3.5 (see *Caveats*).

## Synthetic dataset

A fresh table is created per run (dropped afterward):

```sql
CREATE TABLE <catalog>.<db>.<table> (
  id   bigint NOT NULL,   -- the indexed key column (unique, 0 .. NUM_ROWS-1)
  data string             -- payload, "row-<id>"
) USING iceberg;
```

| Property | Default | Why |
|----------|---------|-----|
| Rows (`NUM_ROWS`) | 10,000,000 | Scale at which the full-scan baseline cost is clearly visible |
| Data files (`NUM_FILES`) | 200 (~50k rows/file) | Something to prune *between* |
| Row → file assignment | round-robin shuffle (`repartition(NUM_FILES)`) | Scatters ids so each file's `id` **min/max spans ~the whole range**. Without this, per-file column stats would prune the baseline too, hiding the index's benefit. |
| Partitioning | none (unpartitioned) | Isolates the index effect from partition pruning |

The index built on top (one IDENTITY index serves all three query shapes below; HASH could serve
`=` and `IN` but not range):

```sql
CALL <catalog>.system.build_scalar_index(
  table     => '<table>',
  columns   => array('id'),
  transform => 'IDENTITY'
);
```

### Two scenarios

The benchmark has two `@TestTemplate` methods:

- **`benchmarkUnpartitioned`** — the table above. Isolates the index's effect with no partition
  pruning in play.
- **`benchmarkPartitioned`** — the realistic combo: partition by a low-cardinality column, index a
  high-cardinality one. Every query also constrains the partition, so the *baseline already prunes
  by partition*; the index then prunes files **within** the surviving partition — the value the
  index adds on top of partitioning.

Partitioned table:

```sql
CREATE TABLE <table> (id bigint NOT NULL, data string, part int NOT NULL)
USING iceberg
PARTITIONED BY (part)
TBLPROPERTIES ('write.distribution-mode'='none', 'write.spark.fanout.enabled'='true');
-- part = id % NUM_PARTITIONS (default 10). distribution-mode=none + fanout keep the round-robin
-- layout so each partition holds multiple files (otherwise Iceberg clusters each partition into one
-- file and there's nothing to prune within it). Queries look like:
--   WHERE part = 0 AND id = <value>
```

## Queries under test

Three query shapes, all on the indexed `id` column, all served by the one IDENTITY index:

```sql
SELECT * FROM <table> WHERE id = <half>;                       -- eq    (point lookup)
SELECT * FROM <table> WHERE id IN (<quarter>, <half>, <3/4>);  -- in    (3 keys)
SELECT * FROM <table> WHERE id BETWEEN <half> AND <half+19>;   -- range (RANGE_WIDTH ids)
```

Interior keys matter: with the scattered layout, every file's `[min,max]` straddles an interior
value, so Iceberg's per-file min/max stats **cannot** prune the baseline — the index is the only
thing that can. Boundary keys (the min/max id) are already pruned by column stats, so they do not
isolate the index's effect. (An absent key currently falls back to a full scan rather than pruning
to zero — by design.)

**Benefit scales with selectivity**, and for **range** it also depends on data layout: because the
rows are round-robin-scattered, consecutive ids land in *different* files, so a `BETWEEN` band of
*N* ids touches up to *N* files. The index still reads only those files (vs. the full table for the
baseline), but the ratio is smaller than for a single-key `=`. On data **clustered** by the key,
ranges prune to far fewer files — though there min/max stats would also help, narrowing the index's
marginal advantage. The index's clearest, layout-independent win is the point lookup.

## What is measured, and how

| Metric | Source | Role |
|--------|--------|------|
| Records read | Spark task input metrics (`SparkListener` on `inputMetrics.recordsRead`) | **Primary.** Full scan reads ~`NUM_ROWS`; indexed reads ~one file (~5k). |
| Bytes read | `inputMetrics.bytesRead` | Weights by file size; corroborates records read |
| Query wall-clock | `System.nanoTime` around `collect()`, median of `RUNS` runs **dropping the first (warmup)** | End-user-facing, but noisy |
| Index build time | `System.nanoTime` around the `CALL` | Full COW rebuild cost at this scale |

**Methodology**

- **Baseline = the same query on the same table *before* the index is built.** Deterministic and
  apples-to-apples; the only variable is the index's presence.
- **Correctness guard.** Each lookup's result set is captured before the index and compared after;
  the test fails if they differ. The index must only change *speed*, never *results*.
- **Records/bytes are the robust signals**; wall-clock is reported but treated as secondary (the
  indexed runs benefit from OS/page cache warmed by the baseline, and `local[2]` is not a
  production profile).

## How to run

Both scenarios (each runs once per configured catalog):

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) ./gradlew \
  :iceberg-spark:iceberg-spark-extensions-3.5_2.13:test \
  --tests "org.apache.iceberg.spark.extensions.TestScalarIndexBenchmark" \
  -DsparkVersions=3.5 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions=
```

Or just one scenario (recommended — each writes 10M rows per catalog, so running both × 4 catalogs
is heavy):

```bash
# ... --tests "org.apache.iceberg.spark.extensions.TestScalarIndexBenchmark.benchmarkPartitioned"
```

Results are logged at WARN and written per scenario + catalog:

```bash
cat spark/v3.5/spark-extensions/build/scalar-index-benchmark-*.txt
```

### Example report shape

```
SCALAR index query benchmark
catalog=testhive rows=10000000 files=200 transform=IDENTITY
index build: <N> ms (full COW rebuild)

query              records read b/a         bytes read b/a             median ms b/a
eq (id=5000000)    10,000,000 -> 50,000     ...                        ...
in (3 ids)         10,000,000 -> 150,000    ...                        ...
range (20 ids)     10,000,000 -> 1,000,000  ...                        ...
```

"b/a" = before → after. Records read drops in proportion to how many files the matching keys live
in: ~1 file for `=`, ~3 for `IN`, up to ~20 for the 20-id range (consecutive ids scatter across
files). The `=` ratio (~200×) is the file-skip equivalent to compare against BF.

## Caveats

- **File-level pruning only.** This build does not push row positions into the scan yet (see
  `SparkScanBuilder.tryPruneUsingScalarIndex`), so the win is "fewer files read," matching BF's
  model. Row-position pruning — where SCALAR could pull *ahead* of BF — is a follow-up.
- **`Dataset#inputFiles()` is not a usable signal on Spark 3.5** for Iceberg's DataSourceV2 scans
  (it does not reflect them), which is why we measure task input metrics rather than file counts.
  If `recordsRead`/`bytesRead` return 0 on a given setup (DSv2 occasionally doesn't populate the
  legacy input metrics), fall back to wall-clock and/or the scan's custom metrics.
- **Runs once per configured catalog** (the test extends the parameterized base), so expect the
  numbers to repeat per catalog; look at the first.
- **`local[2]`, single JVM, local filesystem** — indicative, not a production profile.

## Knobs

Edit the constants at the top of `TestScalarIndexBenchmark`:

| Constant | Meaning |
|----------|---------|
| `NUM_ROWS` | Table size (default 10M; raise for scale tests) |
| `NUM_FILES` | Data-file count (more files → higher potential skip ratio) |
| `RUNS` | Timed runs per query (first discarded as warmup) |
| `RANGE_WIDTH` | Number of consecutive ids spanned by the `BETWEEN` query |
| `queryCases()` | The query shapes/literals (`=`, `IN`, range) |

## Next steps

- Compare HASH vs IDENTITY for `=`/`IN` (HASH is the recommended point-lookup transform; its bucket
  distribution matters on skewed/non-unique keys).
- A key-clustered table variant to show range pruning at its best (and where min/max stats also
  help).
- Larger scales (100M+) to observe the full-rebuild (COW) build-time trend.
- Validation on a real-world dataset once the synthetic numbers look right.
