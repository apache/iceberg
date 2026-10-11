# SCALAR Index — Benchmark Results

Comparison of query cost **with vs. without** the SCALAR index, across three scenarios: the
**unpartitioned** and **partitioned** layouts (single-column IDENTITY index on `id`), and a
**composite** multi-column `ZORDER` index on `(k1 bigint, k2 string)`. 10,000,000 rows, 200 data
files throughout. Numbers are representative (consistent across the hive / hadoop / rest /
spark_catalog runs); see [`scalar-index-benchmark.md`](scalar-index-benchmark.md) for setup and
methodology.

**Primary metric is records read** (the trustworthy signal from Spark task input metrics). `bytesRead`
absolute is a partial DataSourceV2 metric, but its ratio tracks records. Wall-clock is reported but
noisy and, on this `local[2]` + warm-page-cache profile, dominated by fixed per-query index-resolution
overhead — see the note at the bottom.

## Scenario 1 — Unpartitioned (actual)

Baseline = full scan (no partition pruning; interior keys defeat min/max stats).

| Query | Records read: no index → index | Reduction | Wall-clock: no index → index |
|-------|-------------------------------:|:---------:|-----------------------------:|
| `eq`  (`id = 5,000,000`)       | 10,000,000 → 50,000    | **200×** | 768 → 1222 ms |
| `in`  (3 ids)                  | 10,000,000 → 150,000   | **67×**  | 809 → 2357 ms |
| `range` (20 ids)               | 10,000,000 → 1,000,000 | **10×**  | 628 → 1552 ms |

Index build: ~26–36 s (full COW rebuild of 10M rows).

> ⚠️ **These wall-clock numbers predate the leaf row-group-size fix** (see Scenario 2). Records read
> are unaffected; the wall-clock would improve the same way (`readMatching` ~600 ms → ~7 ms) if
> re-run. Not re-measured yet.

## Scenario 2 — Partitioned by `part` (= `id % 10`), index on `id`

Baseline here **already prunes by partition** (reads ~1 partition ≈ 1,000,000 rows); the index then
prunes files **within** the surviving partition. This isolates the value the index adds *on top of*
partitioning.

> _Representative numbers (consistent across catalogs); `benchmarkPartitioned`, 20 files/partition,
> after the leaf-file row-group-size fix (1 MB row groups)._

| Query | Records read: partition-prune only → + index | Reduction (on top of partitioning) | Wall-clock: baseline → +index |
|-------|---------------------------------------------:|:----------------------------------:|:-----------------------------:|
| `eq`  (`part = 0 AND id = 5,000,000`)     | 1,000,000 → 50,075   | **~20×**  | 91 → **59 ms** (index faster) |
| `in`  (`part = 0`, 3 ids)                 | 1,000,000 → 150,301  | **~6.7×** | 98 → **83 ms** (index faster) |
| `range` (`part = 0`, 20 ids)              | 1,000,000 → 650,208  | **~1.5×** | 81 → 95 ms (≈even)            |

Index build: ~29–75 s (full COW rebuild). With the index, `eq`/`in` now **beat** the full-scan
baseline on latency even on this local, warm-cache profile; `range` is ≈even because it genuinely
reads ~650k rows (modest pruning on round-robin-scattered data). On object storage the latency gap
would be far larger.

**Planning-cost fix (the key change).** Leaf files were originally written with the default 128 MB
Parquet row group, so a ~2.5M-entry leaf was ~2 huge row groups and the sorted-key `filter()` could
not skip — `LeafFileReader.readMatching` generically decoded ~a whole row group (~1M+ entries) per
candidate leaf, ~600 ms each. Writing leaf files with **1 MB row groups** (tunable via the
`leaf.row-group-size-bytes` build option) lets a lookup skip to one small row group:

| Sub-step (per query) | Before | After |
|----------------------|-------:|------:|
| tracking `readAll` (Avro) | ~0.5 ms | ~0.5 ms |
| type-check (leaf footer open) | ~0.8 ms | ~5 ms (bigger footer; Phase 3 removes it) |
| **`readMatching`** (1 leaf) | **~600 ms** | **~7 ms** |
| **`readMatching`** (`IN`, 3 leaves) | **~1850 ms** | **~20 ms** |

## Scenario 3 — Composite ZORDER index on `(k1 bigint, k2 string)`

Multi-column key, mixed int + string — the realistic composite case. 10,000,000 rows, 200 files,
`ZORDER` transform over `(k1, k2)` with `k1 = id` (unique) and `k2 = "v" + id`. Queries constrain
**both** columns (`k1 = X AND k2 = 'vX'`); the composite resolve builds one bounding Z-range from the
predicate-box corners and re-applies the AND of per-column predicates as the exact residual filter.

| Query (`k1=X AND k2='vX'`) | Records read: no index → index | Reduction | Bytes read | Wall-clock |
|----------------------------|-------------------------------:|:---------:|-----------:|-----------:|
| `k1 = 2,500,000` | 10,000,000 → 50,000 | **200×** | 1,062,222 → 5,458 | 1468 → **73 ms** |
| `k1 = 5,000,000` | 10,000,000 → 50,000 | **200×** | 1,062,222 → 5,266 | 1136 → **128 ms** |
| `k1 = 7,500,000` | 10,000,000 → 50,000 | **200×** | 1,062,222 → 5,192 | 1153 → **107 ms** |

Index build: ~49 s (full COW rebuild). 10M → 50k is exactly one file's worth (200 files × 50k rows),
so the composite index prunes a mixed int+string key to the single data file holding the match —
matching single-column `eq`. The bounding-Z-range superset plus residual re-check give correct
results; `ZORDER` is coarse for tightly-clustered small numerics (per-dimension min/max scaling is a
follow-up), but with distinct/varchar components like this it prunes to one file.

## Reading the diff between scenarios

- **Unpartitioned**: the index does *all* the pruning (10M → as low as 50k).
- **Partitioned**: partitioning does the first cut (10M → 1M), and the index does a *second* cut
  within the partition (1M → 50k for `eq`). The index keeps paying off even when the table is
  already partitioned on a different column — the realistic production shape.
- **Range** benefits least in both because consecutive ids are round-robin-scattered across files; a
  key-clustered layout would let ranges prune further (though min/max stats would then help too).

## Planning overhead: diagnosed and fixed

Early runs showed wall-clock *regressing* with the index even though far less data was read. Timing
the three per-query planning sub-steps isolated the cause: `LeafFileReader.readMatching` was ~600 ms
per candidate leaf (tracking read and type-check were <1 ms), because leaf files used the **128 MB
default Parquet row group** — so the sorted-key `filter()` could not skip and a whole row group was
generically decoded. Writing leaf files with **1 MB row groups** cut `readMatching` to ~7 ms (1 leaf)
/ ~20 ms (3 leaves) and flipped wall-clock to a win (see Scenario 2).

Remaining, minor: the type-promotion check still opens a leaf footer per query (~5 ms now that
footers carry more row-group metadata). **Phase 3** (store the key type in index metadata, no leaf
open) or a per-snapshot resolution cache would remove it — small next to the ~600 ms already
recovered.

Still true in general: on **object storage** (cold cache, per-object latency), the records-read
reduction (10–200×) converts into an even larger wall-clock win than the local profile shows, since
the baseline full scan there is far more expensive than from a warm page cache.
