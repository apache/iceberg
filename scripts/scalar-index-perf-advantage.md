# SCALAR Index — Performance Advantage (v1 / Level 1 summary)

High-level summary of the measured advantage of the Iceberg SCALAR secondary index vs. no index.
For setup, methodology, and the full numbers see [`scalar-index-benchmark.md`](scalar-index-benchmark.md)
and [`benchmark-results.md`](benchmark-results.md).

**Setup:** synthetic 10,000,000-row Iceberg table, 200 data files, IDENTITY index on a
high-cardinality `id` column, Spark 3.5 embedded (`local[2]`, warm cache). Queries measured
before vs. after the index exists; results asserted identical (the index only changes speed).

## Headline

- **10–200× less data scanned** for high-selectivity lookups the index is built for.
- **Composite multi-column (`ZORDER`) indexes match the single-column point-lookup ratio** (200×)
  for queries constraining all key columns.
- After a leaf-file layout fix, **indexed point and `IN` lookups are faster than a full scan
  even in the worst-case local, warm-cache profile** — on object storage the gap is larger.

## Data scanned (records read: no index → with index)

| Query | Unpartitioned | On top of partitioning |
|-------|---------------|------------------------|
| `WHERE id = X` (point)        | 10,000,000 → 50,000  (**200×**) | 1,000,000 → 50,000  (**20×**) |
| `WHERE id IN (…)` (3 keys)    | 10,000,000 → 150,000 (**67×**)  | 1,000,000 → 150,000 (**6.7×**) |
| `WHERE id BETWEEN a AND b`    | 10,000,000 → 1,000,000 (**10×**) | 1,000,000 → 650,000 (**1.5×**) |

"On top of partitioning" = the realistic shape: table partitioned by one column, index on another;
the baseline already prunes by partition and the index prunes files *within* the surviving partition.

## Composite (multi-column) index — ZORDER

A multi-column SCALAR index over a mixed `(bigint, varchar)` key, `USING ZORDER` — the realistic
composite case (int + string). Queries constraining **both** columns prune to the single matching
file, matching the single-column point-lookup ratio:

| Query | Records read: no index → with index | Reduction |
|-------|--------------------------------------|-----------|
| `WHERE k1 = X AND k2 = 'vX'` | 10,000,000 → 50,000 | **200×** |

Same 10M-row / 200-file table (50,000 = one file's rows). The composite resolve builds one bounding
Z-order range from the predicate box's corners — correct because Z-order (bit-interleaving) is
monotonic under the componentwise product order — then re-applies the AND of per-column predicates as
the exact residual filter. ZORDER is coarse for tightly-clustered small numerics (per-dimension
min/max scaling is a follow-up); well-distributed or varchar components prune to one file as shown. It
reuses Iceberg's in-tree `ZOrderByteUtils`; a true Hilbert curve is a planned follow-up for better
clustering.

## Latency (median ms: no index → with index, partitioned)

| Query | No index | With index |
|-------|---------:|-----------:|
| point `=` | 91 ms | **59 ms** (faster) |
| `IN`      | 98 ms | **83 ms** (faster) |
| range     | 81 ms | 95 ms (≈even) |

Wall-clock is the noisy, environment-dependent signal; the data-scanned reduction above is the
robust one and is what converts to a larger latency win on object storage.

## The fix that unlocked it

Leaf files were written with Parquet's 128 MB default row group, so the sorted-key statistics
filter could not skip and a lookup decoded a whole row group (~600 ms per candidate leaf). Writing
leaf files with **1 MB row groups** cut per-leaf resolution **~600 ms → ~7 ms** and flipped the
index from a latency regression to a win.

Impact of the fix on the *indexed* query (before-fix → after-fix, partitioned, same queries):

| Query | `readMatching` (per leaf) | indexed wall-clock |
|-------|---------------------------|--------------------|
| `eq`    | ~600 ms → **~7 ms**  | 782 ms → **59 ms** (~13×) |
| `in`    | ~1850 ms → **~20 ms** | 2129 ms → **83 ms** (~26×) |
| `range` | ~680 ms → **~7 ms**  | 1298 ms → **95 ms** (~14×) |

(The two tables measure different things: the latency table above is *index vs no index now*; this
one is *the fix's effect on the indexed query*.)

## Caveats (honest scope)

- **File-level pruning only** — the win is "fewer files scanned," matching a Bloom-filter index's
  model. Row-position pruning (fewer rows *within* a file) is future work.
- **Local, warm cache understates the latency win** — on object storage, fewer files = far fewer
  GETs with real per-object latency.
- **Range benefit is layout-dependent** — on round-robin-scattered data a `BETWEEN` band touches
  many files; key-clustered data prunes far better.
- **Build cost** — the index is a full rebuild (~30 s for 10M rows here); scales with table size.

## Comparison framing

The point-lookup file-skip ratio (~200×) is directly comparable to a Bloom-filter skipping index.
SCALAR additionally supports **ranges** (IDENTITY) and exact `(file, position)` lookups, which a
Bloom filter cannot — the basis for going beyond file-skip in later versions.
