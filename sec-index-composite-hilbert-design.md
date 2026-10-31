# SCALAR Composite Key Index via the HILBERT Transform — Design Note (draft)

> **v1 decision (2026-10):** implement the composite transform as **`ZORDER`**, reusing Iceberg's
> in-tree, tested `ZOrderByteUtils` (the encoding `rewrite_data_files` uses) for correct
> order-preserving per-type encoding of int/long/**varchar**. The Iceberg ecosystem's established
> space-filling curve is Z-order (no Hilbert exists in Iceberg today), so this is the lower-risk,
> in-ecosystem path. **Hilbert is a planned follow-up transform** — same composite machinery, swap
> only the interleave — once the composite build/resolve/query path is proven. Below, "Hilbert"
> reads as "the composite curve transform." A known limitation for both: the transform value is a
> single `long`, so only each column's leading bytes are retained; **per-dimension min/max scaling**
> (build-time stats) is the quality follow-up for narrow numeric domains.

Draft design for composite (multi-column) SCALAR indexes, implemented as the **HILBERT transform
function** the spec (`format/index.md`) already reserves. For review and to fold into the SCALAR
proposal / raise with the Iceberg index working group. Aligns with the 2026-09-28 sync decisions
(non-overlapping regions, sort-key-as-expressions total ordering, explicit index type).

## Goal

Index on **multiple key columns** so a predicate over those columns (`WHERE a = X AND b = Y`, or a
multi-dimensional range) can prune files. Done by deriving a single `long` **transform value** from
the N key columns via a space-filling curve, so the index keeps its existing single-scalar
machinery (tracking file ranges, non-overlapping leaf regions, residual re-check).

This promotes `HILBERT` from *reserved* to *defined* in the spec; `IDENTITY`/`HASH` remain the
single-column transforms.

## Metadata (no schema change — already supported)

- `transform-function` = `"HILBERT"`.
- `key-column-ids` = the N key columns, in order (already a `list<int>`).
- Properties (optional): `hilbert.bits-per-dimension` (default: `floor(64 / N)`).
- **Default transform** = `HILBERT` when more than one key column is given; single-column keeps
  HASH/IDENTITY.

## Leaf schema (done — steps 2a/2b)

`[key col₀ … key colₙ, transform_value, file_path, position]`, entries sorted by
`(transform_value, key values…)` → non-overlapping transform-value regions per leaf.

## Encode: N key columns → one `long` (real Hilbert)

1. **Bit budget** — split 64 bits across the N dimensions (`floor(64/N)` bits each; e.g. 2 cols →
   32, 3 cols → 21). More columns ⇒ coarser per-dimension resolution.
2. **Per-dimension normalization to unsigned bits**, order-preserving:
   - `int`/`long`: shift into unsigned range (flip sign bit), take the high `bitsPerDim` bits.
   - `string`/`binary`: take the leading bytes as an unsigned big-endian integer.
   - Only promotions that preserve ordering are safe; an order-changing promotion (e.g. `int→string`)
     is an index-breaking change requiring rebuild (per the sync's type-promotion discussion).
3. **Hilbert index** — map the N normalized coordinates to the Hilbert curve distance (Skilling's
   transpose→index algorithm). Not Z-order/Morton: Hilbert preserves locality better, so similar
   multi-column keys cluster into fewer leaf files. (Iceberg's `ZOrderByteUtils` exists but is a
   *different* curve; we do not reuse it for Hilbert correctness.)

`HilbertTransform implements TransformFunction, Serializable`, so it is captured in the Spark build
UDF closure like `HashTransform`.

## Build (step 2c)

- Select the N key columns as `__key₀…__keyₙ`.
- Compute the transform value with **`functions.struct(__key₀…__keyₙ)` + `UDF1<Row,Long>`** calling
  `transform.transform(rowToList(row))`. Struct (not `array()`) preserves each column's own type;
  Row avoids Scala `Seq` interop.
- Sort by `(transform_value, key₀…keyₙ)`; `repartitionByRange` → non-overlapping leaf regions; small
  Parquet row groups (the existing `leaf.row-group-size-bytes`).

## Query (phased)

A multi-dimensional predicate maps to a region of the N-dimensional key space, which maps to a set
of Hilbert-distance ranges.

- **Phase 1 (first cut):** a single **bounding** `[min,max]` Hilbert range over the query region.
  Correct — over-fetches leaf files; the residual predicate (applied downstream by the engine) drops
  non-matching rows. Encoding-agnostic, so correct for real Hilbert.
- **Phase 2:** precise multi-range decomposition (the query box → the disjoint set of Hilbert
  sub-ranges it covers), issuing one tracking-file probe per range, for tighter pruning.

`TransformFunction` gains `queryRanges(constraints)` so each transform owns its own decomposition
(IDENTITY = one range; HASH = one range per equality value; HILBERT = the above).

## Correctness invariants (unchanged)

- The index is **advisory** — it only reduces the file set; the engine always applies the original
  predicate, so results never depend on the index being present or precise.
- **Non-overlapping** leaf regions (spec v1): a lookup reads a bounded set of leaves.

## Status / phasing

- **Done:** `TransformFunction` abstraction; multi-column leaf data layer (`LeafFileEntry`/`Writer`/
  `Reader`).
- **Next:** `HilbertTransform.encode` + build (2c) with struct-UDF + bounding-range query.
- **Follow-up:** precise Hilbert `queryRanges` decomposition; type-promotion handling for composite
  keys.
