# Lance Java SDK Gaps

Gaps in the Lance Java SDK that affect the iceberg-lance integration.
These should be addressed in the Lance Java SDK, not worked around in iceberg-lance.

## 1. File Length (getBytesWritten)

**Current workaround:** After closing the writer, we use `OutputFile.toInputFile().getLength()`
to get the file size (same pattern as ORC's FileAppender).

**What Lance already has:** The Rust `FileWriter::finish()` returns `Result<u64>` — the total
bytes written. But the JNI layer (`file_writer.rs`) discards this value in `closeNative`:

    Ok(_) => {}  // discards the u64

And the Java `close()` returns void.

**Proposed fix in Lance Java SDK:**
- JNI: Change `closeNative` to return `jlong` with the bytes written
- Java: Store the value in a field, add `public long getBytesWritten()` getter
- Non-breaking: `close()` stays void, `AutoCloseable` contract preserved

## 2. Column Statistics (min/max/null_count per column)

**Current state:** `LanceFileAppender.metrics()` only returns `recordCount`. All other
fields (columnSizes, valueCounts, nullValueCounts, lowerBounds, upperBounds) are null.
This means Iceberg cannot do file-level pruning on Lance data files.

**What Lance is building:** PR #5639 (Column Statistics MVP) adds per-fragment min/max,
null count, and NaN count to the Rust core. Stored as Arrow IPC in the file's global
buffer. Design discussion: https://github.com/lancedb/lance/discussions/4540

**What's missing:** PR #5639 does not touch the Java SDK. Once merged, the JNI layer
and Java SDK need new APIs to read column statistics back — something like
`LanceFileReader.getColumnStatistics()`.

**Proposed fix in Lance Java SDK (after Rust PR #5639 merges):**
- JNI: Expose column statistics from the global buffer via a new native method
- Java: Add `getColumnStatistics()` to LanceFileReader returning per-column min/max/null_count
- iceberg-lance: Map those stats to Iceberg's Metrics object using Conversions.toByteBuffer()

## 3. Split Planning (Parallel Reads)

**Current state:** `FileFormat.LANCE("lance", false)` — splittable is set to false.
`LanceFileAppender.splitOffsets()` returns null. This means each Lance file is read by
a single task. No within-file parallelism.

**Why this matters:** For MPP engines like Spark, parallelism comes from task count.
Without splits, one 512MB Lance file = one task = one core. With splits, that same file
could be 10 tasks across 10 cores.

**Approach (depends on Gap #1 — getBytesWritten):**

During writing, after each batch flush in LanceFileAppender, record the cumulative bytes
written so far (from `getBytesWritten()`) and the corresponding row number. Store this
mapping in the Lance file's schema metadata via `addSchemaMetadata()`:

    "lance:split:0" → "0"              // byte offset 0 → row 0
    "lance:split:34500000" → "50000"    // byte offset 34.5MB → row 50000
    "lance:split:69200000" → "100000"   // byte offset 69.2MB → row 100000

Write side:
- `splitOffsets()` returns the byte offsets: [0, 34500000, 69200000]
- Iceberg stores these in the DataFile manifest entry
- All Iceberg math (estimatedRowsCount, split validation) works correctly
  because these are real byte positions

Read side:
- Iceberg calls `ReadBuilder.split(start=34500000, length=34700000)`
- Our reader opens the Lance file, reads the metadata map
- Looks up byte offset 34500000 → row 50000
- Looks up next split offset 69200000 → row 100000
- Calls `readAll(projectedNames, List.of(new Range(50000, 100000)), batchSize)`
- Lance natively supports row-range reads via its page table

Changes needed:
- Lance Java SDK: expose `getBytesWritten()` (same as Gap #1)
- iceberg-lance: Change `FileFormat.LANCE("lance", true)` — set splittable to true
- iceberg-lance: LanceFileAppender — record byte-offset-to-row mapping after each flush
- iceberg-lance: LanceFileAppender.splitOffsets() — return byte offset list
- iceberg-lance: ReadBuilderWrapper.split() — store start/length, translate to row range on build()

**Why row numbers as offsets won't work:**
Iceberg's `estimatedRowsCount()` in BaseContentScanTask computes:
    length / (fileSizeInBytes - firstOffset) * recordCount
This assumes offsets and length are byte positions. Using row numbers (e.g., 50000)
with fileSizeInBytes (e.g., 50000000) gives a fraction of 0.001 instead of ~0.33,
producing wildly wrong estimates that break Spark's task scheduling.

**Why proportional byte-to-row translation won't work:**
Lance is columnar — bytes are organized by columns, not rows. The first 30% of a file's
bytes does not correspond to the first 30% of rows. Proportional math
(offset / fileSizeInBytes * totalRows) is fundamentally inaccurate for columnar formats.

## 4. Predicate Pushdown (filter)

**Current state:** `ReadBuilderWrapper.filter()` is a no-op. The filter expression is
accepted but never applied. All rows are read and filtering happens in Iceberg's residual
evaluator after the fact.

**Why this matters:** Without pushdown, Lance reads all rows from disk even when a query
has a WHERE clause that could eliminate most of them. For a query like
`SELECT * FROM t WHERE id = 42`, Lance would read all rows instead of using its scalar
index or page-level filtering.

**What Lance supports:** Lance has native predicate pushdown through its scan API:
- Scalar indexes (BTREE, BITMAP) for point lookups and range queries
- Zone map filtering at the page level
- Filter expressions passed to the reader

**What's needed in iceberg-lance:**
- Translate Iceberg's `Expression` (from `ReadBuilder.filter()`) to Lance's filter format
- Pass the translated filter to `readAll()` or use Lance's scan API with filter support
- This is purely iceberg-lance work — no Lance SDK changes needed

**Priority:** Medium. Not blocking for initial contribution since Iceberg applies residual
filters anyway (correctness is preserved). But important for production performance.

## 5. Name Mapping (Schema Evolution)

**Current state:** `ReadBuilderWrapper.withNameMapping()` is a no-op. The NameMapping
is accepted but never used.

**Why this matters:** Iceberg supports schema evolution — columns can be renamed, added,
or reordered. When reading files written with an older schema, Iceberg uses NameMapping
to resolve column identity by field ID rather than column name. Without this, reading
files written before a column rename would fail or return wrong data.

**What's needed in iceberg-lance:**
- When a NameMapping is provided, use it to resolve column names during schema conversion
  in LanceSchemaUtil
- Map Iceberg field IDs to the column names used in the Lance file, regardless of what
  the current schema calls them
- This relies on the `PARQUET:field_id` metadata that LanceSchemaUtil already writes
  into Arrow schema fields

**Priority:** Low for initial contribution. Becomes important when users start evolving
schemas on existing Lance-backed Iceberg tables.

## References

- Lance PR #5639: Column Statistics MVP — https://github.com/lancedb/lance/pull/5639
- Lance Discussion #4540: Column Statistics Design — https://github.com/lancedb/lance/discussions/4540
- Lance Issue #5857: Post-MVP Improvements — https://github.com/lancedb/lance/issues/5857
- Lance JNI file_writer.rs: closeNative discards finish() return value
- Iceberg BaseContentScanTask.java — split planning logic, estimatedRowsCount()
- Iceberg OffsetsAwareSplitScanTaskIterator.java — splits tasks by offset boundaries
- Iceberg BaseFile.java lines 551-553 — split offset validation
- Iceberg GenericReader.java — calls ReadBuilder.split(task.start(), task.length())
- Lance LanceFileReader.readAll() — supports row-range reads via Range parameter
