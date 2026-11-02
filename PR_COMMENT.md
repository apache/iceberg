Thank you for re-running the probe and confirming all cases pass! I've added comprehensive test coverage for delete files as requested.

## Tests Added

Added four test cases to `TestMergeAppend` using `newRowDelta()`:

### 1. `positionDeleteBeforePromotionCommitsWithRowDelta`
Tests position delete (v2) / deletion vector (v3-v4) built before promotion, committed via `newRowDelta().addRows().addDeletes()`.

### 2. `dataFileBeforePromotionDeleteFileAfter` 
Tests data file built before promotion, delete file (position delete v2 or deletion vector v3-v4) built after. Verifies the cross-promotion scenario.

### 3. `equalityDeleteBeforePromotionAppliedToDataAfter`
Tests equality delete built before promotion, applied to data appended after. **Includes scan planning** to catch missing `equalityFieldIds` (as you noted, equality deletes only fail during scan planning).

### 4. `bothDataAndDeleteFilesBeforePromotion`
Tests both data and delete files built before promotion, then new data added after. Comprehensive scenario covering the integer → long ClassCastException root cause.

## Test Design

- **Format version aware**: Tests use format-appropriate delete types (position deletes for v2, deletion vectors for v3-v4)
- **Metadata-only**: Following your probe pattern, these use the core API without reading actual file contents
- **Scan planning verification**: All tests call `table.newScan().planFiles()` to ensure:
  - Commit succeeds
  - Scan planning succeeds  
  - `equalityFieldIds` are preserved (critical for equality deletes)

## Helper Methods

Added three helper methods following the existing `promotionFile()` pattern:
- `promotionDeleteFile()` - Creates position deletes
- `promotionDeletionVector()` - Creates deletion vectors (v3-v4) with referenced data file
- `promotionEqualityDeleteFile()` - Creates equality deletes on the "id" field

All helpers accept the partition spec and value, enabling pre/post-promotion testing.

---

Let me know if you'd like any adjustments to the test coverage!
