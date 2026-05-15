# Kafka Connect Variant Shredding Support

## Overview

This branch adds automatic variant shredding support for Apache Iceberg Kafka Connect. Variant shredding optimizes Parquet storage and query performance by extracting frequently-occurring fields from variant (JSON-like) data into typed columns.

## What is Variant Shredding?

When you store semi-structured data in Iceberg variant columns, the data is typically stored as raw bytes. Variant shredding analyzes the data distribution and extracts common fields into typed Parquet columns for:

- **Better compression**: Typed columns compress better than raw bytes
- **Faster queries**: Queries can push down filters to typed columns
- **Lower storage costs**: Reduced file sizes through better encoding

### Storage Structure

Every variant column has three fields in Parquet:

```
variant_column {
  metadata: BINARY          // Field name dictionary (always present)
  value: BINARY             // Raw variant bytes (always present - full data)
  typed_value: STRUCT {     // Shredded typed fields (optional - only if shredding enabled)
    field1 {
      value: BINARY         // Raw bytes for this field
      typed_value: INT64    // Typed extraction
    }
    field2 {
      value: BINARY
      typed_value: STRING
    }
  }
}
```

**Important:** The raw `value` field always contains complete variant data. The `typed_value` is a query optimization.

## Implementation Changes

### 1. RecordVariantShreddingAnalyzer

**File:** `data/src/main/java/org/apache/iceberg/data/RecordVariantShreddingAnalyzer.java`

New analyzer class that:
- Extracts variant values from generic `Record` objects
- Analyzes buffered rows to determine optimal shredding schemas
- Resolves column indices for Iceberg schemas

### 2. GenericFormatModels Update

**File:** `data/src/main/java/org/apache/iceberg/data/GenericFormatModels.java`

Updated Parquet format model registration to include:
- `RecordVariantShreddingAnalyzer` instance
- `GenericRecord::copy` function for safe buffering

This enables automatic variant shredding for any tool using generic `Record` types (Kafka Connect, custom writers, etc.).

## Configuration

### Enable Variant Shredding (Default: Disabled)

Variant shredding is **disabled by default** to maintain backwards compatibility. Enable it via table properties:

```properties
# Enable variant shredding
iceberg.tables.write-props.write.parquet.shred-variants=true

# Set buffer size for analysis (default: 100)
iceberg.tables.write-props.write.parquet.variant-inference-buffer-size=1000
```

### Kafka Connect Configuration Example

```properties
# Kafka Connect sink configuration
name=iceberg-sink
connector.class=org.apache.iceberg.connect.IcebergSinkConnector
topics=events

# Iceberg catalog configuration
iceberg.catalog=hadoop
iceberg.catalog.warehouse=s3://my-bucket/warehouse

# Table configuration
iceberg.tables=events.events_table

# Enable variant shredding with custom buffer size
iceberg.tables.write-props.write.parquet.shred-variants=true
iceberg.tables.write-props.write.parquet.variant-inference-buffer-size=1000
```

### Configuration Options

| Property | Default | Description |
|----------|---------|-------------|
| `write.parquet.shred-variants` | `false` | Enable/disable variant shredding |
| `write.parquet.variant-inference-buffer-size` | `100` | Number of rows to buffer for schema inference |

## How Shredding Works

### 1. Buffering Phase
- First N rows (default: 100) are buffered in memory
- Each row is copied to prevent reference issues

### 2. Analysis Phase
- After buffer fills, analyzer examines variant data:
  - Identifies frequently-occurring fields (≥10% of rows)
  - Determines most common types for each field
  - Limits to max 300 fields and 50 nesting levels

### 3. Schema Creation
- Creates Parquet `typed_value` schema for common fields
- Example: If 90% of rows have `{"user_id": 123, "action": "click"}`, creates typed columns for those fields

### 4. Write with Shredding
- Buffered rows are replayed with shredded schema
- All subsequent rows use the same schema

## Shredding Selection Rules

Fields are **automatically selected** for shredding based on:

### Inclusion Criteria (all must be true)

```
MIN_FIELD_FREQUENCY = 0.10       // Field appears in ≥10% of rows
MAX_SHREDDED_FIELDS = 300        // Max 300 fields shredded per variant
MAX_SHREDDING_DEPTH = 50         // Max 50 levels of nesting
MAX_INTERMEDIATE_FIELDS = 1000   // Max 1000 unique fields during analysis
```

### Example

Given 1000 variant rows:

```json
// Rows 1-900 (90%)
{"user_id": 123, "action": "click", "timestamp": "2026-05-15"}

// Rows 901-950 (5%)
{"user_id": 456, "action": "view", "session_id": "abc"}

// Rows 951-1000 (5%)
{"product_id": 789, "price": 99.99}
```

**Shredding Result:**
- ✅ `user_id` (90% frequency) → shredded to `typed_value.user_id`
- ✅ `action` (95% frequency) → shredded to `typed_value.action`
- ❌ `timestamp` (90% frequency but maybe type inconsistent) → raw `value` only
- ❌ `session_id` (5% < 10% threshold) → raw `value` only
- ❌ `product_id` (5%) → raw `value` only
- ❌ `price` (5%) → raw `value` only

## Performance Considerations

### Memory Impact

**Buffer Size × Concurrent Writers = Total Memory**

- Default buffer: 100 rows
- If writing to 100 partitions simultaneously: 10,000 rows in memory
- For wide schemas: significant memory pressure

### Recommendations for Large Workloads

**Small Memory Footprint (Recommended for Production):**
```properties
# Keep buffer small
iceberg.tables.write-props.write.parquet.variant-inference-buffer-size=100

# Increase file size to reduce concurrent writers
iceberg.tables.write-props.write.target-file-size-bytes=536870912  # 512MB
```

**Better Analysis (Development/Testing):**
```properties
# Larger buffer for more accurate analysis
iceberg.tables.write-props.write.parquet.variant-inference-buffer-size=5000
```

**Disable if Memory-Constrained:**
```properties
# Disable shredding entirely
iceberg.tables.write-props.write.parquet.shred-variants=false
```

### Latency Impact

- **First N rows:** Buffered in memory (no disk write)
- **After buffer fills:** Analysis + write begins
- **Added latency:** ~50-200ms per file (depends on buffer size)

For streaming workloads, this is typically acceptable as files are written over minutes/hours.

## Best Practices

### 1. Start Disabled, Enable After Understanding Your Data

```properties
# Step 1: Write sample data without shredding
write.parquet.shred-variants=false

# Step 2: Inspect actual variant schemas
# Step 3: Enable shredding with appropriate buffer
write.parquet.shred-variants=true
write.parquet.variant-inference-buffer-size=500
```

### 2. For Known Schemas: Extract Hot Fields

If you know the schema upfront, extract frequently-queried fields to typed columns:

```sql
-- Instead of pure variant:
CREATE TABLE events (
  data VARIANT  -- {"user_id": 123, "action": "click", ...}
);

-- Extract hot fields:
CREATE TABLE events (
  user_id BIGINT,        -- Always typed, always fast
  action STRING,         -- Always typed, always fast
  extra_data VARIANT     -- Only rare/dynamic fields
);
```

This gives you:
- 100% control over what's typed
- No buffering overhead
- Optimal query performance

### 3. Monitor Memory Usage

```bash
# Watch Kafka Connect worker memory
jstat -gcutil <pid> 1000

# If seeing frequent GC or OOM:
# - Reduce buffer size
# - Increase file size
# - Disable shredding
```

## Limitations

### 1. No Explicit Field Control

You **cannot** explicitly specify which fields to shred. The selection is fully automatic based on frequency analysis.

**Workaround:** Extract important fields as typed columns (see Best Practices above).

### 2. Per-File Analysis

Each Parquet file analyzes its own first N rows. If data distribution varies across files, shredding schemas may differ.

**Future Enhancement:** Global sampling with schema caching would provide consistent schemas.

### 3. Memory Overhead

Buffering requires memory proportional to: buffer_size × concurrent_writers × row_size

**Mitigation:** Keep buffer size small and increase target file size.

## Testing

### Unit Tests

The implementation includes tests in `VariantShreddingAnalyzer` that validate:
- Field frequency thresholds
- Type promotion rules
- Maximum field limits
- Nesting depth limits

### Integration Testing

To test with Kafka Connect:

1. **Create Iceberg table with variant column:**
```sql
CREATE TABLE test_variants (
  id INT,
  event_data VARIANT
) USING iceberg;
```

2. **Configure Kafka Connect with shredding enabled:**
```properties
iceberg.tables.write-props.write.parquet.shred-variants=true
iceberg.tables.write-props.write.parquet.variant-inference-buffer-size=100
```

3. **Send test data to Kafka:**
```json
{"id": 1, "event_data": {"user_id": 123, "action": "click"}}
{"id": 2, "event_data": {"user_id": 456, "action": "view"}}
```

4. **Verify shredding in Parquet files:**
```bash
parquet-tools schema <file>.parquet
```

Look for `typed_value` struct in the variant column.

## Troubleshooting

### Issue: High Memory Usage

**Symptoms:** Kafka Connect workers experiencing OOM or frequent GC pauses

**Solutions:**
1. Reduce buffer size: `write.parquet.variant-inference-buffer-size=50`
2. Increase file size: `write.target-file-size-bytes=536870912`
3. Disable shredding: `write.parquet.shred-variants=false`

### Issue: Inconsistent Query Performance

**Symptoms:** Some files query fast, others slow, even with same data pattern

**Cause:** Different files may have different shredding schemas due to data distribution

**Solutions:**
1. Increase buffer size for more representative sampling
2. Extract hot fields as typed columns for consistent performance
3. Future: Use schema hints (not yet implemented)

### Issue: Fields Not Being Shredded

**Symptoms:** Expected fields missing from `typed_value`

**Possible Causes:**
1. Field appears in <10% of buffered rows
2. More than 300 other fields have higher frequency
3. Type inconsistency across rows
4. Nesting depth exceeds 50 levels

**Diagnostics:**
```sql
-- Check variant field statistics
SELECT 
  variant_field,
  COUNT(*) as frequency
FROM 
  table 
GROUP BY 
  variant_field;
```

## Future Enhancements

### 1. Schema Hints

Allow users to pre-define expected variant schema:

```properties
write.parquet.variant-shredding-schema={"user_id":"long","action":"string"}
```

Benefits:
- Zero memory overhead (no buffering)
- Zero latency (immediate writes)
- Consistent schema across all files

### 2. Global Sampling with Schema Caching

Sample once from first N records globally, cache schema, reuse for all files:

```properties
write.parquet.variant-shredding-mode=sampled
write.parquet.variant-sample-size=10000
```

Benefits:
- One-time analysis cost
- Consistent schema across files
- Adapts to actual data

### 3. Field Whitelisting/Blacklisting

Explicit control over which fields to shred:

```properties
# Shred ONLY these fields
write.parquet.variant-shredding.include=user_id,action,timestamp

# Shred everything EXCEPT these
write.parquet.variant-shredding.exclude=rare_field1,temp_field
```

## References

- [Apache Iceberg Variant Type](https://iceberg.apache.org/docs/latest/api/)
- [Parquet Format](https://parquet.apache.org/docs/file-format/)
- [Kafka Connect](https://kafka.apache.org/documentation/#connect)

## Contributing

This is a work in progress. Contributions welcome for:
- Schema hints support
- Global sampling implementation
- Field whitelisting/blacklisting
- Performance optimizations

## License

Apache License 2.0 - See LICENSE file for details
