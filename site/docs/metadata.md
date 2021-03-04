# Metadata Tables

This page describes the internal metadata tables maintained by Iceberg. Please refer to [definitions page](terms.md) for more information
on terms and definitions and the [specifications page](spec.md) for more information on Iceberg's table specification. 

| Name                                              | Description |
| --------------------------------------------------| ------------|
| [`AllDataFilesTable`](#AllDataFilesTable)         | Contains a table's valid data files as rows. A valid data file is readable from any snapshot currently tracked by the table. This table may contain duplicate rows.
| [`AllEntriesTable`](#AllEntriesTable)             | Contains a table's manifest entries as rows, for both delete and data files. Please note that this table exposes internal details, like files that have been deleted. For a table of the live data files, please use `DataFilesTable`.
| [`AllManifestsTable`](#AllManifestsTable)         | Contains a table's valid manifest files as rows. A valid manifest file is referenced from any snapshot currently tracked by the table. This table may contain duplicate rows. 
| [`DataFilesTable`](#DataFilesTable)               | Contains a table's data files as rows.
| [`HistoryTable`](#HistoryTable)                   | Contains a table's history as rows. History is based on the table's snapshot log, which logs each update to the table's current snapshot.
| [`ManifestEntriesTable`](#ManifestEntriesTable)   | Contains a table's manifest entries as rows, for both delete and data files. Please note that this table exposes internal details, like files that have been deleted. For a table of the live data files, please use `DataFilesTable`.
| [`ManifestsTable`](#ManifestsTable)               | Contains a table's manifest files as rows.
| [`PartitionsTable`](#PartitionsTable)             | Contains a table's partitions as rows.
| [`SnapshotsTable`](#SnapshotsTable)               | Contains a table's known snapshots as rows. This does not include snapshots that have been expired using [`ExpireSnapshots`](https://iceberg.apache.org/javadoc/master/org/apache/iceberg/ExpireSnapshots.html).


## Table Schema

### <a id="AllDataFilesTable"></a> 1. `AllDataFilesTable`

```json
table {
  134: content: optional int (Contents of the file: 0=data, 1=position deletes, 2=equality deletes)
  100: file_path: required string (Location URI with FS scheme)
  101: file_format: required string (File format name: avro, orc, or parquet)
  102: partition: required struct<1000: data_bucket: optional int> (Partition data tuple, schema based on the partition spec)
  103: record_count: required long (Number of records in the file)
  104: file_size_in_bytes: required long (Total file size in bytes)
  108: column_sizes: optional map<int, long> (Map of column id to total size on disk)
  109: value_counts: optional map<int, long> (Map of column id to total count, including null and NaN)
  110: null_value_counts: optional map<int, long> (Map of column id to null value count)
  137: nan_value_counts: optional map<int, long> (Map of column id to number of NaN values in the column)
  125: lower_bounds: optional map<int, binary> (Map of column id to lower bound)
  128: upper_bounds: optional map<int, binary> (Map of column id to upper bound)
  131: key_metadata: optional binary (Encryption key metadata blob)
  132: split_offsets: optional list<long> (Splittable offsets)
  135: equality_ids: optional list<int> (Equality comparison field IDs)
  140: sort_order_id: optional int (Sort order ID)
}
```

### <a id="AllEntriesTable"></a> 2. `AllEntriesTable`

```json
table {
  0: status: required int
  1: snapshot_id: optional long
  3: sequence_number: optional long
  2: data_file: required struct<
        134: content: optional int (Contents of the file: 0=data, 1=position deletes, 2=equality deletes),
        100: file_path: required string (Location URI with FS scheme),
        101: file_format: required string (File format name: avro, orc, or parquet),
        102: partition: required struct<1000: data_bucket: optional int> (Partition data tuple, schema based on the partition spec),
            103: record_count: required long (Number of records in the file),
            104: file_size_in_bytes: required long (Total file size in bytes),
            108: column_sizes: optional map<int, long> (Map of column id to total size on disk),
            109: value_counts: optional map<int, long> (Map of column id to total count, including null and NaN),
            110: null_value_counts: optional map<int, long> (Map of column id to null value count),
            137: nan_value_counts: optional map<int, long> (Map of column id to number of NaN values in the column),
            125: lower_bounds: optional map<int, binary> (Map of column id to lower bound),
            128: upper_bounds: optional map<int, binary> (Map of column id to upper bound),
            131: key_metadata: optional binary (Encryption key metadata blob),
            132: split_offsets: optional list<long> (Splittable offsets),
            135: equality_ids: optional list<int> (Equality comparison field IDs),
            140: sort_order_id: optional int (Sort order ID)
    >
}
```

### <a id="AllManifestsTable"></a> 3. `AllManifestsTable`

```json
table {
    1: path: required string
    2: length: required long
    3: partition_spec_id: optional int
    4: added_snapshot_id: optional long
    5: added_data_files_count: optional int
    6: existing_data_files_count: optional int
    7: deleted_data_files_count: optional int
    8: partition_summaries: optional list<
        struct<
            10: contains_null: required boolean,
            11: contains_nan: required boolean,
            12: lower_bound: optional string,
            13: upper_bound: optional string
        >
    >
}
```

### <a id="DataFilesTable"></a> 4. `DataFilesTable`

```json
table {
  134: content: optional int (Contents of the file: 0=data, 1=position deletes, 2=equality deletes)
  100: file_path: required string (Location URI with FS scheme)
  101: file_format: required string (File format name: avro, orc, or parquet)
  102: partition: required struct<1000: data_bucket: optional int> (Partition data tuple, schema based on the partition spec)
  103: record_count: required long (Number of records in the file)
  104: file_size_in_bytes: required long (Total file size in bytes)
  108: column_sizes: optional map<int, long> (Map of column id to total size on disk)
  109: value_counts: optional map<int, long> (Map of column id to total count, including null and NaN)
  110: null_value_counts: optional map<int, long> (Map of column id to null value count)
  137: nan_value_counts: optional map<int, long> (Map of column id to number of NaN values in the column)
  125: lower_bounds: optional map<int, binary> (Map of column id to lower bound)
  128: upper_bounds: optional map<int, binary> (Map of column id to upper bound)
  131: key_metadata: optional binary (Encryption key metadata blob)
  132: split_offsets: optional list<long> (Splittable offsets)
  135: equality_ids: optional list<int> (Equality comparison field IDs)
  140: sort_order_id: optional int (Sort order ID)
}
```

### <a id="HistoryTable"></a> 5. `HistoryTable`

```java
private static final Schema HISTORY_SCHEMA = new Schema(
    Types.NestedField.required(1, "made_current_at", Types.TimestampType.withZone()),
    Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
    Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
    Types.NestedField.required(4, "is_current_ancestor", Types.BooleanType.get())
);
```

### <a id="ManifestEntriesTable"></a> 6. `ManifestEntriesTable`

```json
table {
    0: status: required int
    1: snapshot_id: optional long
    3: sequence_number: optional long
    2: data_file: required struct<
        134: content: optional int (Contents of the file: 0=data, 1=position deletes, 2=equality deletes),
        100: file_path: required string (Location URI with FS scheme),
        101: file_format: required string (File format name: avro, orc, or parquet),
        102: partition: required struct<1000: data_bucket: optional int> (Partition data tuple, schema based on the partition spec),
            103: record_count: required long (Number of records in the file),
            104: file_size_in_bytes: required long (Total file size in bytes),
            108: column_sizes: optional map<int, long> (Map of column id to total size on disk),
            109: value_counts: optional map<int, long> (Map of column id to total count, including null and NaN),
            110: null_value_counts: optional map<int, long> (Map of column id to null value count),
            137: nan_value_counts: optional map<int, long> (Map of column id to number of NaN values in the column),
            125: lower_bounds: optional map<int, binary> (Map of column id to lower bound),
            128: upper_bounds: optional map<int, binary> (Map of column id to upper bound),
            131: key_metadata: optional binary (Encryption key metadata blob),
            132: split_offsets: optional list<long> (Splittable offsets),
            135: equality_ids: optional list<int> (Equality comparison field IDs),
            140: sort_order_id: optional int (Sort order ID)
    >
}
```

### <a id="ManifestsTable"></a> 7. `ManifestsTable`

```json
table {
    1: path: required string
    2: length: required long
    3: partition_spec_id: required int
    4: added_snapshot_id: required long
    5: added_data_files_count: required int
    6: existing_data_files_count: required int
    7: deleted_data_files_count: required int
    8: partition_summaries: required list<
        struct<
            10: contains_null: required boolean,
            11: contains_nan: required boolean,
            12: lower_bound: optional string,
            13: upper_bound: optional string
        >
    >
}
```

### <a id="PartitionsTable"></a> 8. `PartitionsTable`

```java
this.schema = new Schema(
    Types.NestedField.required(1, "partition", table.spec().partitionType()),
    Types.NestedField.required(2, "record_count", Types.LongType.get()),
    Types.NestedField.required(3, "file_count", Types.IntegerType.get())
);
```

### <a id="SnapshotsTable"></a> 9. `SnapshotsTable`

```java
private static final Schema SNAPSHOT_SCHEMA = new Schema(
    Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()),
    Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
    Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
    Types.NestedField.optional(4, "operation", Types.StringType.get()),
    Types.NestedField.optional(5, "manifest_list", Types.StringType.get()),
    Types.NestedField.optional(6, "summary",
        Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.StringType.get()))
);
```
