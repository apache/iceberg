package org.apache.iceberg;

import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public interface DeltaFile {
  static Types.StructType getType(Types.StructType partitionType) {
    // IDs start at 100 to leave room for changes to ManifestEntry
    return Types.StructType.of(
        required(100, "file_path", Types.StringType.get()),
        required(101, "file_format", Types.StringType.get()),
        required(102, "primary_key", partitionType),
        required(103, "record_count", Types.LongType.get()),
        required(104, "file_size_in_bytes", Types.LongType.get()),
        optional(105, "row_counts", Types.MapType.ofRequired(119, 120,
            Types.IntegerType.get(), Types.LongType.get())),
        optional(106, "delete_counts", Types.MapType.ofRequired(119, 120,
            Types.IntegerType.get(), Types.LongType.get())),
        optional(107, "key_metadata", Types.BinaryType.get()),
        optional(108, "split_offsets", Types.ListType.ofRequired(133, Types.LongType.get()))
        // NEXT ID TO ASSIGN: 134
    );
  }

  /**
   * @return fully qualified path to the file, suitable for constructing a Hadoop Path
   */
  CharSequence path();

  /**
   * @return format of the data file
   */
  FileFormat format();

  /**
   * @return partition data for this file as a {@link StructLike}
   */
  StructLike primaryKey();

  /**
   * @return the number of top-level records in the data file
   */
  long rowCount();

  long deleteCount();

  /**
   * @return the data file size in bytes
   */
  long fileSizeInBytes();

  /**
   * @return metadata about how this file is encrypted, or null if the file is stored in plain
   * text.
   */
  ByteBuffer keyMetadata();

  /**
   * Copies this {@link DeltaFile delta file}. Manifest readers can reuse delta file instances; use
   * this method to copy data when collecting files from tasks.
   *
   * @return a copy of this data file
   */
  DeltaFile copy();

  /**
   * Copies this {@link DataFile data file} without file stats. Manifest readers can reuse data file instances; use
   * this method to copy data without stats when collecting files.
   *
   * @return a copy of this data file, without lower bounds, upper bounds, value counts, or null value counts
   */
  DeltaFile copyWithoutStats();

  /**
   * @return List of recommended split locations, if applicable, null otherwise.
   * When available, this information is used for planning scan tasks whose boundaries
   * are determined by these offsets. The returned list must be sorted in ascending order.
   */
  List<Long> splitOffsets();
}
