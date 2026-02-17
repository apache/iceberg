/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/** Interface for partition statistics returned from a {@link PartitionStatisticsScan}. */
public interface PartitionStatistics extends StructLike {
  Types.NestedField EMPTY_PARTITION_FIELD =
      Types.NestedField.required(1, "partition", Types.StructType.of());
  Types.NestedField SPEC_ID = Types.NestedField.required(2, "spec_id", Types.IntegerType.get());
  Types.NestedField DATA_RECORD_COUNT =
      Types.NestedField.required(3, "data_record_count", Types.LongType.get());
  Types.NestedField DATA_FILE_COUNT =
      Types.NestedField.required(4, "data_file_count", Types.IntegerType.get());
  Types.NestedField TOTAL_DATA_FILE_SIZE_IN_BYTES =
      Types.NestedField.required(5, "total_data_file_size_in_bytes", Types.LongType.get());
  Types.NestedField POSITION_DELETE_RECORD_COUNT =
      Types.NestedField.optional(6, "position_delete_record_count", Types.LongType.get());
  Types.NestedField POSITION_DELETE_FILE_COUNT =
      Types.NestedField.optional(7, "position_delete_file_count", Types.IntegerType.get());
  Types.NestedField EQUALITY_DELETE_RECORD_COUNT =
      Types.NestedField.optional(8, "equality_delete_record_count", Types.LongType.get());
  Types.NestedField EQUALITY_DELETE_FILE_COUNT =
      Types.NestedField.optional(9, "equality_delete_file_count", Types.IntegerType.get());
  Types.NestedField TOTAL_RECORD_COUNT =
      Types.NestedField.optional(10, "total_record_count", Types.LongType.get());
  Types.NestedField LAST_UPDATED_AT =
      Types.NestedField.optional(11, "last_updated_at", Types.LongType.get());
  Types.NestedField LAST_UPDATED_SNAPSHOT_ID =
      Types.NestedField.optional(12, "last_updated_snapshot_id", Types.LongType.get());
  // Using default value for v3 field to support v3 reader reading file written by v2
  Types.NestedField DV_COUNT =
      Types.NestedField.required("dv_count")
          .withId(13)
          .ofType(Types.IntegerType.get())
          .withInitialDefault(Literal.of(0))
          .withWriteDefault(Literal.of(0))
          .build();

  static Schema schema(Types.StructType unifiedPartitionType, int formatVersion) {
    Preconditions.checkState(!unifiedPartitionType.fields().isEmpty(), "Table must be partitioned");
    Preconditions.checkState(formatVersion > 0, "Invalid format version: %d", formatVersion);

    if (formatVersion <= 2) {
      return v2Schema(unifiedPartitionType);
    }

    return v3Schema(unifiedPartitionType);
  }

  private static Schema v2Schema(Types.StructType unifiedPartitionType) {
    return new Schema(
        Types.NestedField.required(
            EMPTY_PARTITION_FIELD.fieldId(), EMPTY_PARTITION_FIELD.name(), unifiedPartitionType),
        SPEC_ID,
        DATA_RECORD_COUNT,
        DATA_FILE_COUNT,
        TOTAL_DATA_FILE_SIZE_IN_BYTES,
        POSITION_DELETE_RECORD_COUNT,
        POSITION_DELETE_FILE_COUNT,
        EQUALITY_DELETE_RECORD_COUNT,
        EQUALITY_DELETE_FILE_COUNT,
        TOTAL_RECORD_COUNT,
        LAST_UPDATED_AT,
        LAST_UPDATED_SNAPSHOT_ID);
  }

  private static Schema v3Schema(Types.StructType unifiedPartitionType) {
    return new Schema(
        Types.NestedField.required(
            EMPTY_PARTITION_FIELD.fieldId(), EMPTY_PARTITION_FIELD.name(), unifiedPartitionType),
        SPEC_ID,
        DATA_RECORD_COUNT,
        DATA_FILE_COUNT,
        TOTAL_DATA_FILE_SIZE_IN_BYTES,
        Types.NestedField.required(
            POSITION_DELETE_RECORD_COUNT.fieldId(),
            POSITION_DELETE_RECORD_COUNT.name(),
            Types.LongType.get()),
        Types.NestedField.required(
            POSITION_DELETE_FILE_COUNT.fieldId(),
            POSITION_DELETE_FILE_COUNT.name(),
            Types.IntegerType.get()),
        Types.NestedField.required(
            EQUALITY_DELETE_RECORD_COUNT.fieldId(),
            EQUALITY_DELETE_RECORD_COUNT.name(),
            Types.LongType.get()),
        Types.NestedField.required(
            EQUALITY_DELETE_FILE_COUNT.fieldId(),
            EQUALITY_DELETE_FILE_COUNT.name(),
            Types.IntegerType.get()),
        TOTAL_RECORD_COUNT,
        LAST_UPDATED_AT,
        LAST_UPDATED_SNAPSHOT_ID,
        DV_COUNT);
  }

  /* The positions of each statistics within the full schema of partition statistics. */
  int PARTITION_POSITION = 0;
  int SPEC_ID_POSITION = 1;
  int DATA_RECORD_COUNT_POSITION = 2;
  int DATA_FILE_COUNT_POSITION = 3;
  int TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION = 4;
  int POSITION_DELETE_RECORD_COUNT_POSITION = 5;
  int POSITION_DELETE_FILE_COUNT_POSITION = 6;
  int EQUALITY_DELETE_RECORD_COUNT_POSITION = 7;
  int EQUALITY_DELETE_FILE_COUNT_POSITION = 8;
  int TOTAL_RECORD_COUNT_POSITION = 9;
  int LAST_UPDATED_AT_POSITION = 10;
  int LAST_UPDATED_SNAPSHOT_ID_POSITION = 11;
  int DV_COUNT_POSITION = 12;

  /** Returns the partition of these partition statistics */
  StructLike partition();

  /** Returns the spec ID of the partition of these partition statistics */
  Integer specId();

  /** Returns the number of data records in the partition */
  Long dataRecordCount();

  /** Returns the number of data files in the partition */
  Integer dataFileCount();

  /** Returns the total size of data files in bytes in the partition */
  Long totalDataFileSizeInBytes();

  /**
   * Returns the number of positional delete records in the partition. Also includes dv record count
   * as per spec
   */
  Long positionDeleteRecordCount();

  /** Returns the number of positional delete files in the partition */
  Integer positionDeleteFileCount();

  /** Returns the number of equality delete records in the partition */
  Long equalityDeleteRecordCount();

  /** Returns the number of equality delete files in the partition */
  Integer equalityDeleteFileCount();

  /** Returns the total number of records in the partition */
  Long totalRecords();

  /** Returns the timestamp in milliseconds when the partition was last updated */
  Long lastUpdatedAt();

  /** Returns the ID of the snapshot that last updated this partition */
  Long lastUpdatedSnapshotId();

  /** Returns the number of delete vectors in the partition */
  Integer dvCount();
}
