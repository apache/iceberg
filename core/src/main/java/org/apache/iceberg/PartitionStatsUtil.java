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

import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  public enum Column {
    PARTITION,
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
    LAST_UPDATED_SNAPSHOT_ID
  }

  /**
   * Generates the Partition Stats Files Schema based on a given partition type.
   *
   * <p>Note: Provide the unified partition tuple as mentioned in the spec.
   *
   * @param partitionType the struct type that defines the structure of the partition.
   * @return a schema that corresponds to the provided unified partition type.
   */
  public static Schema schema(Types.StructType partitionType) {
    Preconditions.checkState(
        !partitionType.fields().isEmpty(), "getting schema for an unpartitioned table");

    return new Schema(
        Types.NestedField.required(1, Column.PARTITION.name(), partitionType),
        Types.NestedField.required(2, Column.SPEC_ID.name(), Types.IntegerType.get()),
        Types.NestedField.required(3, Column.DATA_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.required(4, Column.DATA_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.required(
            5, Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.name(), Types.LongType.get()),
        Types.NestedField.optional(
            6, Column.POSITION_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            7, Column.POSITION_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(
            8, Column.EQUALITY_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            9, Column.EQUALITY_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(10, Column.TOTAL_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(11, Column.LAST_UPDATED_AT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            12, Column.LAST_UPDATED_SNAPSHOT_ID.name(), Types.LongType.get()));
  }

  /**
   * Creates an iterable of partition stats records from a given manifest file, using the specified
   * table and record schema.
   *
   * @param table the table from which the manifest file is derived.
   * @param manifest the manifest file containing metadata about the records.
   * @param recordSchema the schema defining the structure of the records.
   * @return a CloseableIterable of partition stats records as defined by the manifest file and
   *     record schema.
   */
  public static CloseableIterable<Record> fromManifest(
      Table table, ManifestFile manifest, Schema recordSchema) {
    Preconditions.checkState(
        !recordSchema.findField(Column.PARTITION.name()).type().asStructType().fields().isEmpty(),
        "record schema should not be unpartitioned");

    return CloseableIterable.transform(
        ManifestFiles.open(manifest, table.io(), table.specs())
            .select(BaseScan.scanColumns(manifest.content()))
            .liveEntries(),
        entry -> fromManifestEntry(entry, table, recordSchema));
  }

  /**
   * Appends statistics from one Record to another.
   *
   * @param toRecord the Record to which statistics will be appended.
   * @param fromRecord the Record from which statistics will be sourced.
   */
  public static void appendStats(Record toRecord, Record fromRecord) {
    Preconditions.checkState(toRecord != null, "Record to update cannot be null");
    Preconditions.checkState(fromRecord != null, "Record to update from cannot be null");

    toRecord.set(
        Column.SPEC_ID.ordinal(),
        Math.max(
            (int) toRecord.get(Column.SPEC_ID.ordinal()),
            (int) fromRecord.get(Column.SPEC_ID.ordinal())));
    checkAndIncrementLong(toRecord, fromRecord, Column.DATA_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.DATA_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.TOTAL_DATA_FILE_SIZE_IN_BYTES);
    checkAndIncrementLong(toRecord, fromRecord, Column.POSITION_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.POSITION_DELETE_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.EQUALITY_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toRecord, fromRecord, Column.EQUALITY_DELETE_FILE_COUNT);
    checkAndIncrementLong(toRecord, fromRecord, Column.TOTAL_RECORD_COUNT);
    if (fromRecord.get(Column.LAST_UPDATED_AT.ordinal()) != null) {
      if (toRecord.get(Column.LAST_UPDATED_AT.ordinal()) == null
          || ((long) toRecord.get(Column.LAST_UPDATED_AT.ordinal())
              < (long) fromRecord.get(Column.LAST_UPDATED_AT.ordinal()))) {
        toRecord.set(
            Column.LAST_UPDATED_AT.ordinal(), fromRecord.get(Column.LAST_UPDATED_AT.ordinal()));
        toRecord.set(
            Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(),
            fromRecord.get(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal()));
      }
    }
  }

  private static Record fromManifestEntry(
      ManifestEntry<?> entry, Table table, Schema recordSchema) {
    GenericRecord record = GenericRecord.create(recordSchema);
    Types.StructType partitionType =
        recordSchema.findField(Column.PARTITION.name()).type().asStructType();
    Record partitionData = coercedPartitionData(entry.file(), table.specs(), partitionType);
    record.set(Column.PARTITION.ordinal(), partitionData);
    record.set(Column.SPEC_ID.ordinal(), entry.file().specId());

    Snapshot snapshot = table.snapshot(entry.snapshotId());
    if (snapshot != null) {
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), snapshot.snapshotId());
      record.set(Column.LAST_UPDATED_AT.ordinal(), snapshot.timestampMillis());
    }

    switch (entry.file().content()) {
      case DATA:
        record.set(Column.DATA_RECORD_COUNT.ordinal(), entry.file().recordCount());
        record.set(Column.DATA_FILE_COUNT.ordinal(), 1);
        record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), entry.file().fileSizeInBytes());
        // default values
        record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), 0);
        record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), 0);
        break;
      case POSITION_DELETES:
        record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), 1);
        // default values
        record.set(Column.DATA_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.DATA_FILE_COUNT.ordinal(), 0);
        record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), 0L);
        record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), 0);
        break;
      case EQUALITY_DELETES:
        record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), 1);
        // default values
        record.set(Column.DATA_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.DATA_FILE_COUNT.ordinal(), 0);
        record.set(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal(), 0L);
        record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), 0L);
        record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), 0);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file content type: " + entry.file().content());
    }

    // Note: Not computing the `TOTAL_RECORD_COUNT` for now as it needs scanning the data.
    record.set(Column.TOTAL_RECORD_COUNT.ordinal(), 0L);

    return record;
  }

  private static Record coercedPartitionData(
      ContentFile<?> file, Map<Integer, PartitionSpec> specs, Types.StructType partitionType) {
    // keep the partition data as per the unified spec by coercing
    StructLike partition =
        PartitionUtil.coercePartition(partitionType, specs.get(file.specId()), file.partition());
    GenericRecord genericRecord = GenericRecord.create(partitionType);
    for (int index = 0; index < partitionType.fields().size(); index++) {
      genericRecord.set(
          index,
          partition.get(index, partitionType.fields().get(index).type().typeId().javaClass()));
    }

    return genericRecord;
  }

  private static void checkAndIncrementLong(Record toUpdate, Record fromRecord, Column column) {
    if ((fromRecord.get(column.ordinal()) != null) && (toUpdate.get(column.ordinal()) != null)) {
      toUpdate.set(
          column.ordinal(),
          toUpdate.get(column.ordinal(), Long.class)
              + fromRecord.get(column.ordinal(), Long.class));
    } else if (fromRecord.get(column.ordinal()) != null) {
      toUpdate.set(column.ordinal(), fromRecord.get(column.ordinal(), Long.class));
    }
  }

  private static void checkAndIncrementInt(Record toUpdate, Record fromRecord, Column column) {
    if ((fromRecord.get(column.ordinal()) != null) && (toUpdate.get(column.ordinal()) != null)) {
      toUpdate.set(
          column.ordinal(),
          toUpdate.get(column.ordinal(), Integer.class)
              + fromRecord.get(column.ordinal(), Integer.class));
    } else if (fromRecord.get(column.ordinal()) != null) {
      toUpdate.set(column.ordinal(), fromRecord.get(column.ordinal(), Integer.class));
    }
  }
}
