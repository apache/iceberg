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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

public class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  public enum Column {
    PARTITION_DATA,
    SPEC_ID,
    DATA_RECORD_COUNT,
    DATA_FILE_COUNT,
    DATA_FILE_SIZE_IN_BYTES,
    POSITION_DELETE_RECORD_COUNT,
    POSITION_DELETE_FILE_COUNT,
    EQUALITY_DELETE_RECORD_COUNT,
    EQUALITY_DELETE_FILE_COUNT,
    TOTAL_RECORD_COUNT,
    LAST_UPDATED_AT,
    LAST_UPDATED_SNAPSHOT_ID
  }

  public static Schema schema(Types.StructType partitionType) {
    if (partitionType.fields().isEmpty()) {
      throw new IllegalArgumentException("getting schema for an unpartitioned table");
    }

    return new Schema(
        Types.NestedField.required(1, Column.PARTITION_DATA.name(), partitionType),
        Types.NestedField.required(2, Column.SPEC_ID.name(), Types.IntegerType.get()),
        Types.NestedField.required(3, Column.DATA_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.required(4, Column.DATA_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.required(5, Column.DATA_FILE_SIZE_IN_BYTES.name(), Types.LongType.get()),
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

  public static CloseableIterable<Record> fromManifest(
      Table table, ManifestFile manifest, Schema recordSchema) {
    CloseableIterable<? extends ManifestEntry<? extends ContentFile<?>>> entries =
        CloseableIterable.transform(
            ManifestFiles.open(manifest, table.io(), table.specs())
                .select(scanColumns(manifest.content())) // don't select stats columns
                .liveEntries(),
            t ->
                (ManifestEntry<? extends ContentFile<?>>)
                    // defensive copy of manifest entry without stats columns
                    t.copyWithoutStats());

    return CloseableIterable.transform(
        entries, entry -> fromManifestEntry(entry, table, recordSchema));
  }

  public static void updateRecord(Record toUpdate, Record fromRecord) {
    toUpdate.set(
        Column.SPEC_ID.ordinal(),
        Math.max(
            (int) toUpdate.get(Column.SPEC_ID.ordinal()),
            (int) fromRecord.get(Column.SPEC_ID.ordinal())));
    incrementLong(toUpdate, fromRecord, Column.DATA_RECORD_COUNT);
    incrementInt(toUpdate, fromRecord, Column.DATA_FILE_COUNT);
    incrementLong(toUpdate, fromRecord, Column.DATA_FILE_SIZE_IN_BYTES);
    checkAndIncrementLong(toUpdate, fromRecord, Column.POSITION_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toUpdate, fromRecord, Column.POSITION_DELETE_FILE_COUNT);
    checkAndIncrementLong(toUpdate, fromRecord, Column.EQUALITY_DELETE_RECORD_COUNT);
    checkAndIncrementInt(toUpdate, fromRecord, Column.EQUALITY_DELETE_FILE_COUNT);
    checkAndIncrementLong(toUpdate, fromRecord, Column.TOTAL_RECORD_COUNT);
    if (toUpdate.get(Column.LAST_UPDATED_AT.ordinal()) != null
        && fromRecord.get(Column.LAST_UPDATED_AT.ordinal()) != null
        && ((long) toUpdate.get(Column.LAST_UPDATED_AT.ordinal())
            < (long) fromRecord.get(Column.LAST_UPDATED_AT.ordinal()))) {
      toUpdate.set(
          Column.LAST_UPDATED_AT.ordinal(), fromRecord.get(Column.LAST_UPDATED_AT.ordinal()));
      toUpdate.set(
          Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(),
          fromRecord.get(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal()));
    }
  }

  public static Record partitionDataToRecord(
      Types.StructType partitionSchema, PartitionData partitionData) {
    GenericRecord genericRecord = GenericRecord.create(partitionSchema);
    for (int index = 0; index < partitionData.size(); index++) {
      genericRecord.set(index, partitionData.get(index));
    }

    return genericRecord;
  }

  private static Record fromManifestEntry(
      ManifestEntry<?> entry, Table table, Schema recordSchema) {
    GenericRecord record = GenericRecord.create(recordSchema);
    Types.StructType partitionType =
        recordSchema.findField(Column.PARTITION_DATA.name()).type().asStructType();
    PartitionData partitionData = coercedPartitionData(entry.file(), table.specs(), partitionType);
    record.set(
        Column.PARTITION_DATA.ordinal(), partitionDataToRecord(partitionType, partitionData));
    record.set(Column.SPEC_ID.ordinal(), entry.file().specId());

    Snapshot snapshot = table.snapshot(entry.snapshotId());
    if (snapshot != null) {
      record.set(Column.LAST_UPDATED_SNAPSHOT_ID.ordinal(), snapshot.snapshotId());
      record.set(Column.LAST_UPDATED_AT.ordinal(), snapshot.timestampMillis());
    }

    switch (entry.file().content()) {
      case DATA:
        record.set(Column.DATA_FILE_COUNT.ordinal(), 1);
        record.set(Column.DATA_RECORD_COUNT.ordinal(), entry.file().recordCount());
        record.set(Column.DATA_FILE_SIZE_IN_BYTES.ordinal(), entry.file().fileSizeInBytes());
        break;
      case POSITION_DELETES:
        record.set(Column.POSITION_DELETE_FILE_COUNT.ordinal(), 1);
        record.set(Column.POSITION_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        break;
      case EQUALITY_DELETES:
        record.set(Column.EQUALITY_DELETE_FILE_COUNT.ordinal(), 1);
        record.set(Column.EQUALITY_DELETE_RECORD_COUNT.ordinal(), entry.file().recordCount());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file content type: " + entry.file().content());
    }

    // TODO: optionally compute TOTAL_RECORD_COUNT based on the flag
    return record;
  }

  private static PartitionData coercedPartitionData(
      ContentFile<?> file, Map<Integer, PartitionSpec> specs, Types.StructType partitionType) {
    // keep the partition data as per the unified spec by coercing
    StructLike partition =
        PartitionUtil.coercePartition(partitionType, specs.get(file.specId()), file.partition());
    PartitionData data = new PartitionData(partitionType);
    for (int i = 0; i < partitionType.fields().size(); i++) {
      Object val = partition.get(i, partitionType.fields().get(i).type().typeId().javaClass());
      if (val != null) {
        data.set(i, val);
      }
    }
    return data;
  }

  private static List<String> scanColumns(ManifestContent content) {
    switch (content) {
      case DATA:
        return BaseScan.SCAN_COLUMNS;
      case DELETES:
        return BaseScan.DELETE_SCAN_COLUMNS;
      default:
        throw new UnsupportedOperationException("Cannot read unknown manifest type: " + content);
    }
  }

  private static void incrementLong(Record toUpdate, Record fromRecord, Column column) {
    toUpdate.set(
        column.ordinal(),
        (long) toUpdate.get(column.ordinal()) + (long) fromRecord.get(column.ordinal()));
  }

  private static void checkAndIncrementLong(Record toUpdate, Record fromRecord, Column column) {
    if (toUpdate.get(column.ordinal()) != null && fromRecord.get(column.ordinal()) != null) {
      incrementLong(toUpdate, fromRecord, column);
    }
  }

  private static void incrementInt(Record toUpdate, Record fromRecord, Column column) {
    toUpdate.set(
        column.ordinal(),
        (int) toUpdate.get(column.ordinal()) + (int) fromRecord.get(column.ordinal()));
  }

  private static void checkAndIncrementInt(Record toUpdate, Record fromRecord, Column column) {
    if (toUpdate.get(column.ordinal()) != null && fromRecord.get(column.ordinal()) != null) {
      incrementInt(toUpdate, fromRecord, column);
    }
  }
}
