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

import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

public class PartitionEntry implements IndexedRecord {
  private PartitionData partitionData;
  private int specId;
  private long dataRecordCount;
  private int dataFileCount;
  private long dataFileSizeInBytes;
  private long posDeleteRecordCount;
  private int posDeleteFileCount;
  private long eqDeleteRecordCount;
  private int eqDeleteFileCount;
  // Optional accurate count of records in a partition after applying the delete files if any
  private long totalRecordCount;
  // Commit time of snapshot that last updated this partition
  private long lastUpdatedAt;
  // ID of snapshot that last updated this partition
  private long lastUpdatedSnapshotId;

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

  private PartitionEntry() {}

  public static Builder builder() {
    return new Builder();
  }

  public PartitionData partitionData() {
    return partitionData;
  }

  public int specId() {
    return specId;
  }

  public long dataRecordCount() {
    return dataRecordCount;
  }

  public int dataFileCount() {
    return dataFileCount;
  }

  public long dataFileSizeInBytes() {
    return dataFileSizeInBytes;
  }

  public long posDeleteRecordCount() {
    return posDeleteRecordCount;
  }

  public int posDeleteFileCount() {
    return posDeleteFileCount;
  }

  public long eqDeleteRecordCount() {
    return eqDeleteRecordCount;
  }

  public int eqDeleteFileCount() {
    return eqDeleteFileCount;
  }

  public long totalRecordCount() {
    return totalRecordCount;
  }

  public long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.partitionData = (PartitionData) v;
        return;
      case 1:
        this.specId = (int) v;
        return;
      case 2:
        this.dataRecordCount = (long) v;
        return;
      case 3:
        this.dataFileCount = (int) v;
        return;
      case 4:
        this.dataFileSizeInBytes = (long) v;
        return;
      case 5:
        this.posDeleteRecordCount = (long) v;
        return;
      case 6:
        this.posDeleteFileCount = (int) v;
        return;
      case 7:
        this.eqDeleteRecordCount = (long) v;
        return;
      case 8:
        this.eqDeleteFileCount = (int) v;
        return;
      case 9:
        this.totalRecordCount = (long) v;
        return;
      case 10:
        this.lastUpdatedAt = (long) v;
        return;
      case 11:
        this.lastUpdatedSnapshotId = (long) v;
        return;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return partitionData;
      case 1:
        return specId;
      case 2:
        return dataRecordCount;
      case 3:
        return dataFileCount;
      case 4:
        return dataFileSizeInBytes;
      case 5:
        return posDeleteRecordCount;
      case 6:
        return posDeleteFileCount;
      case 7:
        return eqDeleteRecordCount;
      case 8:
        return eqDeleteFileCount;
      case 9:
        return totalRecordCount;
      case 10:
        return lastUpdatedAt;
      case 11:
        return lastUpdatedSnapshotId;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public Schema getSchema() {
    return prepareAvroSchema(partitionData.getPartitionType());
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PartitionEntry)) {
      return false;
    }

    PartitionEntry that = (PartitionEntry) o;
    return partitionData.equals(that.partitionData)
        && specId == that.specId
        && dataRecordCount == that.dataRecordCount
        && dataFileCount == that.dataFileCount
        && dataFileSizeInBytes == that.dataFileSizeInBytes
        && posDeleteRecordCount == that.posDeleteRecordCount
        && posDeleteFileCount == that.posDeleteFileCount
        && eqDeleteRecordCount == that.eqDeleteRecordCount
        && eqDeleteFileCount == that.eqDeleteFileCount
        && totalRecordCount == that.totalRecordCount
        && lastUpdatedAt == that.lastUpdatedAt
        && lastUpdatedSnapshotId == that.lastUpdatedSnapshotId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partitionData,
        specId,
        dataRecordCount,
        dataFileCount,
        dataFileSizeInBytes,
        posDeleteRecordCount,
        posDeleteFileCount,
        eqDeleteRecordCount,
        eqDeleteFileCount,
        totalRecordCount,
        lastUpdatedAt,
        lastUpdatedSnapshotId);
  }

  public static org.apache.iceberg.Schema icebergSchema(Types.StructType partitionType) {
    if (partitionType.fields().isEmpty()) {
      throw new IllegalArgumentException("getting schema for an unpartitioned table");
    }

    return new org.apache.iceberg.Schema(
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

  private static Schema prepareAvroSchema(Types.StructType partitionType) {
    return AvroSchemaUtil.convert(icebergSchema(partitionType), "partitionEntry");
  }

  public static class Builder {
    private PartitionData partitionData;
    private int specId;
    private long dataRecordCount;
    private int dataFileCount;
    private long dataFileSizeInBytes;
    private long posDeleteRecordCount;
    private int posDeleteFileCount;
    private long eqDeleteRecordCount;
    private int eqDeleteFileCount;
    private long totalRecordCount;
    private long lastUpdatedAt;
    private long lastUpdatedSnapshotId;

    private Builder() {}

    public Builder withPartitionData(PartitionData newPartitionData) {
      this.partitionData = newPartitionData;
      return this;
    }

    public Builder withSpecId(int newSpecId) {
      this.specId = newSpecId;
      return this;
    }

    public Builder withDataRecordCount(long newDataRecordCount) {
      this.dataRecordCount = newDataRecordCount;
      return this;
    }

    public Builder withDataFileCount(int newDataFileCount) {
      this.dataFileCount = newDataFileCount;
      return this;
    }

    public Builder withDataFileSizeInBytes(long newDataFileSizeInBytes) {
      this.dataFileSizeInBytes = newDataFileSizeInBytes;
      return this;
    }

    public Builder withPosDeleteRecordCount(Long newPosDeleteRecordCount) {
      this.posDeleteRecordCount = newPosDeleteRecordCount;
      return this;
    }

    public Builder withPosDeleteFileCount(Integer newPosDeleteFileCount) {
      this.posDeleteFileCount = newPosDeleteFileCount;
      return this;
    }

    public Builder withEqDeleteRecordCount(Long newEqDeleteRecordCount) {
      this.eqDeleteRecordCount = newEqDeleteRecordCount;
      return this;
    }

    public Builder withEqDeleteFileCount(Integer newEqDeleteFileCount) {
      this.eqDeleteFileCount = newEqDeleteFileCount;
      return this;
    }

    public Builder withTotalRecordCount(Long newTotalRecordCount) {
      this.totalRecordCount = newTotalRecordCount;
      return this;
    }

    public Builder withLastUpdatedAt(Long newLastUpdatedAt) {
      this.lastUpdatedAt = newLastUpdatedAt;
      return this;
    }

    public Builder withLastUpdatedSnapshotId(Long newLastUpdatedSnapshotId) {
      this.lastUpdatedSnapshotId = newLastUpdatedSnapshotId;
      return this;
    }

    public PartitionEntry newInstance() {
      return new PartitionEntry();
    }

    public PartitionEntry build() {
      PartitionEntry partition = new PartitionEntry();
      partition.partitionData = partitionData;
      partition.specId = specId;
      partition.dataRecordCount = dataRecordCount;
      partition.dataFileCount = dataFileCount;
      partition.dataFileSizeInBytes = dataFileSizeInBytes;
      partition.posDeleteRecordCount = posDeleteRecordCount;
      partition.posDeleteFileCount = posDeleteFileCount;
      partition.eqDeleteRecordCount = eqDeleteRecordCount;
      partition.eqDeleteFileCount = eqDeleteFileCount;
      partition.totalRecordCount = totalRecordCount;
      partition.lastUpdatedAt = lastUpdatedAt;
      partition.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
      return partition;
    }
  }
}
