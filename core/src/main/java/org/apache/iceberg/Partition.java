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

public class Partition implements IndexedRecord {
  private PartitionData partitionData;
  private int specId;
  private long dataRecordCount;
  private int dataFileCount;
  private long dataFileSizeInBytes;
  // optional fields are kept as objects instead of primitive.
  private Long posDeleteRecordCount;
  private Integer posDeleteFileCount;
  private Long eqDeleteRecordCount;
  private Integer eqDeleteFileCount;
  // Commit time of snapshot that last updated this partition
  private Long lastUpdatedAt;
  // ID of snapshot that last updated this partition
  private Long lastUpdatedSnapshotId;

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
    LAST_UPDATED_AT,
    LAST_UPDATED_SNAPSHOT_ID
  }

  public Partition() {}

  public Partition(StructLike key, Types.StructType keyType) {
    this.partitionData = toPartitionData(key, keyType);
    this.specId = 0;
    this.dataRecordCount = 0L;
    this.dataFileCount = 0;
    this.dataFileSizeInBytes = 0L;
    this.posDeleteRecordCount = 0L;
    this.posDeleteFileCount = 0;
    this.eqDeleteRecordCount = 0L;
    this.eqDeleteFileCount = 0;
  }

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

  public Long posDeleteRecordCount() {
    return posDeleteRecordCount;
  }

  public Integer posDeleteFileCount() {
    return posDeleteFileCount;
  }

  public Long eqDeleteRecordCount() {
    return eqDeleteRecordCount;
  }

  public Integer eqDeleteFileCount() {
    return eqDeleteFileCount;
  }

  public Long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public Long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  synchronized void update(ContentFile<?> file, Snapshot snapshot) {
    if (snapshot != null) {
      long snapshotCommitTime = snapshot.timestampMillis() * 1000;
      if (this.lastUpdatedAt == null || snapshotCommitTime > this.lastUpdatedAt) {
        this.lastUpdatedAt = snapshotCommitTime;
        this.lastUpdatedSnapshotId = snapshot.snapshotId();
      }
    }

    switch (file.content()) {
      case DATA:
        this.dataRecordCount += file.recordCount();
        this.dataFileCount += 1;
        this.specId = file.specId();
        this.dataFileSizeInBytes += file.fileSizeInBytes();
        break;
      case POSITION_DELETES:
        this.posDeleteRecordCount = file.recordCount();
        this.posDeleteFileCount += 1;
        this.specId = file.specId();
        break;
      case EQUALITY_DELETES:
        this.eqDeleteRecordCount = file.recordCount();
        this.eqDeleteFileCount += 1;
        this.specId = file.specId();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
    }
  }

  /** Needed because StructProjection is not serializable */
  private static PartitionData toPartitionData(StructLike key, Types.StructType keyType) {
    PartitionData data = new PartitionData(keyType);
    for (int i = 0; i < keyType.fields().size(); i++) {
      Object val = key.get(i, keyType.fields().get(i).type().typeId().javaClass());
      if (val != null) {
        data.set(i, val);
      }
    }

    return data;
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
        this.posDeleteRecordCount = (v == null) ? null : (Long) v;
        return;
      case 6:
        this.posDeleteFileCount = (v == null) ? null : (Integer) v;
        return;
      case 7:
        this.eqDeleteRecordCount = (v == null) ? null : (Long) v;
        return;
      case 8:
        this.eqDeleteFileCount = (v == null) ? null : (Integer) v;
        return;
      case 9:
        this.lastUpdatedAt = (v == null) ? null : (Long) v;
        return;
      case 10:
        this.lastUpdatedSnapshotId = (v == null) ? null : (Long) v;
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
        return lastUpdatedAt;
      case 10:
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
    } else if (!(o instanceof Partition)) {
      return false;
    }

    Partition that = (Partition) o;
    if (!(partitionData.equals(that.partitionData)
        && specId == that.specId
        && dataRecordCount == that.dataRecordCount
        && dataFileCount == that.dataFileCount
        && dataFileSizeInBytes == that.dataFileSizeInBytes)) {
      return false;
    }

    return Objects.equals(posDeleteRecordCount, that.posDeleteRecordCount)
        && Objects.equals(posDeleteFileCount, that.posDeleteFileCount)
        && Objects.equals(eqDeleteRecordCount, that.eqDeleteRecordCount)
        && Objects.equals(eqDeleteFileCount, that.eqDeleteFileCount)
        && Objects.equals(lastUpdatedAt, that.lastUpdatedAt)
        && Objects.equals(lastUpdatedSnapshotId, that.lastUpdatedSnapshotId);
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
        Types.NestedField.optional(10, Column.LAST_UPDATED_AT.name(), Types.LongType.get()),
        Types.NestedField.optional(
            11, Column.LAST_UPDATED_SNAPSHOT_ID.name(), Types.LongType.get()));
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
    private Long posDeleteRecordCount;
    private Integer posDeleteFileCount;
    private Long eqDeleteRecordCount;
    private Integer eqDeleteFileCount;
    private Long lastUpdatedAt;
    private Long lastUpdatedSnapshotId;

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

    public Builder withLastUpdatedAt(Long newLastUpdatedAt) {
      this.lastUpdatedAt = newLastUpdatedAt;
      return this;
    }

    public Builder withLastUpdatedSnapshotId(Long newLastUpdatedSnapshotId) {
      this.lastUpdatedSnapshotId = newLastUpdatedSnapshotId;
      return this;
    }

    public Partition build() {
      Partition partition = new Partition(partitionData, partitionData.getPartitionType());
      partition.specId = specId;
      partition.dataRecordCount = dataRecordCount;
      partition.dataFileCount = dataFileCount;
      partition.dataFileSizeInBytes = dataFileSizeInBytes;
      partition.posDeleteRecordCount = posDeleteRecordCount;
      partition.posDeleteFileCount = posDeleteFileCount;
      partition.eqDeleteRecordCount = eqDeleteRecordCount;
      partition.eqDeleteFileCount = eqDeleteFileCount;
      partition.lastUpdatedAt = lastUpdatedAt;
      partition.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
      return partition;
    }
  }
}
