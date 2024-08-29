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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class PartitionStats implements StructLike {

  private Record partition; // PartitionData as Record
  private int specId;
  private long dataRecordCount;
  private int dataFileCount;
  private long totalDataFileSizeInBytes;
  private long positionDeleteRecordCount;
  private int positionDeleteFileCount;
  private long equalityDeleteRecordCount;
  private int equalityDeleteFileCount;
  private long totalRecordCount;
  private Long lastUpdatedAt; // null by default
  private Long lastUpdatedSnapshotId; // null by default

  private static final int STATS_COUNT = 12;

  public PartitionStats(Record partition) {
    this.partition = partition;
  }

  public Record partition() {
    return partition;
  }

  public int specId() {
    return specId;
  }

  public void setSpecId(int specId) {
    this.specId = specId;
  }

  public long dataRecordCount() {
    return dataRecordCount;
  }

  public void setDataRecordCount(long dataRecordCount) {
    this.dataRecordCount = dataRecordCount;
  }

  public int dataFileCount() {
    return dataFileCount;
  }

  public void setDataFileCount(int dataFileCount) {
    this.dataFileCount = dataFileCount;
  }

  public long totalDataFileSizeInBytes() {
    return totalDataFileSizeInBytes;
  }

  public void setTotalDataFileSizeInBytes(long totalDataFileSizeInBytes) {
    this.totalDataFileSizeInBytes = totalDataFileSizeInBytes;
  }

  public long positionDeleteRecordCount() {
    return positionDeleteRecordCount;
  }

  public void setPositionDeleteRecordCount(long positionDeleteRecordCount) {
    this.positionDeleteRecordCount = positionDeleteRecordCount;
  }

  public int positionDeleteFileCount() {
    return positionDeleteFileCount;
  }

  public void setPositionDeleteFileCount(int positionDeleteFileCount) {
    this.positionDeleteFileCount = positionDeleteFileCount;
  }

  public long equalityDeleteRecordCount() {
    return equalityDeleteRecordCount;
  }

  public void setEqualityDeleteRecordCount(long equalityDeleteRecordCount) {
    this.equalityDeleteRecordCount = equalityDeleteRecordCount;
  }

  public int equalityDeleteFileCount() {
    return equalityDeleteFileCount;
  }

  public void setEqualityDeleteFileCount(int equalityDeleteFileCount) {
    this.equalityDeleteFileCount = equalityDeleteFileCount;
  }

  public long totalRecordCount() {
    return totalRecordCount;
  }

  public void setTotalRecordCount(long totalRecordCount) {
    this.totalRecordCount = totalRecordCount;
  }

  public Long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public void setLastUpdatedAt(Long lastUpdatedAt) {
    this.lastUpdatedAt = lastUpdatedAt;
  }

  public Long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  public void setLastUpdatedSnapshotId(Long lastUpdatedSnapshotId) {
    this.lastUpdatedSnapshotId = lastUpdatedSnapshotId;
  }

  /**
   * Updates the partition stats from the data/delete file.
   *
   * @param file the ContentFile from the manifest entry.
   * @param snapshot the snapshot corresponding to the live entry.
   */
  public void liveEntry(ContentFile<?> file, Snapshot snapshot) {
    Preconditions.checkState(file != null, "content file cannot be null");

    specId = file.specId();

    switch (file.content()) {
      case DATA:
        dataRecordCount = file.recordCount();
        dataFileCount = 1;
        totalDataFileSizeInBytes = file.fileSizeInBytes();
        break;
      case POSITION_DELETES:
        positionDeleteRecordCount = file.recordCount();
        positionDeleteFileCount = 1;
        break;
      case EQUALITY_DELETES:
        equalityDeleteRecordCount = file.recordCount();
        equalityDeleteFileCount = 1;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
    }

    if (snapshot != null) {
      lastUpdatedSnapshotId = snapshot.snapshotId();
      lastUpdatedAt = snapshot.timestampMillis();
    }

    // Note: Not computing the `TOTAL_RECORD_COUNT` for now as it needs scanning the data.
  }

  /**
   * Updates the modified time and snapshot ID for the deleted manifest entry.
   *
   * @param snapshot the snapshot corresponding to the deleted manifest entry.
   */
  public void deletedEntry(Snapshot snapshot) {
    if (snapshot != null && lastUpdatedAt != null && snapshot.timestampMillis() > lastUpdatedAt) {
      lastUpdatedAt = snapshot.timestampMillis();
      lastUpdatedSnapshotId = snapshot.snapshotId();
    }
  }

  /**
   * Appends statistics from given entry to current entry.
   *
   * @param entry the entry from which statistics will be sourced.
   */
  public void appendStats(PartitionStats entry) {
    Preconditions.checkState(entry != null, "entry to update from cannot be null");

    specId = Math.max(specId, entry.specId);
    dataRecordCount += entry.dataRecordCount;
    dataFileCount += entry.dataFileCount;
    totalDataFileSizeInBytes += entry.totalDataFileSizeInBytes;
    positionDeleteRecordCount += entry.positionDeleteRecordCount;
    positionDeleteFileCount += entry.positionDeleteFileCount;
    equalityDeleteRecordCount += entry.equalityDeleteRecordCount;
    equalityDeleteFileCount += entry.equalityDeleteFileCount;
    totalRecordCount += entry.totalRecordCount;

    if (entry.lastUpdatedAt != null) {
      if (lastUpdatedAt == null || (lastUpdatedAt < entry.lastUpdatedAt)) {
        lastUpdatedAt = entry.lastUpdatedAt;
        lastUpdatedSnapshotId = entry.lastUpdatedSnapshotId;
      }
    }
  }

  @Override
  public int size() {
    return STATS_COUNT;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    switch (pos) {
      case 0:
        return javaClass.cast(partition);
      case 1:
        return javaClass.cast(specId);
      case 2:
        return javaClass.cast(dataRecordCount);
      case 3:
        return javaClass.cast(dataFileCount);
      case 4:
        return javaClass.cast(totalDataFileSizeInBytes);
      case 5:
        return javaClass.cast(positionDeleteRecordCount);
      case 6:
        return javaClass.cast(positionDeleteFileCount);
      case 7:
        return javaClass.cast(equalityDeleteRecordCount);
      case 8:
        return javaClass.cast(equalityDeleteFileCount);
      case 9:
        return javaClass.cast(totalRecordCount);
      case 10:
        return javaClass.cast(lastUpdatedAt);
      case 11:
        return javaClass.cast(lastUpdatedSnapshotId);
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    switch (pos) {
      case 0:
        partition = (Record) value;
        break;
      case 1:
        specId = (int) value;
        break;
      case 2:
        dataRecordCount = (long) value;
        break;
      case 3:
        dataFileCount = (int) value;
        break;
      case 4:
        totalDataFileSizeInBytes = (long) value;
        break;
      case 5:
        // optional field as per spec, implementation initialize to 0 for counters
        positionDeleteRecordCount = value == null ? 0L : (long) value;
        break;
      case 6:
        // optional field as per spec, implementation initialize to 0 for counters
        positionDeleteFileCount = value == null ? 0 : (int) value;
        break;
      case 7:
        // optional field as per spec, implementation initialize to 0 for counters
        equalityDeleteRecordCount = value == null ? 0L : (long) value;
        break;
      case 8:
        // optional field as per spec, implementation initialize to 0 for counters
        equalityDeleteFileCount = value == null ? 0 : (int) value;
        break;
      case 9:
        // optional field as per spec, implementation initialize to 0 for counters
        totalRecordCount = value == null ? 0L : (long) value;
        break;
      case 10:
        lastUpdatedAt = (Long) value;
        break;
      case 11:
        lastUpdatedSnapshotId = (Long) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PartitionStats)) {
      return false;
    }

    PartitionStats that = (PartitionStats) other;
    return Objects.equals(partition, that.partition)
        && specId == that.specId
        && dataRecordCount == that.dataRecordCount
        && dataFileCount == that.dataFileCount
        && totalDataFileSizeInBytes == that.totalDataFileSizeInBytes
        && positionDeleteRecordCount == that.positionDeleteRecordCount
        && positionDeleteFileCount == that.positionDeleteFileCount
        && equalityDeleteRecordCount == that.equalityDeleteRecordCount
        && equalityDeleteFileCount == that.equalityDeleteFileCount
        && totalRecordCount == that.totalRecordCount
        && Objects.equals(lastUpdatedAt, that.lastUpdatedAt)
        && Objects.equals(lastUpdatedSnapshotId, that.lastUpdatedSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        partition,
        specId,
        dataRecordCount,
        dataFileCount,
        totalDataFileSizeInBytes,
        positionDeleteRecordCount,
        positionDeleteFileCount,
        equalityDeleteRecordCount,
        equalityDeleteFileCount,
        totalRecordCount,
        lastUpdatedAt,
        lastUpdatedSnapshotId);
  }
}
