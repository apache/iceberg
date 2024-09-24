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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class PartitionStats implements StructLike {

  private static final int STATS_COUNT = 12;

  private StructLike partition;
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

  public PartitionStats(StructLike partition, int specId) {
    this.partition = partition;
    this.specId = specId;
  }

  public StructLike partition() {
    return partition;
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

  public long totalDataFileSizeInBytes() {
    return totalDataFileSizeInBytes;
  }

  public long positionDeleteRecordCount() {
    return positionDeleteRecordCount;
  }

  public int positionDeleteFileCount() {
    return positionDeleteFileCount;
  }

  public long equalityDeleteRecordCount() {
    return equalityDeleteRecordCount;
  }

  public int equalityDeleteFileCount() {
    return equalityDeleteFileCount;
  }

  public long totalRecordCount() {
    return totalRecordCount;
  }

  public Long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  public Long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  /**
   * Updates the partition stats from the data/delete file.
   *
   * @param file the {@link ContentFile} from the manifest entry.
   * @param snapshot the snapshot corresponding to the live entry.
   */
  public void liveEntry(ContentFile<?> file, Snapshot snapshot) {
    Preconditions.checkArgument(specId == file.specId(), "Spec IDs must match");

    switch (file.content()) {
      case DATA:
        this.dataRecordCount += file.recordCount();
        this.dataFileCount += 1;
        this.totalDataFileSizeInBytes += file.fileSizeInBytes();
        break;
      case POSITION_DELETES:
        this.positionDeleteRecordCount += file.recordCount();
        this.positionDeleteFileCount += 1;
        break;
      case EQUALITY_DELETES:
        this.equalityDeleteRecordCount += file.recordCount();
        this.equalityDeleteFileCount += 1;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
    }

    if (snapshot != null) {
      updateSnapshotInfo(snapshot.snapshotId(), snapshot.timestampMillis());
    }

    // Note: Not computing the `TOTAL_RECORD_COUNT` for now as it needs scanning the data.
  }

  /**
   * Updates the modified time and snapshot ID for the deleted manifest entry.
   *
   * @param snapshot the snapshot corresponding to the deleted manifest entry.
   */
  public void deletedEntry(Snapshot snapshot) {
    if (snapshot != null) {
      updateSnapshotInfo(snapshot.snapshotId(), snapshot.timestampMillis());
    }
  }

  /**
   * Appends statistics from given entry to current entry.
   *
   * @param entry the entry from which statistics will be sourced.
   */
  public void appendStats(PartitionStats entry) {
    // update spec id for schema evolution scenarios
    this.specId = Math.max(specId, entry.specId);

    this.dataRecordCount += entry.dataRecordCount;
    this.dataFileCount += entry.dataFileCount;
    this.totalDataFileSizeInBytes += entry.totalDataFileSizeInBytes;
    this.positionDeleteRecordCount += entry.positionDeleteRecordCount;
    this.positionDeleteFileCount += entry.positionDeleteFileCount;
    this.equalityDeleteRecordCount += entry.equalityDeleteRecordCount;
    this.equalityDeleteFileCount += entry.equalityDeleteFileCount;
    this.totalRecordCount += entry.totalRecordCount;

    if (entry.lastUpdatedAt != null) {
      updateSnapshotInfo(entry.lastUpdatedSnapshotId, entry.lastUpdatedAt);
    }
  }

  private void updateSnapshotInfo(long snapshotId, long updatedAt) {
    if (lastUpdatedAt == null || lastUpdatedAt < updatedAt) {
      this.lastUpdatedAt = updatedAt;
      this.lastUpdatedSnapshotId = snapshotId;
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
        this.partition = (StructLike) value;
        break;
      case 1:
        this.specId = (int) value;
        break;
      case 2:
        this.dataRecordCount = (long) value;
        break;
      case 3:
        this.dataFileCount = (int) value;
        break;
      case 4:
        this.totalDataFileSizeInBytes = (long) value;
        break;
      case 5:
        // optional field as per spec, implementation initialize to 0 for counters
        this.positionDeleteRecordCount = value == null ? 0L : (long) value;
        break;
      case 6:
        // optional field as per spec, implementation initialize to 0 for counters
        this.positionDeleteFileCount = value == null ? 0 : (int) value;
        break;
      case 7:
        // optional field as per spec, implementation initialize to 0 for counters
        this.equalityDeleteRecordCount = value == null ? 0L : (long) value;
        break;
      case 8:
        // optional field as per spec, implementation initialize to 0 for counters
        this.equalityDeleteFileCount = value == null ? 0 : (int) value;
        break;
      case 9:
        // optional field as per spec, implementation initialize to 0 for counters
        this.totalRecordCount = value == null ? 0L : (long) value;
        break;
      case 10:
        this.lastUpdatedAt = (Long) value;
        break;
      case 11:
        this.lastUpdatedSnapshotId = (Long) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }
}
