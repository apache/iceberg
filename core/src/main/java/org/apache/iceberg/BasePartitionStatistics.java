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

import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.types.Types;

public class BasePartitionStatistics extends SupportsIndexProjection
    implements PartitionStatistics {

  private StructLike partition;
  private Integer specId;
  private Long dataRecordCount;
  private Integer dataFileCount;
  private Long totalDataFileSizeInBytes;
  private Long positionDeleteRecordCount;
  private Integer positionDeleteFileCount;
  private Long equalityDeleteRecordCount;
  private Integer equalityDeleteFileCount;
  private Long totalRecordCount; // Not calculated, as it needs scanning the data. Remains null
  private Long lastUpdatedAt;
  private Long lastUpdatedSnapshotId;
  private Integer dvCount;

  private static final int STATS_COUNT = 13;

  BasePartitionStatistics(StructLike partition, int specId) {
    super(STATS_COUNT);

    this.partition = partition;
    this.specId = specId;

    this.dataRecordCount = 0L;
    this.dataFileCount = 0;
    this.totalDataFileSizeInBytes = 0L;
    this.positionDeleteRecordCount = 0L;
    this.positionDeleteFileCount = 0;
    this.equalityDeleteRecordCount = 0L;
    this.equalityDeleteFileCount = 0;
    this.dvCount = 0;
  }

  /** Used by internal readers to instantiate this class with a projection schema. */
  BasePartitionStatistics(Types.StructType projection) {
    super(STATS_COUNT);
  }

  @Override
  public StructLike partition() {
    return partition;
  }

  @Override
  public Integer specId() {
    return specId;
  }

  @Override
  public Long dataRecordCount() {
    return dataRecordCount;
  }

  @Override
  public Integer dataFileCount() {
    return dataFileCount;
  }

  @Override
  public Long totalDataFileSizeInBytes() {
    return totalDataFileSizeInBytes;
  }

  @Override
  public Long positionDeleteRecordCount() {
    return positionDeleteRecordCount;
  }

  @Override
  public Integer positionDeleteFileCount() {
    return positionDeleteFileCount;
  }

  @Override
  public Long equalityDeleteRecordCount() {
    return equalityDeleteRecordCount;
  }

  @Override
  public Integer equalityDeleteFileCount() {
    return equalityDeleteFileCount;
  }

  @Override
  public Long totalRecords() {
    return totalRecordCount;
  }

  @Override
  public Long lastUpdatedAt() {
    return lastUpdatedAt;
  }

  @Override
  public Long lastUpdatedSnapshotId() {
    return lastUpdatedSnapshotId;
  }

  @Override
  public Integer dvCount() {
    return dvCount;
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case PARTITION_POSITION:
        return partition;
      case SPEC_ID_POSITION:
        return specId;
      case DATA_RECORD_COUNT_POSITION:
        return dataRecordCount;
      case DATA_FILE_COUNT_POSITION:
        return dataFileCount;
      case TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION:
        return totalDataFileSizeInBytes;
      case POSITION_DELETE_RECORD_COUNT_POSITION:
        return positionDeleteRecordCount;
      case POSITION_DELETE_FILE_COUNT_POSITION:
        return positionDeleteFileCount;
      case EQUALITY_DELETE_RECORD_COUNT_POSITION:
        return equalityDeleteRecordCount;
      case EQUALITY_DELETE_FILE_COUNT_POSITION:
        return equalityDeleteFileCount;
      case TOTAL_RECORD_COUNT_POSITION:
        return totalRecordCount;
      case LAST_UPDATED_AT_POSITION:
        return lastUpdatedAt;
      case LAST_UPDATED_SNAPSHOT_ID_POSITION:
        return lastUpdatedSnapshotId;
      case DV_COUNT_POSITION:
        return dvCount;
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    if (value == null) {
      return;
    }

    switch (pos) {
      case PARTITION_POSITION:
        this.partition = (StructLike) value;
        break;
      case SPEC_ID_POSITION:
        this.specId = (int) value;
        break;
      case DATA_RECORD_COUNT_POSITION:
        this.dataRecordCount = (long) value;
        break;
      case DATA_FILE_COUNT_POSITION:
        this.dataFileCount = (int) value;
        break;
      case TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION:
        this.totalDataFileSizeInBytes = (long) value;
        break;
      case POSITION_DELETE_RECORD_COUNT_POSITION:
        this.positionDeleteRecordCount = (long) value;
        break;
      case POSITION_DELETE_FILE_COUNT_POSITION:
        this.positionDeleteFileCount = (int) value;
        break;
      case EQUALITY_DELETE_RECORD_COUNT_POSITION:
        this.equalityDeleteRecordCount = (long) value;
        break;
      case EQUALITY_DELETE_FILE_COUNT_POSITION:
        this.equalityDeleteFileCount = (int) value;
        break;
      case TOTAL_RECORD_COUNT_POSITION:
        this.totalRecordCount = (Long) value;
        break;
      case LAST_UPDATED_AT_POSITION:
        this.lastUpdatedAt = (Long) value;
        break;
      case LAST_UPDATED_SNAPSHOT_ID_POSITION:
        this.lastUpdatedSnapshotId = (Long) value;
        break;
      case DV_COUNT_POSITION:
        this.dvCount = (int) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }
}
