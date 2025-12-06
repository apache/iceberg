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

  /** Used by internal readers to instantiate this class with a projection schema. */
  public BasePartitionStatistics(Types.StructType projection) {
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
      case 0:
        return partition;
      case 1:
        return specId;
      case 2:
        return dataRecordCount;
      case 3:
        return dataFileCount;
      case 4:
        return totalDataFileSizeInBytes;
      case 5:
        return positionDeleteRecordCount;
      case 6:
        return positionDeleteFileCount;
      case 7:
        return equalityDeleteRecordCount;
      case 8:
        return equalityDeleteFileCount;
      case 9:
        return totalRecordCount;
      case 10:
        return lastUpdatedAt;
      case 11:
        return lastUpdatedSnapshotId;
      case 12:
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
        this.positionDeleteRecordCount = (long) value;
        break;
      case 6:
        this.positionDeleteFileCount = (int) value;
        break;
      case 7:
        this.equalityDeleteRecordCount = (long) value;
        break;
      case 8:
        this.equalityDeleteFileCount = (int) value;
        break;
      case 9:
        this.totalRecordCount = (Long) value;
        break;
      case 10:
        this.lastUpdatedAt = (Long) value;
        break;
      case 11:
        this.lastUpdatedSnapshotId = (Long) value;
        break;
      case 12:
        this.dvCount = (int) value;
        break;
      default:
        throw new UnsupportedOperationException("Unknown position: " + pos);
    }
  }
}
