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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link Tracking}. */
class TrackingStruct implements Tracking, StructLike, Serializable {
  private static final EntryStatus[] STATUS_VALUES = EntryStatus.values();

  private EntryStatus status = EntryStatus.EXISTING;
  private Long snapshotId = null;
  private Long sequenceNumber = null;
  private Long fileSequenceNumber = null;
  private Long dvSnapshotId = null;
  private Long firstRowId = null;
  private byte[] deletedPositions = null;
  private byte[] replacedPositions = null;

  TrackingStruct(Types.StructType type) {}

  private TrackingStruct(TrackingStruct toCopy) {
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    this.sequenceNumber = toCopy.sequenceNumber;
    this.fileSequenceNumber = toCopy.fileSequenceNumber;
    this.dvSnapshotId = toCopy.dvSnapshotId;
    this.firstRowId = toCopy.firstRowId;
    this.deletedPositions =
        toCopy.deletedPositions != null
            ? Arrays.copyOf(toCopy.deletedPositions, toCopy.deletedPositions.length)
            : null;
    this.replacedPositions =
        toCopy.replacedPositions != null
            ? Arrays.copyOf(toCopy.replacedPositions, toCopy.replacedPositions.length)
            : null;
  }

  TrackingStruct copy() {
    return new TrackingStruct(this);
  }

  @Override
  public EntryStatus status() {
    return status;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long dataSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public Long fileSequenceNumber() {
    return fileSequenceNumber;
  }

  @Override
  public Long dvSnapshotId() {
    return dvSnapshotId;
  }

  @Override
  public Long firstRowId() {
    return firstRowId;
  }

  @Override
  public ByteBuffer deletedPositions() {
    return deletedPositions != null ? ByteBuffer.wrap(deletedPositions) : null;
  }

  @Override
  public ByteBuffer replacedPositions() {
    return replacedPositions != null ? ByteBuffer.wrap(replacedPositions) : null;
  }

  void setSnapshotId(Long newSnapshotId) {
    this.snapshotId = newSnapshotId;
  }

  void setSequenceNumber(Long newSequenceNumber) {
    this.sequenceNumber = newSequenceNumber;
  }

  void setFirstRowId(Long newFirstRowId) {
    this.firstRowId = newFirstRowId;
  }

  @Override
  public int size() {
    return 8;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return status.id();
      case 1:
        return snapshotId;
      case 2:
        return sequenceNumber;
      case 3:
        return fileSequenceNumber;
      case 4:
        return dvSnapshotId;
      case 5:
        return firstRowId;
      case 6:
        return deletedPositions();
      case 7:
        return replacedPositions();
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    switch (pos) {
      case 0:
        this.status = STATUS_VALUES[(Integer) value];
        break;
      case 1:
        this.snapshotId = (Long) value;
        break;
      case 2:
        this.sequenceNumber = (Long) value;
        break;
      case 3:
        this.fileSequenceNumber = (Long) value;
        break;
      case 4:
        this.dvSnapshotId = (Long) value;
        break;
      case 5:
        this.firstRowId = (Long) value;
        break;
      case 6:
        this.deletedPositions = ByteBuffers.toByteArray((ByteBuffer) value);
        break;
      case 7:
        this.replacedPositions = ByteBuffers.toByteArray((ByteBuffer) value);
        break;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }
}
