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
import java.util.Objects;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/** Mutable {@link StructLike} implementation of {@link Tracking}. */
class TrackingStruct extends SupportsIndexProjection implements Tracking, Serializable {
  private static final Types.StructType BASE_TYPE =
      Types.StructType.of(
          Tracking.STATUS,
          Tracking.SNAPSHOT_ID,
          Tracking.SEQUENCE_NUMBER,
          Tracking.FILE_SEQUENCE_NUMBER,
          Tracking.DV_SNAPSHOT_ID,
          Tracking.FIRST_ROW_ID,
          Tracking.DELETED_POSITIONS,
          Tracking.REPLACED_POSITIONS,
          MetadataColumns.ROW_POSITION);

  private EntryStatus status = null;
  private Long snapshotId = null;
  private Long dataSequenceNumber = null;
  private Long fileSequenceNumber = null;
  private Long dvSnapshotId = null;
  private Long firstRowId = null;
  private byte[] deletedPositions = null;
  private byte[] replacedPositions = null;

  // set by manifest readers, not written to manifests
  private String manifestLocation = null;
  private long manifestPos = -1L;

  TrackingStruct(Types.StructType type) {
    super(BASE_TYPE, type);
  }

  TrackingStruct() {
    super(BASE_TYPE.fields().size());
  }

  private TrackingStruct(TrackingStruct toCopy) {
    super(toCopy);
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    this.dataSequenceNumber = toCopy.dataSequenceNumber;
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
    this.manifestLocation = toCopy.manifestLocation;
    this.manifestPos = toCopy.manifestPos;
  }

  void inheritFrom(Tracking manifestTracking) {
    if (manifestTracking != null) {
      if (snapshotId == null) {
        this.snapshotId = manifestTracking.snapshotId();
      }

      // manifests do not distinguish between data and file sequence numbers
      Preconditions.checkArgument(
          Objects.equals(
              manifestTracking.dataSequenceNumber(), manifestTracking.fileSequenceNumber()),
          "Manifest data and file sequence numbers must be equal, got %s and %s",
          manifestTracking.dataSequenceNumber(),
          manifestTracking.fileSequenceNumber());

      if (status == EntryStatus.ADDED) {
        if (dataSequenceNumber == null) {
          this.dataSequenceNumber = manifestTracking.fileSequenceNumber();
        }

        if (fileSequenceNumber == null) {
          this.fileSequenceNumber = manifestTracking.fileSequenceNumber();
        }
      }
    }
  }

  void setManifestLocation(String location) {
    this.manifestLocation = location;
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
    return dataSequenceNumber;
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

  @Override
  public String manifestLocation() {
    return manifestLocation;
  }

  @Override
  public long manifestPos() {
    return manifestPos;
  }

  @Override
  public TrackingStruct copy() {
    return new TrackingStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return status != null ? status.id() : null;
      case 1:
        return snapshotId();
      case 2:
        return dataSequenceNumber();
      case 3:
        return fileSequenceNumber();
      case 4:
        return dvSnapshotId;
      case 5:
        return firstRowId;
      case 6:
        return deletedPositions();
      case 7:
        return replacedPositions();
      case 8:
        return manifestPos;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.status = EntryStatus.fromId((Integer) value);
        break;
      case 1:
        this.snapshotId = (Long) value;
        break;
      case 2:
        this.dataSequenceNumber = (Long) value;
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
      case 8:
        this.manifestPos = (long) value;
        break;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId == null ? "null" : snapshotId)
        .add("data_sequence_number", dataSequenceNumber == null ? "null" : dataSequenceNumber)
        .add("file_sequence_number", fileSequenceNumber == null ? "null" : fileSequenceNumber)
        .add("dv_snapshot_id", dvSnapshotId == null ? "null" : dvSnapshotId)
        .add("first_row_id", firstRowId == null ? "null" : firstRowId)
        .add("deleted_positions", deletedPositions == null ? "null" : "(binary)")
        .add("replaced_positions", replacedPositions == null ? "null" : "(binary)")
        .toString();
  }
}
