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

  /** Constructor for Java serialization. */
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

  private TrackingStruct(
      EntryStatus status,
      Long snapshotId,
      Long dataSequenceNumber,
      Long fileSequenceNumber,
      Long dvSnapshotId,
      Long firstRowId,
      byte[] deletedPositions,
      byte[] replacedPositions) {
    super(BASE_TYPE.fields().size());
    this.status = status;
    this.snapshotId = snapshotId;
    this.dataSequenceNumber = dataSequenceNumber;
    this.fileSequenceNumber = fileSequenceNumber;
    this.dvSnapshotId = dvSnapshotId;
    this.firstRowId = firstRowId;
    this.deletedPositions = deletedPositions;
    this.replacedPositions = replacedPositions;
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

  /** Creates a builder for a newly added file in the given snapshot. */
  static Builder added(long snapshotId) {
    return new Builder(snapshotId);
  }

  /** Creates a builder for an existing file based on tracking read from a manifest. */
  static Builder existing(Tracking source) {
    Preconditions.checkArgument(source != null, "Invalid source tracking: null");
    return new Builder(source);
  }

  /** Creates a builder for a deleted file in the given snapshot. */
  static Builder deleted(Tracking source, long snapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source tracking: null");
    return new Builder(EntryStatus.DELETED, source, snapshotId);
  }

  /** Creates a builder for a replaced file in the given snapshot. */
  static Builder replaced(Tracking source, long snapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source tracking: null");
    return new Builder(EntryStatus.REPLACED, source, snapshotId);
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

  static class Builder {
    private final EntryStatus status;
    private final Long snapshotId;
    private final Long dataSequenceNumber;
    private final Long fileSequenceNumber;
    private final Long firstRowId;
    private Long dvSnapshotId;
    private byte[] deletedPositions;
    private byte[] replacedPositions;

    private Builder(long snapshotId) {
      this.status = EntryStatus.ADDED;
      this.snapshotId = snapshotId;
      this.dataSequenceNumber = null;
      this.fileSequenceNumber = null;
      this.firstRowId = null;
      this.deletedPositions = null;
      this.replacedPositions = null;
    }

    private Builder(Tracking source) {
      this(EntryStatus.EXISTING, source, source.snapshotId());
    }

    private Builder(EntryStatus status, Tracking source, Long snapshotId) {
      Preconditions.checkArgument(
          source.dataSequenceNumber() != null,
          "Invalid tracking source: data sequence number is null");
      Preconditions.checkArgument(
          source.fileSequenceNumber() != null,
          "Invalid tracking source: file sequence number is null");
      checkStatus(source.status(), status);
      this.status = status;
      this.snapshotId = snapshotId;
      this.dataSequenceNumber = source.dataSequenceNumber();
      this.fileSequenceNumber = source.fileSequenceNumber();
      this.firstRowId = source.firstRowId();
      this.dvSnapshotId = source.dvSnapshotId();
      this.deletedPositions = null;
      this.replacedPositions = null;
    }

    // TODO: extend allowed transitions once MODIFIED status is added.
    private static void checkStatus(EntryStatus from, EntryStatus to) {
      Preconditions.checkArgument(from != null, "Invalid tracking source: status is null");
      Preconditions.checkArgument(
          from == EntryStatus.ADDED || from == EntryStatus.EXISTING,
          "Cannot transition from %s status",
          from);
      Preconditions.checkArgument(
          to == EntryStatus.EXISTING || to == EntryStatus.DELETED || to == EntryStatus.REPLACED,
          "Cannot transition to %s status",
          to);
      Preconditions.checkArgument(from != to, "Invalid status transition: %s -> %s", from, to);
    }

    Builder dvSnapshotId(long id) {
      Preconditions.checkState(
          status != EntryStatus.DELETED, "Cannot set dv snapshot ID on a DELETED entry");
      Preconditions.checkState(
          deletedPositions == null && replacedPositions == null,
          "Cannot set dv snapshot ID with manifest delete vector positions");
      this.dvSnapshotId = id;
      return this;
    }

    // TODO: revisit when MODIFIED status is added; MDV setters will need to handle MODIFIED.
    Builder deletedPositions(ByteBuffer positions) {
      Preconditions.checkState(
          status == EntryStatus.EXISTING, "Cannot set deleted positions on a %s entry", status);
      Preconditions.checkState(
          dvSnapshotId == null, "Cannot set deleted positions with dv snapshot ID");
      this.deletedPositions = positions != null ? ByteBuffers.toByteArray(positions) : null;
      return this;
    }

    // TODO: revisit when MODIFIED status is added; MDV setters will need to handle MODIFIED.
    Builder replacedPositions(ByteBuffer positions) {
      Preconditions.checkState(
          status == EntryStatus.EXISTING, "Cannot set replaced positions on a %s entry", status);
      Preconditions.checkState(
          dvSnapshotId == null, "Cannot set replaced positions with dv snapshot ID");
      this.replacedPositions = positions != null ? ByteBuffers.toByteArray(positions) : null;
      return this;
    }

    TrackingStruct build() {
      return new TrackingStruct(
          status,
          snapshotId,
          dataSequenceNumber,
          fileSequenceNumber,
          dvSnapshotId,
          firstRowId,
          deletedPositions,
          replacedPositions);
    }
  }
}
