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

import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;

class TrackingBuilder {
  private final EntryStatus status;
  private final Long snapshotId;
  private final Long dataSequenceNumber;
  private final Long fileSequenceNumber;
  private final Long firstRowId;
  // Snapshot used to stamp dv_snapshot_id when dvUpdated() is called.
  private final long currentSnapshotId;
  private Long dvSnapshotId;
  private byte[] deletedPositions;
  private byte[] replacedPositions;

  /** Constructs a builder for a fresh ADDED entry in the given snapshot. */
  TrackingBuilder(long snapshotId) {
    this.status = EntryStatus.ADDED;
    this.snapshotId = snapshotId;
    this.currentSnapshotId = snapshotId;
    this.dataSequenceNumber = null;
    this.fileSequenceNumber = null;
    this.firstRowId = null;
    this.dvSnapshotId = null;
    this.deletedPositions = null;
    this.replacedPositions = null;
  }

  /**
   * Constructs a builder derived from {@code source} at the current snapshot.
   *
   * <p>Without MODIFIED, output status is always EXISTING and {@code currentSnapshotId} is used
   * only for {@link #dvUpdated()} stamping. When MODIFIED lands, status will be derived from the
   * source, snapshot equality, and which mutation methods are called.
   */
  // TODO: when MODIFIED is added, derive status from source + currentSnapshotId + mutations.
  TrackingBuilder(Tracking source, long currentSnapshotId) {
    validateSource(source);
    checkStatus(source.status(), EntryStatus.EXISTING);
    this.status = EntryStatus.EXISTING;
    this.snapshotId = source.snapshotId();
    this.currentSnapshotId = currentSnapshotId;
    this.dataSequenceNumber = source.dataSequenceNumber();
    this.fileSequenceNumber = source.fileSequenceNumber();
    this.firstRowId = source.firstRowId();
    this.dvSnapshotId = source.dvSnapshotId();
    this.deletedPositions = null;
    this.replacedPositions = null;
  }

  /** Stamps {@code dv_snapshot_id} with the builder's current snapshot. */
  // TODO: revisit when MODIFIED status is added; this should also flip the status to MODIFIED.
  TrackingBuilder dvUpdated() {
    // DV applies to data files; deleted/replaced positions apply to manifest files
    Preconditions.checkState(
        deletedPositions == null && replacedPositions == null,
        "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");
    this.dvSnapshotId = currentSnapshotId;
    return this;
  }

  // TODO: revisit when MODIFIED status is added; MDV setters will need to handle MODIFIED.
  TrackingBuilder deletedPositions(ByteBuffer positions) {
    Preconditions.checkState(
        status == EntryStatus.EXISTING, "Cannot set deleted positions on %s entry", status);
    // DV applies to data files; deleted positions apply to manifest files
    Preconditions.checkState(
        dvSnapshotId == null,
        "Cannot set deleted positions on a data file entry (DV snapshot ID is set)");
    this.deletedPositions = positions != null ? ByteBuffers.toByteArray(positions) : null;
    return this;
  }

  // TODO: revisit when MODIFIED status is added; MDV setters will need to handle MODIFIED.
  TrackingBuilder replacedPositions(ByteBuffer positions) {
    Preconditions.checkState(
        status == EntryStatus.EXISTING, "Cannot set replaced positions on %s entry", status);
    // DV applies to data files; replaced positions apply to manifest files
    Preconditions.checkState(
        dvSnapshotId == null,
        "Cannot set replaced positions on a data file entry (DV snapshot ID is set)");
    this.replacedPositions = positions != null ? ByteBuffers.toByteArray(positions) : null;
    return this;
  }

  Tracking build() {
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

  /** Returns a terminal (DELETED or REPLACED) tracking row derived from {@code source}. */
  static Tracking terminal(EntryStatus to, Tracking source, long currentSnapshotId) {
    validateSource(source);
    checkStatus(source.status(), to);
    return new TrackingStruct(
        to,
        currentSnapshotId,
        source.dataSequenceNumber(),
        source.fileSequenceNumber(),
        source.dvSnapshotId(),
        source.firstRowId(),
        null,
        null);
  }

  private static void validateSource(Tracking source) {
    Preconditions.checkArgument(source != null, "Invalid source tracking: null");
    Preconditions.checkArgument(
        source.dataSequenceNumber() != null,
        "Invalid tracking source: data sequence number is null");
    Preconditions.checkArgument(
        source.fileSequenceNumber() != null,
        "Invalid tracking source: file sequence number is null");
  }

  // TODO: extend allowed transitions once MODIFIED status is added.
  private static void checkStatus(EntryStatus from, EntryStatus to) {
    Preconditions.checkState(from != null, "Invalid tracking source: status is null");
    switch (from) {
      case ADDED:
        Preconditions.checkState(
            to == EntryStatus.EXISTING || to == EntryStatus.DELETED || to == EntryStatus.REPLACED,
            "Invalid status transition: ADDED -> %s (ADDED is the starting status)",
            to);
        break;
      case EXISTING:
        Preconditions.checkState(
            to == EntryStatus.EXISTING || to == EntryStatus.DELETED || to == EntryStatus.REPLACED,
            "Invalid status transition: EXISTING -> %s",
            to);
        break;
      case DELETED:
      case REPLACED:
        throw new IllegalStateException(
            String.format("Invalid status transition: %s -> %s (%s is terminal)", from, to, from));
      default:
        throw new IllegalStateException(String.format("Unknown source status: %s", from));
    }
  }
}
