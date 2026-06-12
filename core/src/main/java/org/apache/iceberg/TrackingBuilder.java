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
  private final long newSnapshotId;
  private final Long snapshotId;
  private final Long dataSequenceNumber;
  private final Long fileSequenceNumber;
  private final Long firstRowId;
  private EntryStatus status;
  private Long dvSnapshotId;
  private byte[] deletedPositions;
  private byte[] replacedPositions;

  /**
   * Creates a builder for a newly added file.
   *
   * @param newSnapshotId the snapshot ID in which the new tracking instance will be committed
   */
  static TrackingBuilder added(long newSnapshotId) {
    return new TrackingBuilder(newSnapshotId);
  }

  /**
   * Creates a builder for a tracking row derived from {@code source}.
   *
   * @param source source tracking from a manifest entry
   * @param newSnapshotId the snapshot ID in which the new tracking instance will be committed
   */
  static TrackingBuilder from(Tracking source, long newSnapshotId) {
    return new TrackingBuilder(source, newSnapshotId);
  }

  /**
   * Returns a DELETED tracking row derived from {@code source}.
   *
   * @param source source tracking from a manifest entry
   * @param newSnapshotId the snapshot ID in which the new tracking instance will be committed
   */
  static Tracking deleted(Tracking source, long newSnapshotId) {
    return terminal(EntryStatus.DELETED, source, newSnapshotId);
  }

  /**
   * Returns a REPLACED tracking row derived from {@code source}.
   *
   * @param source source tracking from a manifest entry
   * @param newSnapshotId the snapshot ID in which the new tracking instance will be committed
   */
  static Tracking replaced(Tracking source, long newSnapshotId) {
    return terminal(EntryStatus.REPLACED, source, newSnapshotId);
  }

  private TrackingBuilder(long newSnapshotId) {
    this.status = EntryStatus.ADDED;
    this.snapshotId = newSnapshotId;
    this.newSnapshotId = newSnapshotId;
    this.dataSequenceNumber = null;
    this.fileSequenceNumber = null;
    this.firstRowId = null;
    this.dvSnapshotId = null;
    this.deletedPositions = null;
    this.replacedPositions = null;
  }

  private TrackingBuilder(Tracking source, long newSnapshotId) {
    validateSource(source);
    validateStatusTransition(source.status(), EntryStatus.EXISTING);
    this.status = EntryStatus.EXISTING;
    this.snapshotId = source.snapshotId();
    this.newSnapshotId = newSnapshotId;
    this.dataSequenceNumber = source.dataSequenceNumber();
    this.fileSequenceNumber = source.fileSequenceNumber();
    this.firstRowId = source.firstRowId();
    this.dvSnapshotId = source.dvSnapshotId();
    this.deletedPositions = null;
    this.replacedPositions = null;
  }

  /** Indicates that the DV has been updated for the new Tracking. */
  TrackingBuilder dvUpdated() {
    Preconditions.checkState(
        deletedPositions == null && replacedPositions == null,
        "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");
    this.dvSnapshotId = newSnapshotId;
    if (status == EntryStatus.EXISTING) {
      this.status = EntryStatus.MODIFIED;
    }

    return this;
  }

  /** Sets the positions deleted by this commit for a manifest entry. */
  TrackingBuilder deletedPositions(ByteBuffer positions) {
    Preconditions.checkState(
        status != EntryStatus.ADDED, "Cannot set deleted positions on ADDED entry");
    this.deletedPositions = ByteBuffers.toByteArray(positions);
    this.dvSnapshotId = newSnapshotId;
    this.status = EntryStatus.MODIFIED;
    return this;
  }

  /** Sets the positions replaced by this commit for a manifest entry. */
  TrackingBuilder replacedPositions(ByteBuffer positions) {
    Preconditions.checkState(
        status != EntryStatus.ADDED, "Cannot set replaced positions on ADDED entry");
    this.replacedPositions = ByteBuffers.toByteArray(positions);
    this.dvSnapshotId = newSnapshotId;
    this.status = EntryStatus.MODIFIED;
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

  private static Tracking terminal(EntryStatus to, Tracking source, long newSnapshotId) {
    validateSource(source);
    validateStatusTransition(source.status(), to);
    return new TrackingStruct(
        to,
        newSnapshotId,
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

  private static void validateStatusTransition(EntryStatus from, EntryStatus to) {
    Preconditions.checkState(from != null, "Invalid tracking source: status is null");
    Preconditions.checkState(
        from != EntryStatus.DELETED && from != EntryStatus.REPLACED,
        "Cannot revive non-live entry with status %s",
        from);
    Preconditions.checkState(
        to != EntryStatus.ADDED, "Cannot transition to ADDED: ADDED is the starting status");
  }
}
