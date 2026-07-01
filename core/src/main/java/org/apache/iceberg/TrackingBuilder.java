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
  private final Long newSnapshotId;
  private final Long snapshotId;
  private final Long dataSequenceNumber;
  private final Long fileSequenceNumber;
  private Long firstRowId;
  private EntryStatus status;
  private Long dvSnapshotId;
  private byte[] deletedPositions;
  private byte[] replacedPositions;

  /**
   * Creates a builder for a newly added file.
   *
   * @param newSnapshotId the snapshot ID in which the new tracking instance will be committed, or
   *     null for a staged-write ADDED entry whose snapshot ID will be inherited at read time from
   *     the manifest's snapshot_id
   */
  static TrackingBuilder added(Long newSnapshotId) {
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

  /**
   * Returns a {@link TrackingBuilder} for a v4 {@code content_entry} row whose tracking values are
   * provided by the caller — used when values come from outside the writer: a root-manifest entry's
   * commit sequence number, or a legacy {@link ManifestEntry}'s sequence numbers projected into v4
   * shape. Unlike {@link #added(long)}, which defaults to {@code ADDED} status with null sequence
   * numbers, and {@link #from(Tracking, long)}, which derives from a prior tracking row, this
   * factory takes every field explicitly.
   *
   * @param status entry status (typically {@link EntryStatus#ADDED} for newly written manifest
   *     references, {@link EntryStatus#EXISTING} for carried-over references and projected legacy
   *     entries, or {@link EntryStatus#DELETED} for terminal entries)
   * @param snapshotId snapshot ID for this commit
   * @param dataSequenceNumber the data sequence number for the entry
   * @param fileSequenceNumber the file sequence number for the entry
   * @param firstRowId first-row-id for ADDED data files within a referenced manifest, or carried-
   *     over first-row-id for EXISTING/DELETED data file projections; null for delete files and
   *     entries that will inherit at read time
   */
  static TrackingBuilder forContentEntry(
      EntryStatus status,
      long snapshotId,
      long dataSequenceNumber,
      long fileSequenceNumber,
      Long firstRowId) {
    Preconditions.checkArgument(status != null, "Invalid status: null");
    return new TrackingBuilder(
        status, snapshotId, dataSequenceNumber, fileSequenceNumber, firstRowId);
  }

  private TrackingBuilder(Long newSnapshotId) {
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

  // Constructor for explicit tracking rows used by forContentEntry.
  // The tracking-mutator setters (dvUpdated / deletedPositions / replacedPositions) are not
  // intended to be used with explicit tracking — the TrackedFileBuilder guards that combination.
  private TrackingBuilder(
      EntryStatus status,
      long snapshotId,
      Long dataSequenceNumber,
      Long fileSequenceNumber,
      Long firstRowId) {
    this.status = status;
    this.snapshotId = snapshotId;
    this.newSnapshotId = snapshotId;
    this.dataSequenceNumber = dataSequenceNumber;
    this.fileSequenceNumber = fileSequenceNumber;
    this.firstRowId = firstRowId;
    this.dvSnapshotId = null;
    this.deletedPositions = null;
    this.replacedPositions = null;
  }

  /** Indicates that the DV has been updated for the new Tracking. */
  TrackingBuilder dvUpdated() {
    Preconditions.checkState(
        deletedPositions == null && replacedPositions == null,
        "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");
    Preconditions.checkState(
        newSnapshotId != null, "Cannot mark DV updated without a known commit snapshot ID");
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
    Preconditions.checkState(
        newSnapshotId != null, "Cannot set deleted positions without a known commit snapshot ID");
    this.deletedPositions = ByteBuffers.toByteArray(positions);
    this.dvSnapshotId = newSnapshotId;
    this.status = EntryStatus.MODIFIED;
    return this;
  }

  /** Sets the positions replaced by this commit for a manifest entry. */
  TrackingBuilder replacedPositions(ByteBuffer positions) {
    Preconditions.checkState(
        status != EntryStatus.ADDED, "Cannot set replaced positions on ADDED entry");
    Preconditions.checkState(
        newSnapshotId != null, "Cannot set replaced positions without a known commit snapshot ID");
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
