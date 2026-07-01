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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class TrackedFileBuilder {
  private final FileContent contentType;
  private final TrackingBuilder trackingBuilder;

  // Required fields
  private Integer formatVersion = null;
  private String location = null;
  private FileFormat fileFormat = null;
  private Long recordCount = null;
  private Long fileSizeInBytes = null;

  // optional fields
  private Integer specId = null;
  private PartitionData partition = null;
  private ContentStats contentStats = null;
  private Integer sortOrderId = null;
  private DeletionVector deletionVector = null;
  private ManifestInfo manifestInfo = null;
  private ByteBuffer keyMetadata = null;
  private List<Long> splitOffsets = null;
  private List<Integer> equalityIds = null;

  /**
   * Creates a builder for a newly added data file entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFileBuilder data(long newSnapshotId) {
    return new TrackedFileBuilder(FileContent.DATA, newSnapshotId);
  }

  /**
   * Creates a builder for a newly added equality delete file entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFileBuilder equalityDelete(long newSnapshotId) {
    return new TrackedFileBuilder(FileContent.EQUALITY_DELETES, newSnapshotId);
  }

  /**
   * Creates a builder for a newly added data manifest entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFileBuilder dataManifest(long newSnapshotId) {
    return new TrackedFileBuilder(FileContent.DATA_MANIFEST, newSnapshotId);
  }

  /**
   * Creates a builder for a newly added delete manifest entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFileBuilder deleteManifest(long newSnapshotId) {
    return new TrackedFileBuilder(FileContent.DELETE_MANIFEST, newSnapshotId);
  }

  /**
   * Creates a builder for a tracked file derived from {@code source}.
   *
   * @param source source tracked file to copy fields from
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFileBuilder from(TrackedFile source, long newSnapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source: null");
    return new TrackedFileBuilder(source, newSnapshotId);
  }

  /**
   * Returns a DELETED tracked file derived from {@code source}.
   *
   * @param source source tracked file
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFile deleted(TrackedFile source, long newSnapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source: null");
    return terminal(source, TrackingBuilder.deleted(source.tracking(), newSnapshotId));
  }

  /**
   * Returns a REPLACED tracked file derived from {@code source}.
   *
   * <p>Manifest entries cannot transition to REPLACED status.
   *
   * @param source source tracked file
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFile replaced(TrackedFile source, long newSnapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source: null");
    Preconditions.checkArgument(
        !isLeafManifest(source.contentType()),
        "Cannot transition manifest entries to REPLACED status");
    return terminal(source, TrackingBuilder.replaced(source.tracking(), newSnapshotId));
  }

  private static TrackedFile terminal(TrackedFile source, Tracking tracking) {
    return new TrackedFileStruct(
        tracking,
        source.contentType(),
        source.formatVersion(),
        source.location(),
        source.fileFormat(),
        (PartitionData) source.partition(),
        source.recordCount(),
        source.fileSizeInBytes(),
        source.specId(),
        source.contentStats(),
        source.sortOrderId(),
        source.deletionVector(),
        source.manifestInfo(),
        source.keyMetadata(),
        source.splitOffsets(),
        source.equalityIds());
  }

  private TrackedFileBuilder(FileContent contentType, long snapshotId) {
    this.contentType = contentType;
    this.trackingBuilder = TrackingBuilder.added(snapshotId);
  }

  private TrackedFileBuilder(TrackedFile source, long snapshotId) {
    this.contentType = source.contentType();
    this.formatVersion = source.formatVersion();
    this.location = source.location();
    this.fileFormat = source.fileFormat();
    this.recordCount = source.recordCount();
    this.fileSizeInBytes = source.fileSizeInBytes();
    this.partition = (PartitionData) source.partition();
    this.specId = source.specId();
    this.contentStats = source.contentStats();
    this.sortOrderId = source.sortOrderId();
    this.deletionVector = source.deletionVector();
    this.manifestInfo = source.manifestInfo();
    this.keyMetadata = source.keyMetadata();
    this.splitOffsets = source.splitOffsets();
    this.equalityIds = source.equalityIds();
    this.trackingBuilder = TrackingBuilder.from(source.tracking(), snapshotId);
  }

  TrackedFileBuilder formatVersion(int newFormatVersion) {
    Preconditions.checkArgument(
        newFormatVersion >= 0, "Invalid format version: %s (must be >= 0)", newFormatVersion);
    this.formatVersion = newFormatVersion;
    return this;
  }

  TrackedFileBuilder location(String newLocation) {
    Preconditions.checkArgument(newLocation != null, "Invalid location: null");
    this.location = newLocation;
    return this;
  }

  TrackedFileBuilder fileFormat(FileFormat newFileFormat) {
    Preconditions.checkArgument(newFileFormat != null, "Invalid file format: null");
    this.fileFormat = newFileFormat;
    return this;
  }

  TrackedFileBuilder recordCount(long newRecordCount) {
    Preconditions.checkArgument(
        newRecordCount >= 0, "Invalid record count: %s (must be >= 0)", newRecordCount);
    this.recordCount = newRecordCount;
    return this;
  }

  TrackedFileBuilder fileSizeInBytes(long newFileSizeInBytes) {
    Preconditions.checkArgument(
        newFileSizeInBytes >= 0,
        "Invalid file size in bytes: %s (must be >= 0)",
        newFileSizeInBytes);
    this.fileSizeInBytes = newFileSizeInBytes;
    return this;
  }

  TrackedFileBuilder partition(PartitionSpec spec, StructLike newPartition) {
    Preconditions.checkArgument(spec != null, "Invalid spec: null");
    Preconditions.checkArgument(
        !isLeafManifest(contentType), "Cannot set partition for manifest entries");
    if (spec.isUnpartitioned()) {
      Preconditions.checkArgument(
          newPartition == null, "Invalid partition: must be null for unpartitioned spec");
    } else {
      Preconditions.checkArgument(newPartition != null, "Invalid partition: null");
      this.partition = DataFiles.copyPartitionData(spec, newPartition, null);
    }
    this.specId = spec.specId();
    return this;
  }

  TrackedFileBuilder contentStats(ContentStats newContentStats) {
    Preconditions.checkArgument(newContentStats != null, "Invalid content stats: null");
    this.contentStats = newContentStats;
    return this;
  }

  TrackedFileBuilder sortOrderId(int newSortOrderId) {
    Preconditions.checkArgument(
        !isLeafManifest(contentType), "Cannot set sort order for manifest files");
    Preconditions.checkArgument(
        newSortOrderId >= 0, "Invalid sort order ID: %s (must be >= 0)", newSortOrderId);
    this.sortOrderId = newSortOrderId;
    return this;
  }

  TrackedFileBuilder deletionVector(DeletionVector newDeletionVector) {
    Preconditions.checkArgument(newDeletionVector != null, "Invalid deletion vector: null");
    Preconditions.checkArgument(
        contentType == FileContent.DATA,
        "Cannot add deletion vector for file with content: %s",
        contentType);
    if (deletionVector != null) {
      Preconditions.checkArgument(
          deletionVector.cardinality() < newDeletionVector.cardinality(),
          "Invalid DV update, cardinality must increase: existing=%s, new=%s",
          deletionVector.cardinality(),
          newDeletionVector.cardinality());
      Preconditions.checkArgument(
          !deletionVector.location().equals(newDeletionVector.location())
              || deletionVector.offset() != newDeletionVector.offset(),
          "Invalid DV update: same location and offset");
    }
    this.deletionVector = newDeletionVector;
    trackingBuilder.dvUpdated();
    return this;
  }

  TrackedFileBuilder manifestInfo(ManifestInfo newManifestInfo) {
    Preconditions.checkArgument(newManifestInfo != null, "Invalid manifest info: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Cannot add manifest info for file with content: %s",
        contentType);
    this.manifestInfo = newManifestInfo;
    return this;
  }

  TrackedFileBuilder keyMetadata(ByteBuffer newKeyMetadata) {
    Preconditions.checkArgument(newKeyMetadata != null, "Invalid key metadata: null");
    this.keyMetadata = newKeyMetadata;
    return this;
  }

  TrackedFileBuilder splitOffsets(List<Long> newSplitOffsets) {
    Preconditions.checkArgument(newSplitOffsets != null, "Invalid split offsets: null");
    Preconditions.checkArgument(
        !isLeafManifest(contentType), "Cannot set split offsets for manifest files");
    this.splitOffsets = newSplitOffsets;
    return this;
  }

  TrackedFileBuilder equalityIds(List<Integer> newEqualityIds) {
    Preconditions.checkArgument(newEqualityIds != null, "Invalid equality IDs: null");
    Preconditions.checkArgument(
        contentType == FileContent.EQUALITY_DELETES,
        "Cannot add equality IDs for file with content: %s",
        contentType);
    this.equalityIds = newEqualityIds;
    return this;
  }

  TrackedFileBuilder deletedPositions(ByteBuffer newDeletedPositions) {
    Preconditions.checkArgument(newDeletedPositions != null, "Invalid deleted positions: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Cannot add deleted positions for file with content: %s",
        contentType);
    trackingBuilder.deletedPositions(newDeletedPositions);
    return this;
  }

  TrackedFileBuilder replacedPositions(ByteBuffer newReplacedPositions) {
    Preconditions.checkArgument(newReplacedPositions != null, "Invalid replaced positions: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Cannot add replaced positions for file with content: %s",
        contentType);
    trackingBuilder.replacedPositions(newReplacedPositions);
    return this;
  }

  private static boolean isLeafManifest(FileContent contentType) {
    return contentType == FileContent.DATA_MANIFEST || contentType == FileContent.DELETE_MANIFEST;
  }

  TrackedFile build() {
    Preconditions.checkArgument(formatVersion != null, "Missing required field: format version");
    Preconditions.checkArgument(location != null, "Missing required field: location");
    Preconditions.checkArgument(fileFormat != null, "Missing required field: file format");
    Preconditions.checkArgument(recordCount != null, "Missing required field: record count");
    Preconditions.checkArgument(
        fileSizeInBytes != null, "Missing required field: file size in bytes");
    Preconditions.checkArgument(
        !isLeafManifest(contentType) || manifestInfo != null,
        "Missing required field: manifest info");
    Preconditions.checkArgument(
        contentType != FileContent.EQUALITY_DELETES || equalityIds != null,
        "Missing required field: equality IDs");

    return new TrackedFileStruct(
        trackingBuilder.build(),
        contentType,
        formatVersion,
        location,
        fileFormat,
        partition,
        recordCount,
        fileSizeInBytes,
        specId,
        contentStats,
        sortOrderId,
        deletionVector,
        manifestInfo,
        keyMetadata,
        splitOffsets,
        equalityIds);
  }
}
