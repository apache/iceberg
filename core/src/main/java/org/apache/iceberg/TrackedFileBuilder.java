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
  private PartitionData partitionData = null;

  // optional fields
  private Integer specId = null;
  private ContentStats contentStats = null;
  private Integer sortOrderId = null;
  private DeletionVector deletionVector = null;
  private ManifestInfo manifestInfo = null;
  private ByteBuffer keyMetadata = null;
  private List<Long> splitOffsets = null;
  private List<Integer> equalityIds = null;

  // tracking-related deferred mutators applied at build() time
  private boolean dvUpdated = false;
  private ByteBuffer deletedPositions = null;
  private ByteBuffer replacedPositions = null;

  /**
   * Creates a builder for a newly added data file entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed, or null
   *     for a staged-write ADDED entry whose snapshot ID will be inherited at read time from the
   *     manifest's snapshot_id
   */
  static TrackedFileBuilder data(Long newSnapshotId) {
    return new TrackedFileBuilder(FileContent.DATA, newSnapshotId);
  }

  /**
   * Creates a builder for a newly added equality delete file entry.
   *
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed, or null
   *     for a staged-write ADDED entry whose snapshot ID will be inherited at read time from the
   *     manifest's snapshot_id
   */
  static TrackedFileBuilder equalityDelete(Long newSnapshotId) {
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
   * Creates a builder with an externally-constructed TrackingBuilder. Use for content_entry rows
   * whose tracking values come from outside the writer (v4+ manifest references, projections from
   * legacy ManifestEntry).
   */
  static TrackedFileBuilder explicitTracking(
      FileContent contentType, TrackingBuilder trackingBuilder) {
    Preconditions.checkArgument(contentType != null, "Invalid content type: null");
    Preconditions.checkArgument(trackingBuilder != null, "Invalid tracking builder: null");
    return new TrackedFileBuilder(contentType, trackingBuilder);
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
   * <p>Manifest entries cannot transition to REPLACED.
   *
   * @param source source tracked file
   * @param newSnapshotId the snapshot ID in which the new tracked file will be committed
   */
  static TrackedFile replaced(TrackedFile source, long newSnapshotId) {
    Preconditions.checkArgument(source != null, "Invalid source: null");
    Preconditions.checkArgument(
        !isLeafManifest(source.contentType()),
        "Manifest entries cannot transition to REPLACED, but entry type is: %s",
        source.contentType());
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

  private TrackedFileBuilder(FileContent contentType, Long snapshotId) {
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
    this.partitionData = (PartitionData) source.partition();
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

  private TrackedFileBuilder(FileContent contentType, TrackingBuilder trackingBuilder) {
    this.contentType = contentType;
    this.trackingBuilder = trackingBuilder;
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

  TrackedFileBuilder specId(int newSpecId) {
    Preconditions.checkArgument(newSpecId >= 0, "Invalid spec ID: %s (must be >= 0)", newSpecId);
    this.specId = newSpecId;
    return this;
  }

  TrackedFileBuilder partition(PartitionData newPartitionData) {
    Preconditions.checkArgument(newPartitionData != null, "Invalid partition: null");
    this.partitionData = newPartitionData;
    return this;
  }

  TrackedFileBuilder contentStats(ContentStats newContentStats) {
    Preconditions.checkArgument(newContentStats != null, "Invalid content stats: null");
    this.contentStats = newContentStats;
    return this;
  }

  TrackedFileBuilder sortOrderId(int newSortOrderId) {
    Preconditions.checkArgument(
        !isLeafManifest(contentType),
        "Sort order ID cannot be added to manifest entries, but entry type is: %s",
        contentType);
    Preconditions.checkArgument(
        newSortOrderId >= 0, "Invalid sort order ID: %s (must be >= 0)", newSortOrderId);
    this.sortOrderId = newSortOrderId;
    return this;
  }

  TrackedFileBuilder deletionVector(DeletionVector newDeletionVector) {
    Preconditions.checkArgument(newDeletionVector != null, "Invalid deletion vector: null");
    Preconditions.checkArgument(
        contentType == FileContent.DATA,
        "Deletion vector can only be added to DATA entries, but entry type is: %s",
        contentType);
    Preconditions.checkArgument(
        this.deletionVector == null || !this.deletionVector.equals(newDeletionVector),
        "The same deletion vector already added");
    this.deletionVector = newDeletionVector;
    this.dvUpdated = true;
    return this;
  }

  TrackedFileBuilder manifestInfo(ManifestInfo newManifestInfo) {
    Preconditions.checkArgument(newManifestInfo != null, "Invalid manifest info: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Manifest info can only be added to manifests, but entry type is: %s",
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
        !isLeafManifest(contentType),
        "Split offsets cannot be added to manifest entries, but entry type is: %s",
        contentType);
    this.splitOffsets = newSplitOffsets;
    return this;
  }

  TrackedFileBuilder equalityIds(List<Integer> newEqualityIds) {
    Preconditions.checkArgument(newEqualityIds != null, "Invalid equality IDs: null");
    Preconditions.checkArgument(
        contentType == FileContent.EQUALITY_DELETES,
        "Equality IDs can only be added to EQUALITY_DELETES entries, but entry type is: %s",
        contentType);
    this.equalityIds = newEqualityIds;
    return this;
  }

  TrackedFileBuilder deletedPositions(ByteBuffer newDeletedPositions) {
    Preconditions.checkArgument(newDeletedPositions != null, "Invalid deleted positions: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Deleted positions can only be added to manifest entries, but entry type is: %s",
        contentType);
    this.deletedPositions = newDeletedPositions;
    return this;
  }

  TrackedFileBuilder replacedPositions(ByteBuffer newReplacedPositions) {
    Preconditions.checkArgument(newReplacedPositions != null, "Invalid replaced positions: null");
    Preconditions.checkArgument(
        isLeafManifest(contentType),
        "Replaced positions can only be added to manifest entries, but entry type is: %s",
        contentType);
    this.replacedPositions = newReplacedPositions;
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
        partitionData != null || isLeafManifest(contentType),
        "Partition is required for content type %s",
        contentType);
    Preconditions.checkArgument(
        !isLeafManifest(contentType) || manifestInfo != null,
        "Missing required field: manifest info");
    Preconditions.checkArgument(
        contentType != FileContent.EQUALITY_DELETES || equalityIds != null,
        "Missing required field: equality IDs");

    if (dvUpdated) {
      trackingBuilder.dvUpdated();
    }

    if (deletedPositions != null) {
      trackingBuilder.deletedPositions(deletedPositions);
    }

    if (replacedPositions != null) {
      trackingBuilder.replacedPositions(replacedPositions);
    }

    Tracking trackingResult = trackingBuilder.build();

    return new TrackedFileStruct(
        trackingResult,
        contentType,
        formatVersion,
        location,
        fileFormat,
        partitionData,
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
