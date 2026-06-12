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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads v4+ root manifest files, yielding one {@link ManifestFile} per {@code DATA_MANIFEST} or
 * {@code DELETE_MANIFEST} content_entry row.
 *
 * <p>Direct data-file entries ({@code content_type=DATA} or {@code EQUALITY_DELETES}) are skipped
 * with a DEBUG log; they represent the small-write optimization which is deferred to a future
 * phase.
 *
 * <p>The reader projects the partition column with the placeholder type from {@link
 * RootManifestWriter#emptyPartitionPlaceholderIfNeeded}. Manifest reference rows leave the
 * partition column null on write, and direct data-file entries are skipped, so reading with the
 * placeholder is sufficient — Iceberg's field-id projection returns null when no writer field
 * matches the placeholder id.
 */
class RootManifestReader {
  private static final Logger LOG = LoggerFactory.getLogger(RootManifestReader.class);

  private RootManifestReader() {}

  /**
   * Reads a v4+ root manifest and returns the list of {@link ManifestFile} objects.
   *
   * @param rootManifest the root manifest input file
   * @return list of manifest files (data and delete), in the order they appear in the root manifest
   */
  static List<ManifestFile> read(InputFile rootManifest) {
    Preconditions.checkArgument(rootManifest != null, "Invalid root manifest input file: null");
    Types.StructType readPartitionType =
        RootManifestWriter.emptyPartitionPlaceholderIfNeeded(Types.StructType.of());
    Schema contentEntrySchema =
        new Schema(
            TrackedFile.schemaWithContentStats(
                    readPartitionType, RootManifestWriter.ROOT_CONTENT_STATS_TYPE)
                .fields());

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, rootManifest)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .build();

    List<ManifestFile> manifests = Lists.newArrayList();
    try {
      for (TrackedFileStruct row : rows) {
        FileContent content = row.contentType();
        if (content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST) {
          manifests.add(toManifestFile(row));
        } else {
          // Direct data-file entries (DATA, EQUALITY_DELETES) are the small-write optimization,
          // deferred to a future phase. Skip them silently at DEBUG level.
          LOG.debug(
              "Skipping direct data-file entry with content_type={} in root manifest {}",
              content,
              rootManifest.location());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read root manifest: " + rootManifest.location(), e);
    } finally {
      try {
        rows.close();
      } catch (IOException e) {
        LOG.warn("Failed to close root manifest reader for {}", rootManifest.location(), e);
      }
    }

    return manifests;
  }

  private static ManifestFile toManifestFile(TrackedFileStruct row) {
    Tracking tracking = row.tracking();
    Preconditions.checkArgument(
        tracking != null, "Invalid root manifest entry: missing tracking struct");

    ManifestContent manifestContent =
        row.contentType() == FileContent.DATA_MANIFEST
            ? ManifestContent.DATA
            : ManifestContent.DELETES;

    Long snapshotId = tracking.snapshotId();
    Long sequenceNumber = tracking.dataSequenceNumber();
    long seqNum = sequenceNumber != null ? sequenceNumber : 0L;

    ManifestInfo info = row.manifestInfo();
    int addedFiles = info != null ? info.addedFilesCount() : 0;
    int existingFiles = info != null ? info.existingFilesCount() : 0;
    int deletedFiles = info != null ? info.deletedFilesCount() : 0;
    long addedRows = info != null ? info.addedRowsCount() : 0L;
    long existingRows = info != null ? info.existingRowsCount() : 0L;
    long deletedRows = info != null ? info.deletedRowsCount() : 0L;
    long minSequenceNumber = info != null ? info.minSequenceNumber() : seqNum;
    Integer replacedFiles = projectedReplacedFilesCount(info);
    Long replacedRows = projectedReplacedRowsCount(info);

    Integer specId = row.specId();
    int partitionSpecId = specId != null ? specId : 0;

    return new GenericManifestFile(
        row.location(),
        row.fileSizeInBytes(),
        partitionSpecId,
        manifestContent,
        seqNum,
        minSequenceNumber,
        snapshotId,
        null /* no partition summaries in root manifest entries */,
        row.keyMetadata(),
        addedFiles,
        addedRows,
        existingFiles,
        existingRows,
        deletedFiles,
        deletedRows,
        tracking.firstRowId(),
        row.recordCount(),
        row.formatVersion(),
        replacedFiles,
        replacedRows);
  }

  /** Returns the REPLACED file count from {@code info} when present and non-zero, else null. */
  private static Integer projectedReplacedFilesCount(ManifestInfo info) {
    if (info == null) {
      return null;
    }

    return info.replacedFilesCount() > 0 ? info.replacedFilesCount() : null;
  }

  /**
   * Returns the REPLACED row count from {@code info} when its file count is non-zero, else null.
   */
  private static Long projectedReplacedRowsCount(ManifestInfo info) {
    if (info == null) {
      return null;
    }

    return info.replacedFilesCount() > 0 ? info.replacedRowsCount() : null;
  }
}
