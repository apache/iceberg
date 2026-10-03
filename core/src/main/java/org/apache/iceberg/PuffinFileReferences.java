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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

final class PuffinFileReferences {

  static final String SOURCE_STATISTICS = "statistics";
  static final String SOURCE_DELETION_VECTOR = "deletion_vector";

  // Delete manifests do not store Puffin blob metadata. Iceberg currently defines Puffin delete
  // files as deletion-vector-v1 blobs over the row-position metadata field.
  private static final List<String> DV_REFERENCED_BLOB_TYPES =
      ImmutableList.of(StandardBlobTypes.DV_V1);
  private static final List<Integer> DV_REFERENCED_FIELD_IDS =
      ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId());

  private PuffinFileReferences() {}

  static PuffinFileReference fromStatisticsFile(long snapshotId, StatisticsFile statisticsFile) {
    List<BlobMetadata> blobs = statisticsFile.blobMetadata();

    List<String> referencedBlobTypes =
        blobs.stream().map(BlobMetadata::type).distinct().collect(ImmutableList.toImmutableList());

    List<Integer> referencedFieldIds =
        blobs.stream()
            .flatMap(blob -> blob.fields().stream())
            .distinct()
            .collect(ImmutableList.toImmutableList());

    return new PuffinFileReference(
        snapshotId,
        statisticsFile.path(),
        SOURCE_STATISTICS,
        statisticsFile.fileSizeInBytes(),
        blobs.size(),
        referencedBlobTypes,
        referencedFieldIds);
  }

  static PuffinFileReference fromDeletionVectorFile(
      long snapshotId, String path, long fileSizeInBytes, int referencedBlobCount) {
    return new PuffinFileReference(
        snapshotId,
        path,
        SOURCE_DELETION_VECTOR,
        fileSizeInBytes,
        referencedBlobCount,
        DV_REFERENCED_BLOB_TYPES,
        DV_REFERENCED_FIELD_IDS);
  }

  static StaticDataTask.Row toRow(Schema baseTableSchema, PuffinFileReference puffinFile) {
    ImmutableList.Builder<StaticDataTask.Row> referencedFields =
        ImmutableList.builderWithExpectedSize(puffinFile.referencedFieldIds().size());

    for (Integer fieldId : puffinFile.referencedFieldIds()) {
      referencedFields.add(
          StaticDataTask.Row.of(fieldId, currentFieldName(baseTableSchema, fieldId)));
    }

    return StaticDataTask.Row.of(
        puffinFile.snapshotId(),
        puffinFile.filePath(),
        puffinFile.source(),
        puffinFile.fileSizeInBytes(),
        puffinFile.referencedBlobCount(),
        puffinFile.referencedBlobTypes(),
        referencedFields.build());
  }

  private static String currentFieldName(Schema baseTableSchema, int fieldId) {
    if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
      return MetadataColumns.ROW_POSITION.name();
    }

    // Field IDs are authoritative. Names are resolved against the current table schema and may be
    // null when a field has been dropped.
    return baseTableSchema.findColumnName(fieldId);
  }

  static class DeletionVectorFileAccumulator {
    private final long snapshotId;
    private final String path;
    private final long fileSizeInBytes;
    private final Set<DeletionVectorBlobKey> referencedBlobs = Sets.newLinkedHashSet();

    DeletionVectorFileAccumulator(long snapshotId, String path, long fileSizeInBytes) {
      this.snapshotId = snapshotId;
      this.path = path;
      this.fileSizeInBytes = fileSizeInBytes;
    }

    void add(long entryFileSizeInBytes, long contentOffset, long contentSizeInBytes) {
      Preconditions.checkState(
          fileSizeInBytes == entryFileSizeInBytes,
          "Deletion vectors in the same Puffin file have different file sizes: "
              + "path=%s, snapshotId=%s, expected=%s, actual=%s",
          path,
          snapshotId,
          fileSizeInBytes,
          entryFileSizeInBytes);

      referencedBlobs.add(new DeletionVectorBlobKey(contentOffset, contentSizeInBytes));
    }

    PuffinFileReference toPuffinFileReference() {
      return PuffinFileReferences.fromDeletionVectorFile(
          snapshotId, path, fileSizeInBytes, referencedBlobs.size());
    }
  }

  record PuffinFileReference(
      long snapshotId,
      String filePath,
      String source,
      long fileSizeInBytes,
      int referencedBlobCount,
      List<String> referencedBlobTypes,
      List<Integer> referencedFieldIds) {}

  private record DeletionVectorBlobKey(long contentOffset, long contentSizeInBytes) {}
}
