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

import java.util.Map;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Reader for v4 manifest files containing {@link TrackedFile} entries.
 *
 * <p>Supports reading both root manifests and leaf manifests. Returns TrackedFile entries which can
 * represent data files, equality deletes, or manifest references.
 */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  private final InputFile file;
  private final Map<Integer, PartitionSpec> specsById;

  V4ManifestReader(InputFile file, Map<Integer, PartitionSpec> specsById) {
    this.file = file;
    this.specsById = specsById;
  }

  /** Returns all entries in the manifest. */
  CloseableIterable<TrackedFile> entries() {
    return open();
  }

  /** Returns only live entries (ADDED or EXISTING, not DELETED or REPLACED). */
  CloseableIterable<TrackedFile> liveEntries() {
    return CloseableIterable.filter(open(), this::isLive);
  }

  /** Returns copied live entries for safe use outside iteration. */
  @Override
  public CloseableIterator<TrackedFile> iterator() {
    return CloseableIterable.transform(liveEntries(), TrackedFile::copy).iterator();
  }

  Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  private boolean isLive(TrackedFile tf) {
    if (tf == null) {
      return false;
    }

    Tracking tracking = tf.tracking();
    return tracking != null && tracking.isLive();
  }

  private CloseableIterable<TrackedFile> open() {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(
        format != null, "Unable to determine format of manifest: %s", file.location());

    // Hack: Exclude SPLIT_OFFSETS and EQUALITY_IDS from read projection to tolerate
    // manifests that don't write list element field IDs
    // TODO: Fix it
    Schema fullSchema = V4Metadata.entrySchema(Types.StructType.of());
    Schema readSchema =
        new Schema(
            fullSchema.columns().stream()
                .filter(
                    f ->
                        f.fieldId() != TrackedFile.SPLIT_OFFSETS.fieldId()
                            && f.fieldId() != TrackedFile.EQUALITY_IDS.fieldId())
                .collect(java.util.stream.Collectors.toList()));

    CloseableIterable<TrackedFile> reader =
        InternalData.read(format, file)
            .project(readSchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .reuseContainers()
            .build();

    addCloseable(reader);
    return reader;
  }
}
