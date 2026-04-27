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

/**
 * Adapts a {@link TrackedFile} to the {@link ManifestEntry} interface for v3 pipeline
 * compatibility.
 *
 * <p>This allows code that works with ManifestEntry (ManifestFiles.read(), ManifestGroup, etc.) to
 * consume entries from v4 manifests via {@link V4ManifestReader}.
 */
class TrackedFileEntryAdapter<F extends ContentFile<F>> implements ManifestEntry<F> {
  private final TrackedFile trackedFile;
  private final F adapted;
  private final PartitionSpec spec;

  // mutable fields for InheritableMetadata
  private Long overrideSnapshotId = null;
  private Long overrideDataSeqNum = null;
  private Long overrideFileSeqNum = null;
  private boolean snapshotIdOverridden = false;
  private boolean dataSeqNumOverridden = false;
  private boolean fileSeqNumOverridden = false;

  @SuppressWarnings("unchecked")
  TrackedFileEntryAdapter(TrackedFile trackedFile, PartitionSpec spec) {
    this.trackedFile = trackedFile;
    this.spec = spec;
    this.adapted = (F) adaptFile(trackedFile, spec);
  }

  private static ContentFile<?> adaptFile(TrackedFile file, PartitionSpec spec) {
    if (file.contentType() == FileContent.DATA) {
      return TrackedFileAdapters.asDataFile(file, spec);
    }

    // for EQUALITY_DELETES and POSITION_DELETES, use a minimal delete file adapter
    return TrackedFileAdapters.asDeleteFile(file, spec);
  }

  @Override
  public Status status() {
    Tracking tracking = trackedFile.tracking();
    if (tracking == null) {
      return Status.EXISTING;
    }

    EntryStatus entryStatus = tracking.status();
    if (entryStatus == null) {
      return Status.EXISTING;
    }

    switch (entryStatus) {
      case EXISTING:
        return Status.EXISTING;
      case ADDED:
        return Status.ADDED;
      case DELETED:
      case REPLACED:
        return Status.DELETED;
      default:
        throw new UnsupportedOperationException("Unknown entry status: " + entryStatus);
    }
  }

  @Override
  public Long snapshotId() {
    if (snapshotIdOverridden) {
      return overrideSnapshotId;
    }

    return trackedFile.tracking() != null ? trackedFile.tracking().snapshotId() : null;
  }

  @Override
  public void setSnapshotId(long snapshotId) {
    this.overrideSnapshotId = snapshotId;
    this.snapshotIdOverridden = true;
  }

  @Override
  public Long dataSequenceNumber() {
    if (dataSeqNumOverridden) {
      return overrideDataSeqNum;
    }

    return trackedFile.tracking() != null ? trackedFile.tracking().dataSequenceNumber() : null;
  }

  @Override
  public void setDataSequenceNumber(long dataSequenceNumber) {
    this.overrideDataSeqNum = dataSequenceNumber;
    this.dataSeqNumOverridden = true;
  }

  @Override
  public Long fileSequenceNumber() {
    if (fileSeqNumOverridden) {
      return overrideFileSeqNum;
    }

    return trackedFile.tracking() != null ? trackedFile.tracking().fileSequenceNumber() : null;
  }

  @Override
  public void setFileSequenceNumber(long fileSequenceNumber) {
    this.overrideFileSeqNum = fileSequenceNumber;
    this.fileSeqNumOverridden = true;
  }

  @Override
  public F file() {
    return adapted;
  }

  @Override
  public ManifestEntry<F> copy() {
    return new TrackedFileEntryAdapter<>(trackedFile.copy(), spec);
  }

  @Override
  public ManifestEntry<F> copyWithoutStats() {
    return new TrackedFileEntryAdapter<>(trackedFile.copyWithoutStats(), spec);
  }
}
