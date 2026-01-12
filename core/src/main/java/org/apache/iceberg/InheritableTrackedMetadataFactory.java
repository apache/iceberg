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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Factory for creating {@link InheritableTrackedMetadata} instances. */
class InheritableTrackedMetadataFactory {

  private InheritableTrackedMetadataFactory() {}

  /**
   * Creates inheritable metadata from explicit values.
   *
   * @param snapshotId the snapshot ID
   * @param sequenceNumber the sequence number
   * @return inheritable metadata instance
   */
  static InheritableTrackedMetadata create(long snapshotId, long sequenceNumber) {
    return new BaseInheritableTrackedMetadata(snapshotId, sequenceNumber);
  }

  /**
   * Creates inheritable metadata from a tracked file representing a manifest (for reading leaf
   * manifests).
   *
   * @param manifestEntry the DATA_MANIFEST or DELETE_MANIFEST tracked file from root
   * @return inheritable metadata instance
   */
  static InheritableTrackedMetadata fromTrackedFile(TrackedFile manifestEntry) {
    Preconditions.checkArgument(
        manifestEntry.contentType() == FileContent.DATA_MANIFEST
            || manifestEntry.contentType() == FileContent.DELETE_MANIFEST,
        "Can only create metadata from tracked files for manifests, got: %s",
        manifestEntry.contentType());

    TrackingInfo tracking = manifestEntry.trackingInfo();
    Preconditions.checkNotNull(
        tracking,
        "Manifest tracked file is missing tracking info and appears to be uncommitted: %s",
        manifestEntry.location());

    Long snapshotId = tracking.snapshotId();
    Long sequenceNumber = tracking.dataSequenceNumber();
    String manifestLocation = manifestEntry.location();

    Preconditions.checkNotNull(
        snapshotId, "Manifest tracked file must have snapshot ID: %s", manifestLocation);

    return new BaseInheritableTrackedMetadata(
        snapshotId, sequenceNumber != null ? sequenceNumber : 0L, manifestLocation);
  }

  static class BaseInheritableTrackedMetadata implements InheritableTrackedMetadata {
    private final long snapshotId;
    private final long sequenceNumber;
    private final String manifestLocation;

    private BaseInheritableTrackedMetadata(long snapshotId, long sequenceNumber) {
      this(snapshotId, sequenceNumber, null);
    }

    private BaseInheritableTrackedMetadata(
        long snapshotId, long sequenceNumber, String manifestLocation) {
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.manifestLocation = manifestLocation;
    }

    @Override
    public TrackedFileStruct apply(TrackedFileStruct entry) {
      TrackingInfo tracking = entry.trackingInfo();

      if (tracking == null || tracking.snapshotId() == null) {
        entry.setSnapshotId(snapshotId);
      }

      // in v1 tables, the sequence number is not persisted and can be safely defaulted to 0
      // in v2+ tables, the sequence number should be inherited iff the entry status is ADDED
      if (tracking == null || tracking.dataSequenceNumber() == null) {
        if (sequenceNumber == 0
            || (tracking != null && tracking.status() == TrackingInfo.Status.ADDED)) {
          entry.setSequenceNumber(sequenceNumber);
          entry.setFileSequenceNumber(sequenceNumber);
        }
      }

      if (manifestLocation != null) {
        entry.setManifestLocation(manifestLocation);
      }

      return entry;
    }
  }
}
