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
  static InheritableTrackedMetadata fromTrackedFile(TrackedFile<?> manifestEntry) {
    Preconditions.checkArgument(
        manifestEntry.contentType() == FileContent.DATA_MANIFEST
            || manifestEntry.contentType() == FileContent.DELETE_MANIFEST,
        "Can only create metadata from manifest entries, got: %s",
        manifestEntry.contentType());

    TrackingInfo tracking = manifestEntry.trackingInfo();
    Long snapshotId = tracking != null ? tracking.snapshotId() : null;
    Long sequenceNumber = tracking != null ? tracking.sequenceNumber() : null;

    Preconditions.checkArgument(
        snapshotId != null, "Manifest entry must have snapshot ID: %s", manifestEntry.location());

    return new BaseInheritableTrackedMetadata(
        snapshotId, sequenceNumber != null ? sequenceNumber : 0L);
  }

  static class BaseInheritableTrackedMetadata implements InheritableTrackedMetadata {
    private final long snapshotId;
    private final long sequenceNumber;

    private BaseInheritableTrackedMetadata(long snapshotId, long sequenceNumber) {
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    @Override
    public TrackedFile<?> apply(TrackedFile<?> entry) {
      TrackingInfo tracking = entry.trackingInfo();

      if (tracking == null || tracking.snapshotId() == null) {
        entry.setSnapshotId(snapshotId);
      }

      // in v1 tables, the sequence number is not persisted and can be safely defaulted to 0
      // in v2+ tables, the sequence number should be inherited iff the entry status is ADDED
      if (tracking == null || tracking.sequenceNumber() == null) {
        if (sequenceNumber == 0
            || (tracking != null && tracking.status() == TrackingInfo.Status.ADDED)) {
          entry.setSequenceNumber(sequenceNumber);
          entry.setFileSequenceNumber(sequenceNumber);
        }
      }

      return entry;
    }
  }
}
