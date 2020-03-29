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

class InheritableMetadataFactory {

  private static final InheritableMetadata EMPTY = new EmptyInheritableMetadata();

  private InheritableMetadataFactory() {}

  static InheritableMetadata empty() {
    return EMPTY;
  }

  static InheritableMetadata fromManifest(ManifestFile manifest) {
    return new BaseInheritableMetadata(manifest.snapshotId(), manifest.sequenceNumber());
  }

  static class BaseInheritableMetadata implements InheritableMetadata {

    private final Long snapshotId;
    private final Long sequenceNumber;

    private BaseInheritableMetadata(Long snapshotId, Long sequenceNumber) {
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    @Override
    public ManifestEntry apply(ManifestEntry manifestEntry) {
      if (manifestEntry.snapshotId() == null) {
        manifestEntry.setSnapshotId(snapshotId);
      }
      if (manifestEntry.sequenceNumber() == null && this.sequenceNumber != null) {
        manifestEntry.setSequenceNumber(sequenceNumber);
      }
      return manifestEntry;
    }
  }

  static class EmptyInheritableMetadata implements InheritableMetadata {

    private EmptyInheritableMetadata() {}

    @Override
    public ManifestEntry apply(ManifestEntry manifestEntry) {
      if (manifestEntry.snapshotId() == null) {
        throw new IllegalArgumentException("Entries must have explicit snapshot ids if inherited metadata is empty");
      }
      return manifestEntry;
    }
  }
}
