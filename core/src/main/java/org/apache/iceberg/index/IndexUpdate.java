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
package org.apache.iceberg.index;

import java.io.Serializable;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/** Represents a change to index metadata. */
public interface IndexUpdate extends Serializable {
  void applyTo(IndexMetadata.Builder indexMetadataBuilder);

  /** Assigns a UUID to the index. */
  class AssignUUID implements IndexUpdate {
    private final String uuid;

    public AssignUUID(String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.assignUUID(uuid);
    }
  }

  /** Adds a new index version to the index metadata. */
  class AddVersion implements IndexUpdate {
    private final IndexVersion indexVersion;

    public AddVersion(IndexVersion indexVersion) {
      this.indexVersion = indexVersion;
    }

    public IndexVersion indexVersion() {
      return indexVersion;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.addVersion(indexVersion);
    }
  }

  /** Sets the current index version in the index metadata. */
  class SetCurrentVersion implements IndexUpdate {
    private final int versionId;

    public SetCurrentVersion(int versionId) {
      this.versionId = versionId;
    }

    public int versionId() {
      return versionId;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.setCurrentVersionId(versionId);
    }
  }

  /** Adds a new index snapshot to the index metadata. */
  class AddSnapshot implements IndexUpdate {
    private final IndexSnapshot indexSnapshot;

    public AddSnapshot(IndexSnapshot indexSnapshot) {
      this.indexSnapshot = indexSnapshot;
    }

    public IndexSnapshot indexSnapshot() {
      return indexSnapshot;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.addSnapshot(indexSnapshot);
    }
  }

  /** Removes index snapshots from the index metadata. */
  class RemoveSnapshots implements IndexUpdate {
    private final Set<Long> indexSnapshotIds;

    public RemoveSnapshots(long indexSnapshotId) {
      this.indexSnapshotIds = ImmutableSet.of(indexSnapshotId);
    }

    public RemoveSnapshots(Set<Long> indexSnapshotIds) {
      this.indexSnapshotIds = ImmutableSet.copyOf(indexSnapshotIds);
    }

    public Set<Long> indexSnapshotIds() {
      return indexSnapshotIds;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.removeSnapshots(indexSnapshotIds);
    }
  }

  /** Sets the location of the index. */
  class SetLocation implements IndexUpdate {
    private final String location;

    public SetLocation(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }

    @Override
    public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
      indexMetadataBuilder.setLocation(location);
    }
  }
}
