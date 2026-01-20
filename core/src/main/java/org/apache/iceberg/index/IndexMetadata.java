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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.immutables.value.Value;

/**
 * Metadata for an index.
 *
 * <p>Index metadata is stored in metadata files and contains the full state of an index at a point
 * in time.
 */
@SuppressWarnings("ImmutablesStyle")
@Value.Immutable(builder = false)
@Value.Style(allParameters = true, visibilityString = "PACKAGE")
public interface IndexMetadata extends Serializable {

  int SUPPORTED_INDEX_FORMAT_VERSION = 1;
  int DEFAULT_INDEX_FORMAT_VERSION = 1;

  /**
   * Return the UUID that identifies this index.
   *
   * <p>Generated when the index is created. Implementations must throw an exception if an index's
   * UUID does not match the expected UUID after refreshing metadata.
   *
   * @return the index UUID as a string
   */
  String uuid();

  /**
   * Return the format version for this index.
   *
   * <p>An integer version number for the index metadata format; format-version is 1 for current
   * version of spec.
   *
   * @return the format version
   */
  int formatVersion();

  /**
   * Return the type of this index.
   *
   * <p>One of the supported index-types. For example: BTREE, TERM, IVF. Must be supplied during the
   * creation of an index and must not be changed.
   *
   * @return the index type
   */
  IndexType type();

  /**
   * Return the column IDs contained by this index.
   *
   * <p>The ids of the columns contained by the index.
   *
   * @return a list of column IDs
   */
  List<Integer> indexColumnIds();

  /**
   * Return the column IDs that this index is optimized for.
   *
   * <p>The ids of the columns that the index is designed to optimize for retrieval.
   *
   * @return a list of column IDs
   */
  List<Integer> optimizedColumnIds();

  /**
   * Return the index's base location.
   *
   * <p>Used to create index file locations.
   *
   * @return the index location
   */
  String location();

  /**
   * Return the ID of the current version of this index.
   *
   * @return the current version ID
   */
  int currentVersionId();

  /**
   * Return the list of known versions of this index.
   *
   * <p>The number of versions retained is implementation-specific. current-version-id must be
   * present in this list.
   *
   * @return a list of index versions
   */
  List<IndexVersion> versions();

  /**
   * Return the version history of this index.
   *
   * <p>A list of version log entries with the timestamp and version-id for every change to
   * current-version-id. The number of entries retained is implementation-specific.
   * current-version-id may or may not be present in this list.
   *
   * @return a list of index history entries
   */
  List<IndexHistoryEntry> history();

  /**
   * Return the snapshots of this index.
   *
   * <p>During index maintenance a new index snapshot is generated for the specific Table snapshot,
   * and it is added to the snapshots list.
   *
   * @return a list of index snapshots
   */
  List<IndexSnapshot> snapshots();

  /**
   * Return the metadata file location for this index metadata.
   *
   * @return the metadata file location, or null if not set
   */
  @Nullable
  String metadataFileLocation();

  /**
   * Return the current version of this index.
   *
   * @return the current index version
   */
  default IndexVersion currentVersion() {
    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in index versions: %s",
        currentVersionId(),
        versionsById().keySet());

    return versionsById().get(currentVersionId());
  }

  /**
   * Return a version by ID.
   *
   * @param versionId the version ID
   * @return the version, or null if not found
   */
  default IndexVersion version(int versionId) {
    return versionsById().get(versionId);
  }

  /**
   * Return a map of version ID to version.
   *
   * @return a map of versions by ID
   */
  @Value.Derived
  default Map<Integer, IndexVersion> versionsById() {
    ImmutableMap.Builder<Integer, IndexVersion> builder = ImmutableMap.builder();
    for (IndexVersion version : versions()) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  /**
   * Return a map of index snapshot ID to snapshot.
   *
   * @return a map of snapshots by index snapshot ID
   */
  @Value.Derived
  default Map<Long, IndexSnapshot> snapshotsById() {
    ImmutableMap.Builder<Long, IndexSnapshot> builder = ImmutableMap.builder();
    for (IndexSnapshot snapshot : snapshots()) {
      builder.put(snapshot.indexSnapshotId(), snapshot);
    }

    return builder.build();
  }

  /**
   * Return a map of table snapshot ID to index snapshot.
   *
   * @return a map of snapshots by table snapshot ID
   */
  @Value.Derived
  default Map<Long, IndexSnapshot> snapshotsByTableSnapshotId() {
    ImmutableMap.Builder<Long, IndexSnapshot> builder = ImmutableMap.builder();
    for (IndexSnapshot snapshot : snapshots()) {
      builder.put(snapshot.tableSnapshotId(), snapshot);
    }

    return builder.build();
  }

  /**
   * Return a snapshot by index snapshot ID.
   *
   * @param indexSnapshotId the index snapshot ID
   * @return the snapshot, or null if not found
   */
  default IndexSnapshot snapshot(long indexSnapshotId) {
    return snapshotsById().get(indexSnapshotId);
  }

  /**
   * Return a snapshot by table snapshot ID.
   *
   * @param tableSnapshotId the table snapshot ID
   * @return the snapshot, or null if no index snapshot exists for the table snapshot
   */
  default IndexSnapshot snapshotForTableSnapshot(long tableSnapshotId) {
    return snapshotsByTableSnapshotId().get(tableSnapshotId);
  }

  @Value.Check
  default void check() {
    Preconditions.checkArgument(
        formatVersion() > 0 && formatVersion() <= IndexMetadata.SUPPORTED_INDEX_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(IndexMetadata base) {
    return new Builder(base);
  }

  /** Builder for IndexMetadata. */
  class Builder {
    private static final int LAST_ADDED = -1;
    private final List<IndexVersion> versions;
    private final List<IndexHistoryEntry> history;
    private final List<IndexSnapshot> snapshots;
    private int formatVersion = DEFAULT_INDEX_FORMAT_VERSION;
    private int currentVersionId;
    private String location;
    private String uuid;
    private String metadataLocation;
    private IndexType type;
    private List<Integer> indexColumnIds;
    private List<Integer> optimizedColumnIds;

    // internal change tracking
    private Integer lastAddedVersionId = null;
    private IndexHistoryEntry historyEntry = null;

    // indexes
    private final Map<Integer, IndexVersion> versionsById;
    private final Map<Long, IndexSnapshot> snapshotsById;

    private Builder() {
      this.versions = Lists.newArrayList();
      this.versionsById = Maps.newHashMap();
      this.history = Lists.newArrayList();
      this.snapshots = Lists.newArrayList();
      this.snapshotsById = Maps.newHashMap();
      this.indexColumnIds = ImmutableList.of();
      this.optimizedColumnIds = ImmutableList.of();
      this.uuid = null;
    }

    private Builder(IndexMetadata base) {
      this.versions = Lists.newArrayList(base.versions());
      this.versionsById = Maps.newHashMap(base.versionsById());
      this.history = Lists.newArrayList(base.history());
      this.snapshots = Lists.newArrayList(base.snapshots());
      this.snapshotsById = Maps.newHashMap(base.snapshotsById());
      this.formatVersion = base.formatVersion();
      this.currentVersionId = base.currentVersionId();
      this.location = base.location();
      this.uuid = base.uuid();
      this.type = base.type();
      this.indexColumnIds = ImmutableList.copyOf(base.indexColumnIds());
      this.optimizedColumnIds = ImmutableList.copyOf(base.optimizedColumnIds());
      this.metadataLocation = null;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      this.location = newLocation;
      return this;
    }

    public Builder setMetadataLocation(String newMetadataLocation) {
      this.metadataLocation = newMetadataLocation;
      return this;
    }

    public Builder setUUID(String newUuid) {
      this.uuid = newUuid;
      return this;
    }

    public Builder assignUUID() {
      if (uuid == null) {
        this.uuid = UUID.randomUUID().toString();
      }

      return this;
    }

    public Builder setType(IndexType newType) {
      Preconditions.checkArgument(null != newType, "Invalid type: null");
      this.type = newType;
      return this;
    }

    public Builder setIndexColumnIds(List<Integer> newIndexColumnIds) {
      Preconditions.checkArgument(null != newIndexColumnIds, "Invalid index column ids: null");
      this.indexColumnIds = ImmutableList.copyOf(newIndexColumnIds);
      return this;
    }

    public Builder setOptimizedColumnIds(List<Integer> newOptimizedColumnIds) {
      Preconditions.checkArgument(
          null != newOptimizedColumnIds, "Invalid optimized column ids: null");
      this.optimizedColumnIds = ImmutableList.copyOf(newOptimizedColumnIds);
      return this;
    }

    public Builder setCurrentVersionId(int newVersionId) {
      if (newVersionId == LAST_ADDED) {
        Preconditions.checkState(
            lastAddedVersionId != null,
            "Cannot set last version id: no current version id has been set");
        return setCurrentVersionId(lastAddedVersionId);
      }

      if (currentVersionId == newVersionId) {
        return this;
      }

      IndexVersion version = versionsById.get(newVersionId);
      Preconditions.checkArgument(
          version != null, "Cannot set current version to unknown version: %s", newVersionId);

      this.currentVersionId = newVersionId;

      this.historyEntry =
          ImmutableIndexHistoryEntry.builder()
              .timestampMillis(version.timestampMillis())
              .versionId(version.versionId())
              .build();

      return this;
    }

    public Builder addVersion(IndexVersion version) {
      addVersionInternal(version);
      return this;
    }

    private int addVersionInternal(IndexVersion newVersion) {
      int newVersionId = reuseOrCreateNewVersionId(newVersion);
      IndexVersion version = newVersion;
      if (newVersionId != version.versionId()) {
        version = ImmutableIndexVersion.builder().from(version).versionId(newVersionId).build();
      }

      if (versionsById.containsKey(newVersionId)) {
        this.lastAddedVersionId = newVersionId;
        return newVersionId;
      }

      versions.add(version);
      versionsById.put(version.versionId(), version);

      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private int reuseOrCreateNewVersionId(IndexVersion indexVersion) {
      int newVersionId = indexVersion.versionId();
      for (IndexVersion version : versions) {
        if (version.versionId() >= newVersionId) {
          newVersionId = version.versionId() + 1;
        }
      }

      return newVersionId;
    }

    public Builder addSnapshot(IndexSnapshot snapshot) {
      Preconditions.checkArgument(
          !snapshotsById.containsKey(snapshot.indexSnapshotId()),
          "Cannot add snapshot with duplicate id: %s",
          snapshot.indexSnapshotId());

      snapshots.add(snapshot);
      snapshotsById.put(snapshot.indexSnapshotId(), snapshot);
      return this;
    }

    public IndexMetadata build() {
      Preconditions.checkArgument(null != uuid, "Invalid uuid: null");
      Preconditions.checkArgument(null != type, "Invalid type: null");
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(!versions.isEmpty(), "Invalid versions: empty");
      Preconditions.checkArgument(
          versionsById.containsKey(currentVersionId),
          "Cannot find current version %s in versions",
          currentVersionId);

      if (historyEntry != null) {
        history.add(historyEntry);
      }

      return ImmutableIndexMetadata.of(
          uuid,
          formatVersion,
          type,
          ImmutableList.copyOf(indexColumnIds),
          ImmutableList.copyOf(optimizedColumnIds),
          location,
          currentVersionId,
          ImmutableList.copyOf(versions),
          ImmutableList.copyOf(history),
          ImmutableList.copyOf(snapshots),
          metadataLocation);
    }
  }
}
