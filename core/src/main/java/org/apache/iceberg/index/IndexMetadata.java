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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
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
   * Return the list of metadata changes for this index.
   *
   * @return a list of metadata updates
   */
  List<MetadataUpdate> changes();

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
    private final List<MetadataUpdate> changes;
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
      this.changes = Lists.newArrayList();
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
      this.changes = Lists.newArrayList();
      this.formatVersion = base.formatVersion();
      this.currentVersionId = base.currentVersionId();
      this.location = base.location();
      this.uuid = base.uuid();
      this.type = base.type();
      this.indexColumnIds = ImmutableList.copyOf(base.indexColumnIds());
      this.optimizedColumnIds = ImmutableList.copyOf(base.optimizedColumnIds());
      this.metadataLocation = null;
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= formatVersion,
          "Cannot downgrade v%s index to v%s",
          formatVersion,
          newFormatVersion);

      if (formatVersion == newFormatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (null != location && location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder setMetadataLocation(String newMetadataLocation) {
      this.metadataLocation = newMetadataLocation;
      return this;
    }

    public Builder assignUUID(String newUUID) {
      Preconditions.checkArgument(newUUID != null, "Cannot set uuid to null");
      Preconditions.checkArgument(uuid == null || newUUID.equals(uuid), "Cannot reassign uuid");

      if (!newUUID.equals(uuid)) {
        this.uuid = newUUID;
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    public Builder setType(IndexType newType) {
      Preconditions.checkArgument(null != newType, "Invalid index type: null");
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

      if (lastAddedVersionId != null && lastAddedVersionId == newVersionId) {
        changes.add(new MetadataUpdate.SetCurrentIndexVersion(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetCurrentIndexVersion(newVersionId));
      }

      // Use the timestamp from the index version if it was added in current set of changes.
      // Otherwise, use the current system time. This handles cases where the index version
      // was set as current in the past and is being re-activated.
      boolean versionAddedInThisChange =
          changes(MetadataUpdate.AddIndexVersion.class)
              .anyMatch(added -> added.indexVersion().versionId() == newVersionId);

      this.historyEntry =
          ImmutableIndexHistoryEntry.builder()
              .timestampMillis(
                  versionAddedInThisChange ? version.timestampMillis() : System.currentTimeMillis())
              .versionId(version.versionId())
              .build();

      return this;
    }

    public Builder setCurrentVersion(IndexVersion version) {
      return setCurrentVersionId(addVersionInternal(version));
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
        boolean addedInBuilder =
            changes(MetadataUpdate.AddIndexVersion.class)
                .anyMatch(added -> added.indexVersion().versionId() == newVersionId);
        this.lastAddedVersionId = addedInBuilder ? newVersionId : null;
        return newVersionId;
      }

      versions.add(version);
      versionsById.put(version.versionId(), version);

      changes.add(new MetadataUpdate.AddIndexVersion(version));

      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private int reuseOrCreateNewVersionId(IndexVersion indexVersion) {
      // if the index version already exists, use its id; otherwise use the highest id + 1
      int newVersionId = indexVersion.versionId();
      for (IndexVersion version : versions) {
        if (sameIndexVersion(version, indexVersion)) {
          return version.versionId();
        } else if (version.versionId() >= newVersionId) {
          newVersionId = version.versionId() + 1;
        }
      }

      return newVersionId;
    }

    /**
     * Checks whether the given view versions would behave the same while ignoring the view version
     * id, the creation timestamp, and the operation.
     *
     * @param one the view version to compare
     * @param two the view version to compare
     * @return true if the given view versions would behave the same
     */
    private boolean sameIndexVersion(IndexVersion one, IndexVersion two) {
      return Objects.equals(one.properties(), two.properties());
    }

    public Builder addSnapshot(IndexSnapshot snapshot) {
      Preconditions.checkArgument(
          !snapshotsById.containsKey(snapshot.indexSnapshotId()),
          "Cannot add snapshot with duplicate id: %s",
          snapshot.indexSnapshotId());

      snapshots.add(snapshot);
      snapshotsById.put(snapshot.indexSnapshotId(), snapshot);
      changes.add(new MetadataUpdate.AddIndexSnapshot(snapshot));
      return this;
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      IndexVersion currentVersion = versionsById.get(currentVersionId);
      Map<String, String> newProperties = Maps.newHashMap();
      if (currentVersion.properties() != null) {
        newProperties.putAll(currentVersion.properties());
      }
      newProperties.putAll(updated);

      IndexVersion newVersion =
          ImmutableIndexVersion.builder()
              .from(currentVersion)
              .versionId(currentVersion.versionId() + 1)
              .timestampMillis(System.currentTimeMillis())
              .properties(newProperties)
              .build();

      setCurrentVersion(newVersion);
      changes.add(new MetadataUpdate.SetProperties(updated));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      IndexVersion currentVersion = versionsById.get(currentVersionId);
      Map<String, String> newProperties = Maps.newHashMap();
      if (currentVersion.properties() != null) {
        newProperties.putAll(currentVersion.properties());
      }
      propertiesToRemove.forEach(newProperties::remove);

      IndexVersion newVersion =
          ImmutableIndexVersion.builder()
              .from(currentVersion)
              .versionId(currentVersion.versionId() + 1)
              .timestampMillis(System.currentTimeMillis())
              .properties(newProperties)
              .build();

      setCurrentVersion(newVersion);
      changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    public Builder removeSnapshots(Set<Long> indexSnapshotIdsToRemove) {
      Preconditions.checkArgument(
          indexSnapshotIdsToRemove != null && !indexSnapshotIdsToRemove.isEmpty(),
          "Cannot remove snapshots: snapshot IDs to remove cannot be null or empty");

      List<IndexSnapshot> snapshotsToRemove = Lists.newArrayList();
      for (Long snapshotId : indexSnapshotIdsToRemove) {
        IndexSnapshot snapshot = snapshotsById.get(snapshotId);
        if (snapshot != null) {
          snapshotsToRemove.add(snapshot);
          snapshotsById.remove(snapshotId);
        }
      }

      snapshots.removeAll(snapshotsToRemove);

      if (!snapshotsToRemove.isEmpty()) {
        changes.add(new MetadataUpdate.RemoveIndexSnapshots(indexSnapshotIdsToRemove));
      }

      return this;
    }

    public IndexMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(!versions.isEmpty(), "Invalid index: no versions were added");
      Preconditions.checkArgument(null != type, "Invalid index type: null");
      Preconditions.checkArgument(!indexColumnIds.isEmpty(), "Index column IDs cannot be empty");
      Preconditions.checkArgument(
          !optimizedColumnIds.isEmpty(), "Optimized column IDs cannot be empty");

      // when associated with a metadata file, metadata must have no changes so that the metadata
      // matches exactly what is in the metadata file, which does not store changes. metadata
      // location with changes is inconsistent.
      Preconditions.checkArgument(
          metadataLocation == null || changes.isEmpty(),
          "Cannot create index metadata with a metadata location and changes");

      if (null != historyEntry) {
        history.add(historyEntry);
      }

      Map<String, String> indexProperties = versionsById.get(currentVersionId).properties();
      int historySize =
          PropertyUtil.propertyAsInt(
              indexProperties == null ? ImmutableMap.of() : indexProperties,
              IndexProperties.VERSION_HISTORY_SIZE,
              IndexProperties.VERSION_HISTORY_SIZE_DEFAULT);

      Preconditions.checkArgument(
          historySize > 0,
          "%s must be positive but was %s",
          IndexProperties.VERSION_HISTORY_SIZE,
          historySize);

      // expire old versions, but keep at least the versions added in this builder and the current
      // version
      int numVersions =
          ImmutableSet.builder()
              .addAll(
                  changes(MetadataUpdate.AddIndexVersion.class)
                      .map(v -> v.indexVersion().versionId())
                      .collect(Collectors.toSet()))
              .add(currentVersionId)
              .build()
              .size();
      int numVersionsToKeep = Math.max(numVersions, historySize);

      List<IndexVersion> retainedVersions;
      List<IndexHistoryEntry> retainedHistory;
      if (versions.size() > numVersionsToKeep) {
        retainedVersions =
            expireVersions(versionsById, numVersionsToKeep, versionsById.get(currentVersionId));
        Set<Integer> retainedVersionIds =
            retainedVersions.stream().map(IndexVersion::versionId).collect(Collectors.toSet());
        retainedHistory = updateHistory(history, retainedVersionIds);
      } else {
        retainedVersions = versions;
        retainedHistory = history;
      }

      return ImmutableIndexMetadata.of(
          null == uuid ? UUID.randomUUID().toString() : uuid,
          formatVersion,
          type,
          indexColumnIds,
          optimizedColumnIds,
          location,
          currentVersionId,
          retainedVersions,
          retainedHistory,
          snapshots,
          changes,
          metadataLocation);
    }

    @VisibleForTesting
    static List<IndexVersion> expireVersions(
        Map<Integer, IndexVersion> versionsById,
        int numVersionsToKeep,
        IndexVersion currentVersion) {
      // version ids are assigned sequentially. keep the latest versions by ID.
      List<Integer> ids = Lists.newArrayList(versionsById.keySet());
      ids.sort(Comparator.reverseOrder());

      List<IndexVersion> retainedVersions = Lists.newArrayList();
      // always retain the current version
      retainedVersions.add(currentVersion);

      for (int idToKeep : ids.subList(0, Math.min(numVersionsToKeep, ids.size()))) {
        if (retainedVersions.size() == numVersionsToKeep) {
          break;
        }

        IndexVersion version = versionsById.get(idToKeep);
        if (currentVersion.versionId() != version.versionId()) {
          retainedVersions.add(version);
        }
      }

      return retainedVersions;
    }

    @VisibleForTesting
    static List<IndexHistoryEntry> updateHistory(
        List<IndexHistoryEntry> history, Set<Integer> ids) {
      List<IndexHistoryEntry> retainedHistory = Lists.newArrayList();
      for (IndexHistoryEntry entry : history) {
        if (ids.contains(entry.versionId())) {
          retainedHistory.add(entry);
        } else {
          // clear history past any unknown version
          retainedHistory.clear();
        }
      }

      return retainedHistory;
    }

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }
  }
}
