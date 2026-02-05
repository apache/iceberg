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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;

/**
 * Metadata for an index.
 *
 * <p>Index metadata is stored in metadata files and contains the full state of an index at a point
 * in time. The actual index data is stored separately, typically in the index location, and the
 * metadata contains pointers to these data files. Engines can use the index type to understand
 * where the data is stored and in what format.
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
   * <p>The ids of the columns which are stored losslessly in the index instance. Must be supplied
   * during the creation of an index and must not be changed.
   *
   * @return a list of column IDs
   */
  List<Integer> indexColumnIds();

  /**
   * Return the column IDs that this index is optimized for.
   *
   * <p>The ids of the columns that the index is designed to optimize for retrieval. Must be
   * supplied during the creation of an index and must not be changed.
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
   * @return a list of index updates
   */
  List<IndexUpdate> changes();

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
    private final List<IndexVersion> versions;
    private final List<IndexHistoryEntry> history;
    private final List<IndexSnapshot> snapshots;
    private final List<IndexUpdate> changes;
    private int formatVersion = DEFAULT_INDEX_FORMAT_VERSION;
    private int currentVersionId = 0;
    private String location;
    private String uuid;
    private String metadataLocation;
    private IndexType type;
    private List<Integer> indexColumnIds;
    private List<Integer> optimizedColumnIds;

    // internal change tracking
    private final Map<Integer, IndexVersion> newVersionsByUserVersionId = Maps.newHashMap();
    private final List<IndexVersion> newVersions = Lists.newArrayList();
    private final Map<Long, IndexSnapshot> newSnapshotsByTableSnapshotId = Maps.newHashMap();
    private IndexHistoryEntry historyEntry = null;

    // indexes
    private final Map<Integer, IndexVersion> versionsById;
    private final Map<Long, IndexSnapshot> snapshotsByTableSnapshotId;

    private Builder() {
      this.versions = Lists.newArrayList();
      this.versionsById = Maps.newHashMap();
      this.history = Lists.newArrayList();
      this.snapshots = Lists.newArrayList();
      this.snapshotsByTableSnapshotId = Maps.newHashMap();
      this.changes = Lists.newArrayList();
      this.indexColumnIds = ImmutableList.of();
      this.optimizedColumnIds = ImmutableList.of();
      this.uuid = UUID.randomUUID().toString();
    }

    private Builder(IndexMetadata base) {
      this.versions = Lists.newArrayList(base.versions());
      this.versionsById = Maps.newHashMap(base.versionsById());
      this.history = Lists.newArrayList(base.history());
      this.snapshots = Lists.newArrayList(base.snapshots());
      this.snapshotsByTableSnapshotId = Maps.newHashMap(base.snapshotsByTableSnapshotId());
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
      changes.add(new IndexUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (!newLocation.equals(location)) {
        this.location = newLocation;
        changes.add(new IndexUpdate.SetLocation(newLocation));
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

    public Builder setCurrentVersion(IndexVersion version) {
      Preconditions.checkArgument(version != null, "Invalid index version: null");
      IndexVersion newVersion = findSameVersion(version);
      IndexHistoryEntry entry;
      if (newVersion == null) {
        Preconditions.checkArgument(
            !newVersionsByUserVersionId.containsKey(version.versionId()),
            "Invalid index version id. Version %s already added to the index with different properties: %s.",
            version.versionId(),
            newVersionsByUserVersionId.get(version.versionId()));

        int newVersionId = findNewVersionId();
        if (newVersionId != version.versionId()) {
          // We need to generate a new version id
          newVersion =
              ImmutableIndexVersion.builder().from(version).versionId(findNewVersionId()).build();
        } else {
          newVersion = version;
        }

        newVersionsByUserVersionId.put(version.versionId(), newVersion);
        newVersions.add(newVersion);
        entry =
            ImmutableIndexHistoryEntry.builder()
                .versionId(newVersion.versionId())
                .timestampMillis(newVersion.timestampMillis())
                .build();
      } else {
        entry =
            ImmutableIndexHistoryEntry.builder()
                .versionId(newVersion.versionId())
                .timestampMillis(System.currentTimeMillis())
                .build();
      }

      if (currentVersionId != newVersion.versionId()) {
        this.currentVersionId = newVersion.versionId();
        this.historyEntry = entry;
      }

      changes.add(new IndexUpdate.SetCurrentVersion(version));
      return this;
    }

    public Builder addSnapshot(IndexSnapshot snapshot) {
      Preconditions.checkArgument(
          !snapshotsByTableSnapshotId.containsKey(snapshot.tableSnapshotId())
              && !newSnapshotsByTableSnapshotId.containsKey(snapshot.tableSnapshotId()),
          "Invalid table snapshot id. Snapshot for table snapshot %s already added to the index.",
          snapshot.tableSnapshotId());
      Preconditions.checkArgument(
          versionsById.containsKey(snapshot.versionId())
              || newVersionsByUserVersionId.containsKey(snapshot.versionId()),
          "Invalid index version id. Cannot add snapshot with unknown version id: %s",
          snapshot.versionId());

      IndexSnapshot newSnapshot = snapshot;
      IndexVersion newVersion = newVersionsByUserVersionId.get(snapshot.versionId());
      if (newVersion != null && newVersion.versionId() != snapshot.versionId()) {
        newSnapshot =
            ImmutableIndexSnapshot.builder()
                .from(snapshot)
                .versionId(newVersion.versionId())
                .build();
      }

      newSnapshotsByTableSnapshotId.put(newSnapshot.tableSnapshotId(), newSnapshot);

      changes.add(new IndexUpdate.AddSnapshot(snapshot));
      return this;
    }

    public Builder removeSnapshots(Set<Long> indexSnapshotIdsToRemove) {
      Preconditions.checkArgument(
          indexSnapshotIdsToRemove != null && !indexSnapshotIdsToRemove.isEmpty(),
          "Invalid snapshot id set to remove: %s",
          snapshots);

      Set<Long> existingSnapshotsIdsToRemove = Sets.newHashSet(indexSnapshotIdsToRemove);
      existingSnapshotsIdsToRemove.retainAll(
          snapshotsByTableSnapshotId.values().stream()
              .map(IndexSnapshot::indexSnapshotId)
              .collect(Collectors.toSet()));

      if (!existingSnapshotsIdsToRemove.isEmpty()) {
        snapshotsByTableSnapshotId
            .entrySet()
            .removeIf(
                entry -> existingSnapshotsIdsToRemove.contains(entry.getValue().indexSnapshotId()));
        snapshots.removeIf(
            snapshot -> existingSnapshotsIdsToRemove.contains(snapshot.indexSnapshotId()));
        changes.add(new IndexUpdate.RemoveSnapshots(existingSnapshotsIdsToRemove));
      }

      return this;
    }

    public IndexMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(
          !versions.isEmpty() || !newVersions.isEmpty(), "Invalid index: no versions were added");
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

      versions.addAll(newVersions);
      newVersions.forEach(version -> versionsById.put(version.versionId(), version));
      snapshots.addAll(newSnapshotsByTableSnapshotId.values());

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
                  changes(IndexUpdate.SetCurrentVersion.class)
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

    /**
     * Checks whether the given view versions would behave the same while ignoring the view version
     * id, the creation timestamp, and the operation.
     *
     * @param one the view version to compare
     * @param two the view version to compare
     * @return true if the given view versions would behave the same
     */
    private boolean sameIndexVersion(IndexVersion one, IndexVersion two) {
      return Objects.equals(one.properties(), two.properties())
          || (one.properties() == null && two.properties().isEmpty())
          || (two.properties() == null && one.properties().isEmpty());
    }

    private IndexVersion findSameVersion(IndexVersion indexVersion) {
      return versions.stream()
          .filter(version -> sameIndexVersion(version, indexVersion))
          .findAny()
          .orElseGet(
              () ->
                  newVersionsByUserVersionId.values().stream()
                      .filter(version -> sameIndexVersion(version, indexVersion))
                      .findAny()
                      .orElse(null));
    }

    private int findNewVersionId() {
      int newVersionId = 1;
      for (IndexVersion v : versions) {
        if (v.versionId() >= newVersionId) {
          newVersionId = v.versionId() + 1;
        }
      }

      for (IndexVersion v : newVersionsByUserVersionId.values()) {
        if (v.versionId() >= newVersionId) {
          newVersionId = v.versionId() + 1;
        }
      }

      return newVersionId;
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

    private <U extends IndexUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }
  }
}
