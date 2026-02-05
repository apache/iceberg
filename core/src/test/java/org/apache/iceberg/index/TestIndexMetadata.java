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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class TestIndexMetadata {

  private IndexVersion newIndexVersion(int id) {
    return newIndexVersion(id, System.currentTimeMillis());
  }

  private IndexVersion newIndexVersion(int id, long timestampMillis) {
    return ImmutableIndexVersion.builder().versionId(id).timestampMillis(timestampMillis).build();
  }

  private IndexVersion newIndexVersion(
      int id, long timestampMillis, Map<String, String> properties) {
    return ImmutableIndexVersion.builder()
        .versionId(id)
        .timestampMillis(timestampMillis)
        .properties(properties)
        .build();
  }

  @Test
  public void testExpiration() {
    // purposely use versions and timestamps that do not match to check that version ID is used
    IndexVersion v1 = newIndexVersion(1);
    IndexVersion v3 = newIndexVersion(3);
    IndexVersion v2 = newIndexVersion(2);
    Map<Integer, IndexVersion> versionsById = ImmutableMap.of(1, v1, 2, v2, 3, v3);

    assertThat(IndexMetadata.Builder.expireVersions(versionsById, 3, v1))
        .containsExactlyInAnyOrder(v1, v2, v3);
    assertThat(IndexMetadata.Builder.expireVersions(versionsById, 2, v1))
        .containsExactlyInAnyOrder(v1, v3);
    assertThat(IndexMetadata.Builder.expireVersions(versionsById, 1, v1)).containsExactly(v1);
  }

  @Test
  public void testUpdateHistory() {
    IndexVersion v1 = newIndexVersion(1);
    IndexVersion v2 = newIndexVersion(2);
    IndexVersion v3 = newIndexVersion(3);

    IndexHistoryEntry one =
        ImmutableIndexHistoryEntry.builder()
            .versionId(v1.versionId())
            .timestampMillis(v1.timestampMillis())
            .build();
    IndexHistoryEntry two =
        ImmutableIndexHistoryEntry.builder()
            .versionId(v2.versionId())
            .timestampMillis(v2.timestampMillis())
            .build();
    IndexHistoryEntry three =
        ImmutableIndexHistoryEntry.builder()
            .versionId(v3.versionId())
            .timestampMillis(v3.timestampMillis())
            .build();

    assertThat(
            IndexMetadata.Builder.updateHistory(
                ImmutableList.of(one, two, three), ImmutableSet.of(1, 2, 3)))
        .containsExactly(one, two, three);

    // one was an invalid entry in the history, so all previous elements are removed
    assertThat(
            IndexMetadata.Builder.updateHistory(
                ImmutableList.of(three, two, one, two, three), ImmutableSet.of(2, 3)))
        .containsExactly(two, three);

    // two was an invalid entry in the history, so all previous elements are removed
    assertThat(
            IndexMetadata.Builder.updateHistory(
                ImmutableList.of(one, two, three, one, three), ImmutableSet.of(1, 3)))
        .containsExactly(three, one, three);
  }

  @Test
  public void nullAndMissingFields() {
    assertThatThrownBy(() -> IndexMetadata.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid location: null");

    assertThatThrownBy(() -> IndexMetadata.builder().setLocation("location").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index: no versions were added");
  }

  @Test
  public void unsupportedFormatVersion() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(23)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(newIndexVersion(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format version: 23");

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(0)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(newIndexVersion(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v1 index to v0");
  }

  @Test
  public void emptyIndexVersion() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index: no versions were added");
  }

  @Test
  public void invalidVersionHistorySizeToKeep() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(
                        newIndexVersion(
                            1, 1000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "0")))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("version.history.num-entries must be positive but was 0");
  }

  @Test
  public void indexVersionHistoryNormalization() {
    // Each version must have different properties to avoid deduplication
    IndexVersion indexVersionOne =
        newIndexVersion(
            1, 1000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "1"));
    IndexVersion indexVersionTwo =
        newIndexVersion(
            2, 2000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "2"));
    IndexVersion indexVersionThree =
        newIndexVersion(
            3, 3000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "3"));

    IndexMetadata originalIndexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .setCurrentVersion(indexVersionTwo)
            .setCurrentVersion(indexVersionThree)
            .build();

    // the first build will not expire versions that were added in the builder
    assertThat(originalIndexMetadata.versions()).hasSize(3);
    assertThat(originalIndexMetadata.history()).hasSize(1);

    // rebuild the metadata to expire older versions
    IndexMetadata indexMetadata = IndexMetadata.buildFrom(originalIndexMetadata).build();
    assertThat(indexMetadata.versions()).hasSize(2);
    assertThat(indexMetadata.history()).hasSize(1);

    // make sure that metadata changes reflect the current state after the history was adjusted
    List<IndexUpdate> changes = originalIndexMetadata.changes();
    assertThat(changes).hasSize(4);
    assertThat(changes)
        .element(0)
        .isInstanceOf(IndexUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetLocation.class))
        .extracting(IndexUpdate.SetLocation::location)
        .isEqualTo("location");

    assertThat(changes)
        .element(1)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(2)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(3)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionThree);
  }

  @Test
  public void indexVersionHistoryIsCorrectlyRetained() {
    // Each version must have different properties to avoid deduplication
    IndexVersion indexVersionOne =
        newIndexVersion(
            1, 1000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "1"));
    IndexVersion indexVersionTwo =
        newIndexVersion(
            2, 2000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "2"));
    IndexVersion indexVersionThree =
        newIndexVersion(
            3, 3000L, ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "3"));

    IndexMetadata originalIndexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .build();
    originalIndexMetadata =
        IndexMetadata.buildFrom(originalIndexMetadata).setCurrentVersion(indexVersionTwo).build();
    originalIndexMetadata =
        IndexMetadata.buildFrom(originalIndexMetadata).setCurrentVersion(indexVersionThree).build();

    assertThat(originalIndexMetadata.versions())
        .hasSize(2)
        .containsExactlyInAnyOrder(indexVersionTwo, indexVersionThree);
    assertThat(originalIndexMetadata.history())
        .hasSize(2)
        .last()
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);

    // rebuild the metadata to expire older versions
    IndexMetadata indexMetadata = IndexMetadata.buildFrom(originalIndexMetadata).build();
    assertThat(indexMetadata.versions())
        .hasSize(2)
        // there is no requirement about the order of versions
        .containsExactlyInAnyOrder(indexVersionThree, indexVersionTwo);
    assertThat(indexMetadata.history())
        .hasSize(2)
        .last()
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);

    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).setCurrentVersion(indexVersionTwo).build();
    assertThat(updated.versions())
        .hasSize(2)
        .containsExactlyInAnyOrder(indexVersionTwo, indexVersionThree);
    assertThat(updated.history())
        .hasSize(3)
        .element(1)
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);
    assertThat(updated.history()).last().extracting(IndexHistoryEntry::versionId).isEqualTo(2);

    IndexMetadata index =
        IndexMetadata.buildFrom(updated).setCurrentVersion(indexVersionThree).build();
    assertThat(index.versions())
        .hasSize(2)
        .containsExactlyInAnyOrder(indexVersionTwo, indexVersionThree);
    assertThat(index.history())
        .hasSize(4)
        .element(1)
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);
    assertThat(index.history()).element(2).extracting(IndexHistoryEntry::versionId).isEqualTo(2);
    assertThat(index.history()).last().extracting(IndexHistoryEntry::versionId).isEqualTo(3);
  }

  @Test
  public void versionHistoryEntryMaintainCorrectTimeline() {
    // Each version must have different properties to avoid deduplication
    IndexVersion indexVersionOne = newIndexVersion(1, 1000, ImmutableMap.of("v", "1"));
    IndexVersion indexVersionTwo = newIndexVersion(2, 2000, ImmutableMap.of("v", "2"));
    IndexVersion indexVersionThree = newIndexVersion(3, 3000, ImmutableMap.of("v", "3"));

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .build();

    indexMetadata =
        IndexMetadata.buildFrom(indexMetadata).setCurrentVersion(indexVersionTwo).build();

    // setting an existing index version as the new current should update the timestamp in the
    // history
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).setCurrentVersion(indexVersionOne).build();

    List<IndexHistoryEntry> history = updated.history();
    assertThat(history)
        .hasSize(3)
        .element(0)
        .isEqualTo(ImmutableIndexHistoryEntry.builder().versionId(1).timestampMillis(1000).build());
    assertThat(history)
        .element(1)
        .isEqualTo(ImmutableIndexHistoryEntry.builder().versionId(2).timestampMillis(2000).build());
    assertThat(history)
        .element(2)
        .satisfies(
            v -> {
              assertThat(v.versionId()).isEqualTo(1);
              assertThat(v.timestampMillis())
                  .isGreaterThan(3000)
                  .isLessThanOrEqualTo(System.currentTimeMillis());
            });

    // adding a new index version and setting it as current should use the index version's timestamp
    // in the history (which has been set to a fixed value for testing)
    updated = IndexMetadata.buildFrom(updated).setCurrentVersion(indexVersionThree).build();
    List<IndexHistoryEntry> historyTwo = updated.history();
    assertThat(historyTwo)
        .hasSize(4)
        .last()
        .satisfies(
            v -> {
              assertThat(v.versionId()).isEqualTo(3);
              assertThat(v.timestampMillis()).isEqualTo(3000);
            });

    // setting an older index version as the new current (aka doing a rollback) should update the
    // timestamp in the history
    IndexMetadata reactiveOldIndexVersion =
        IndexMetadata.buildFrom(updated).setCurrentVersion(indexVersionOne).build();
    List<IndexHistoryEntry> historyThree = reactiveOldIndexVersion.history();
    assertThat(historyThree)
        .hasSize(5)
        .last()
        .satisfies(
            v -> {
              assertThat(v.versionId()).isEqualTo(1);
              assertThat(v.timestampMillis())
                  .isGreaterThan(3000)
                  .isLessThanOrEqualTo(System.currentTimeMillis());
            });
  }

  @Test
  public void indexMetadataAndMetadataChanges() {
    // Each version must have different properties to avoid deduplication
    IndexVersion indexVersionOne = newIndexVersion(1, 1000L, ImmutableMap.of("key1", "prop1"));
    IndexVersion indexVersionTwo = newIndexVersion(2, 2000L, ImmutableMap.of("key2", "prop2"));
    IndexVersion indexVersionThree = newIndexVersion(3, 3000L, ImmutableMap.of("key3", "prop3"));

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1, 2))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .setCurrentVersion(indexVersionTwo)
            .setCurrentVersion(indexVersionThree)
            .build();

    assertThat(indexMetadata.versions())
        .hasSize(3)
        .containsExactly(indexVersionOne, indexVersionTwo, indexVersionThree);
    assertThat(indexMetadata.history()).hasSize(1);
    assertThat(indexMetadata.currentVersionId()).isEqualTo(3);
    assertThat(indexMetadata.currentVersion()).isEqualTo(indexVersionThree);
    assertThat(indexMetadata.formatVersion()).isEqualTo(IndexMetadata.DEFAULT_INDEX_FORMAT_VERSION);
    assertThat(indexMetadata.type()).isEqualTo(IndexType.BTREE);
    assertThat(indexMetadata.indexColumnIds()).isEqualTo(ImmutableList.of(1, 2));
    assertThat(indexMetadata.optimizedColumnIds()).isEqualTo(ImmutableList.of(1));
    assertThat(indexMetadata.location()).isEqualTo("custom-location");

    List<IndexUpdate> changes = indexMetadata.changes();
    assertThat(changes).hasSize(4);

    assertThat(changes)
        .element(0)
        .isInstanceOf(IndexUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetLocation.class))
        .extracting(IndexUpdate.SetLocation::location)
        .isEqualTo("custom-location");

    assertThat(changes)
        .element(1)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(2)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(3)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::indexVersion)
        .isEqualTo(indexVersionThree);
  }

  @Test
  public void indexVersionIDReassignment() {
    // all index versions have the same ID but different properties so they won't be deduplicated
    IndexVersion indexVersionOne = newIndexVersion(1000, 1000L, ImmutableMap.of("v", "1"));
    IndexVersion indexVersionTwo = newIndexVersion(1001, 2000L, ImmutableMap.of("v", "2"));
    IndexVersion indexVersionThree = newIndexVersion(1002, 3000L, ImmutableMap.of("v", "3"));

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .setCurrentVersion(indexVersionTwo)
            .setCurrentVersion(indexVersionThree)
            .build();

    assertThat(indexMetadata.currentVersion())
        .isEqualTo(ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());

    // IDs of the index versions should be re-assigned
    assertThat(indexMetadata.versions())
        .hasSize(3)
        .containsExactly(
            ImmutableIndexVersion.builder().from(indexVersionOne).versionId(1).build(),
            ImmutableIndexVersion.builder().from(indexVersionTwo).versionId(2).build(),
            ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());
  }

  @Test
  public void indexVersionDeduplication() {
    // all index versions have the same ID
    // additionally, there are duplicate index versions that only differ in their creation timestamp
    IndexVersion indexVersionOne = newIndexVersion(1, 1000L);
    IndexVersion indexVersionTwo = newIndexVersion(2, 2000L, ImmutableMap.of("key", "value"));
    IndexVersion indexVersionThree = newIndexVersion(3, 3000L, ImmutableMap.of("key2", "value2"));
    IndexVersion indexVersionOneUpdated =
        ImmutableIndexVersion.builder()
            .from(indexVersionOne)
            .versionId(4)
            .timestampMillis(4000)
            .build();
    IndexVersion indexVersionTwoUpdated =
        ImmutableIndexVersion.builder()
            .from(indexVersionTwo)
            .versionId(5)
            .timestampMillis(5000)
            .build();
    IndexVersion indexVersionThreeUpdated =
        ImmutableIndexVersion.builder()
            .from(indexVersionThree)
            .versionId(6)
            .timestampMillis(6000)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(indexVersionOne)
            .setCurrentVersion(indexVersionTwo)
            .setCurrentVersion(indexVersionThree)
            .setCurrentVersion(indexVersionOneUpdated)
            .setCurrentVersion(indexVersionTwoUpdated)
            .setCurrentVersion(indexVersionThreeUpdated)
            .build();

    assertThat(indexMetadata.currentVersion())
        .isEqualTo(ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());

    // IDs of the index versions should be re-assigned and index versions should be de-duplicated
    assertThat(indexMetadata.versions())
        .hasSize(3)
        .containsExactly(
            indexVersionOne,
            ImmutableIndexVersion.builder().from(indexVersionTwo).versionId(2).build(),
            ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());
  }

  @Test
  public void snapshotsById() {
    IndexVersion version = newIndexVersion(1, 1000L);
    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .build();
    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(101L)
            .indexSnapshotId(201L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(version)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(2).containsExactly(snapshot1, snapshot2);
    assertThat(indexMetadata.snapshot(200L)).isEqualTo(snapshot1);
    assertThat(indexMetadata.snapshot(201L)).isEqualTo(snapshot2);
    assertThat(indexMetadata.snapshotForTableSnapshot(100L)).isEqualTo(snapshot1);
    assertThat(indexMetadata.snapshotForTableSnapshot(101L)).isEqualTo(snapshot2);
  }

  @Test
  public void addSnapshotWithUnknownVersionId() {
    IndexVersion version = newIndexVersion(1, 1000L);
    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(999) // unknown version id
            .build();

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("custom-location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(version)
                    .addSnapshot(snapshot)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index version id. Cannot add snapshot with unknown version id: 999");
  }

  @Test
  public void addDuplicateSnapshotForTableSnapshot() {
    IndexVersion version = newIndexVersion(1, 1000L);
    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .build();
    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L) // same table snapshot id
            .indexSnapshotId(201L)
            .versionId(1)
            .build();

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("custom-location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(version)
                    .addSnapshot(snapshot1)
                    .addSnapshot(snapshot2)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid table snapshot id. Snapshot for table snapshot 100 already added to the index.");
  }

  @Test
  public void removeSnapshots() {
    IndexVersion version = newIndexVersion(1, 1000L);
    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .build();
    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(101L)
            .indexSnapshotId(201L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(version)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(2);

    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(200L)).build();
    assertThat(updated.snapshots()).hasSize(1).containsExactly(snapshot2);
  }

  @Test
  public void removeNonExistentSnapshots() {
    IndexVersion version = newIndexVersion(1, 1000L);
    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .upgradeFormatVersion(1)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(ImmutableList.of(1))
            .setOptimizedColumnIds(ImmutableList.of(1))
            .setCurrentVersion(version)
            .addSnapshot(snapshot)
            .build();

    indexMetadata =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(999L)).build();

    assertThat(indexMetadata.snapshots()).hasSize(1).containsExactly(snapshot);
  }

  @Test
  public void indexTypeIsRequired() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("location")
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(newIndexVersion(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index type: null");
  }

  @Test
  public void indexColumnIdsAreRequired() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setOptimizedColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(newIndexVersion(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Index column IDs cannot be empty");
  }

  @Test
  public void optimizedColumnIdsAreRequired() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(1)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(ImmutableList.of(1))
                    .setCurrentVersion(newIndexVersion(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Optimized column IDs cannot be empty");
  }
}
