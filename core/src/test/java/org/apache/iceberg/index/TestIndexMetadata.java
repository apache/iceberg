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
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class TestIndexMetadata {

  private static final List<Integer> INDEX_COLUMN_IDS = ImmutableList.of(1);
  private static final List<Integer> OPTIMIZED_COLUMN_IDS = ImmutableList.of(1);

  private IndexVersion newIndexVersion(int id) {
    return newIndexVersion(id, System.currentTimeMillis());
  }

  private IndexVersion newIndexVersion(int id, long timestampMillis) {
    return ImmutableIndexVersion.builder()
        .versionId(id)
        .timestampMillis(timestampMillis)
        .properties(ImmutableMap.of())
        .build();
  }

  private IndexVersion newIndexVersion(int id, Map<String, String> properties) {
    return ImmutableIndexVersion.builder()
        .versionId(id)
        .timestampMillis(System.currentTimeMillis())
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

    assertThatThrownBy(
            () -> IndexMetadata.builder().setLocation("location").setCurrentVersionId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 1");

    assertThatThrownBy(() -> IndexMetadata.builder().assignUUID(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set uuid to null");
  }

  @Test
  public void unsupportedFormatVersion() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(23)
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(
                        ImmutableIndexVersion.builder()
                            .versionId(1)
                            .timestampMillis(23L)
                            .properties(ImmutableMap.of())
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format version: 23");

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .upgradeFormatVersion(0)
                    .setLocation("location")
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v1 index to v0");
  }

  @Test
  public void emptyIndexVersion() {
    assertThatThrownBy(
            () -> IndexMetadata.builder().setLocation("location").setCurrentVersionId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 1");
  }

  @Test
  public void invalidCurrentVersionId() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(
                        ImmutableIndexVersion.builder()
                            .versionId(1)
                            .timestampMillis(23L)
                            .properties(ImmutableMap.of())
                            .build())
                    .setCurrentVersionId(23)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 23");
  }

  @Test
  public void invalidVersionHistorySizeToKeep() {
    // Each version must have different properties to avoid deduplication
    Map<String, String> props1 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "0", "v", "1");
    Map<String, String> props2 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "0", "v", "2");
    Map<String, String> props3 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "0", "v", "3");
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(newIndexVersion(1, props1))
                    .addVersion(newIndexVersion(2, props2))
                    .addVersion(newIndexVersion(3, props3))
                    .setCurrentVersionId(3)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("version.history.num-entries must be positive but was 0");
  }

  @Test
  public void indexVersionHistoryNormalization() {
    // Each version must have different properties to avoid deduplication
    Map<String, String> properties1 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "1");
    Map<String, String> properties2 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "2");
    Map<String, String> properties3 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "3");
    IndexVersion indexVersionOne = newIndexVersion(1, properties1);
    IndexVersion indexVersionTwo = newIndexVersion(2, properties2);
    IndexVersion indexVersionThree = newIndexVersion(3, properties3);

    IndexMetadata originalIndexMetadata =
        IndexMetadata.builder()
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .addVersion(indexVersionThree)
            .setCurrentVersionId(3)
            .build();

    // the first build will not expire versions that were added in the builder
    assertThat(originalIndexMetadata.versions()).hasSize(3);
    assertThat(originalIndexMetadata.history()).hasSize(1);

    // rebuild the metadata to expire older versions
    IndexMetadata indexMetadata = IndexMetadata.buildFrom(originalIndexMetadata).build();
    assertThat(indexMetadata.versions()).hasSize(2);
    assertThat(indexMetadata.history()).hasSize(1);

    // make sure that metadata changes reflect the current state after the history was adjusted,
    // meaning that the first index version shouldn't be included
    List<IndexUpdate> changes = originalIndexMetadata.changes();
    assertThat(changes).hasSize(5);
    assertThat(changes)
        .element(0)
        .isInstanceOf(IndexUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetLocation.class))
        .extracting(IndexUpdate.SetLocation::location)
        .isEqualTo("location");

    assertThat(changes)
        .element(1)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(2)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(3)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionThree);

    assertThat(changes)
        .element(4)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::versionId)
        .isEqualTo(-1);
  }

  @Test
  public void indexVersionHistoryIsCorrectlyRetained() {
    // Each version must have different properties to avoid deduplication
    Map<String, String> properties1 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "1");
    Map<String, String> properties2 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "2");
    Map<String, String> properties3 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "3");
    IndexVersion indexVersionOne = newIndexVersion(1, properties1);
    IndexVersion indexVersionTwo = newIndexVersion(2, properties2);
    IndexVersion indexVersionThree = newIndexVersion(3, properties3);

    IndexMetadata originalIndexMetadata =
        IndexMetadata.builder()
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .addVersion(indexVersionThree)
            .setCurrentVersionId(3)
            .build();

    assertThat(originalIndexMetadata.versions())
        .hasSize(3)
        .containsExactlyInAnyOrder(indexVersionOne, indexVersionTwo, indexVersionThree);
    assertThat(originalIndexMetadata.history())
        .hasSize(1)
        .first()
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);

    // rebuild the metadata to expire older versions
    IndexMetadata indexMetadata = IndexMetadata.buildFrom(originalIndexMetadata).build();
    assertThat(indexMetadata.versions())
        .hasSize(2)
        // there is no requirement about the order of versions
        .containsExactlyInAnyOrder(indexVersionThree, indexVersionTwo);
    assertThat(indexMetadata.history())
        .hasSize(1)
        .first()
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);

    IndexMetadata updated = IndexMetadata.buildFrom(indexMetadata).setCurrentVersionId(2).build();
    assertThat(updated.versions())
        .hasSize(2)
        .containsExactlyInAnyOrder(indexVersionTwo, indexVersionThree);
    assertThat(updated.history())
        .hasSize(2)
        .element(0)
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);
    assertThat(updated.history()).element(1).extracting(IndexHistoryEntry::versionId).isEqualTo(2);

    IndexMetadata index = IndexMetadata.buildFrom(updated).setCurrentVersionId(3).build();
    assertThat(index.versions())
        .hasSize(2)
        .containsExactlyInAnyOrder(indexVersionTwo, indexVersionThree);
    assertThat(index.history())
        .hasSize(3)
        .element(0)
        .extracting(IndexHistoryEntry::versionId)
        .isEqualTo(3);
    assertThat(index.history()).element(1).extracting(IndexHistoryEntry::versionId).isEqualTo(2);
    assertThat(index.history()).element(2).extracting(IndexHistoryEntry::versionId).isEqualTo(3);

    // indexVersionId 1 has been removed from versions, so this should fail
    assertThatThrownBy(() -> IndexMetadata.buildFrom(index).setCurrentVersionId(1).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set current version to unknown version: 1");
  }

  @Test
  public void versionHistoryEntryMaintainCorrectTimeline() {
    // Each version must have different properties to avoid deduplication
    Map<String, String> props1 = ImmutableMap.of("v", "1");
    Map<String, String> props2 = ImmutableMap.of("v", "2");
    Map<String, String> props3 = ImmutableMap.of("v", "3");
    IndexVersion indexVersionOne =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(1000)
            .properties(props1)
            .build();
    IndexVersion indexVersionTwo =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(2000)
            .properties(props2)
            .build();
    IndexVersion indexVersionThree =
        ImmutableIndexVersion.builder()
            .versionId(3)
            .timestampMillis(3000)
            .properties(props3)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .setCurrentVersionId(1)
            .build();

    // setting an existing index version as the new current should update the timestamp in the
    // history
    IndexMetadata updated = IndexMetadata.buildFrom(indexMetadata).setCurrentVersionId(2).build();

    List<IndexHistoryEntry> history = updated.history();
    assertThat(history)
        .hasSize(2)
        .element(0)
        .isEqualTo(ImmutableIndexHistoryEntry.builder().versionId(1).timestampMillis(1000).build());
    assertThat(history)
        .element(1)
        .satisfies(
            v -> {
              assertThat(v.versionId()).isEqualTo(2);
              assertThat(v.timestampMillis())
                  .isGreaterThan(3000)
                  .isLessThanOrEqualTo(System.currentTimeMillis());
            });

    // adding a new index version and setting it as current should use the index version's timestamp
    // in the history (which has been set to a fixed value for testing)
    updated =
        IndexMetadata.buildFrom(updated)
            .addVersion(indexVersionThree)
            .setCurrentVersionId(3)
            .build();
    List<IndexHistoryEntry> historyTwo = updated.history();
    assertThat(historyTwo)
        .hasSize(3)
        .containsAll(history)
        .element(2)
        .isEqualTo(ImmutableIndexHistoryEntry.builder().versionId(3).timestampMillis(3000).build());

    // setting an older index version as the new current (aka doing a rollback) should update the
    // timestamp in the history
    IndexMetadata reactiveOldIndexVersion =
        IndexMetadata.buildFrom(updated).setCurrentVersionId(1).build();
    List<IndexHistoryEntry> historyThree = reactiveOldIndexVersion.history();
    assertThat(historyThree)
        .hasSize(4)
        .containsAll(historyTwo)
        .element(3)
        .satisfies(
            v -> {
              assertThat(v.versionId()).isEqualTo(1);
              assertThat(v.timestampMillis())
                  .isGreaterThan(3000)
                  .isLessThanOrEqualTo(System.currentTimeMillis());
            });
  }

  @Test
  public void versionsAddedInCurrentBuildAreRetained() {
    Map<String, String> propertiesV1 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "1");
    Map<String, String> propertiesV2 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "2");
    Map<String, String> propertiesV3 =
        ImmutableMap.of(IndexProperties.VERSION_HISTORY_SIZE, "2", "v", "3");
    IndexVersion v1 = newIndexVersion(1, propertiesV1);
    IndexVersion v2 = newIndexVersion(2, propertiesV2);
    IndexVersion v3 = newIndexVersion(3, propertiesV3);

    IndexMetadata metadata =
        IndexMetadata.builder()
            .setLocation("location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(v1)
            .setCurrentVersionId(v1.versionId())
            .build();
    assertThat(metadata.versions()).containsOnly(v1);

    // make sure all currently added versions are retained
    IndexMetadata updated = IndexMetadata.buildFrom(metadata).addVersion(v2).addVersion(v3).build();
    assertThat(updated.versions()).containsExactlyInAnyOrder(v1, v2, v3);

    // rebuild the metadata to expire older versions
    updated = IndexMetadata.buildFrom(updated).build();
    assertThat(updated.versions()).containsExactlyInAnyOrder(v1, v3);
  }

  @Test
  public void indexMetadataAndMetadataChanges() {
    // Each version must have different properties to avoid deduplication
    Map<String, String> props1 = ImmutableMap.of("v", "1");
    Map<String, String> props2 = ImmutableMap.of("v", "2");
    Map<String, String> props3 = ImmutableMap.of("v", "3");
    IndexVersion indexVersionOne =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(System.currentTimeMillis())
            .properties(props1)
            .build();
    IndexVersion indexVersionTwo =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(System.currentTimeMillis())
            .properties(props2)
            .build();
    IndexVersion indexVersionThree =
        ImmutableIndexVersion.builder()
            .versionId(3)
            .timestampMillis(System.currentTimeMillis())
            .properties(props3)
            .build();

    String uuid = "fa6506c3-7681-40c8-86dc-e36561f83385";
    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .assignUUID(uuid)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .addVersion(indexVersionThree)
            .setCurrentVersionId(3)
            .build();

    assertThat(indexMetadata.versions())
        .hasSize(3)
        .containsExactly(indexVersionOne, indexVersionTwo, indexVersionThree);
    assertThat(indexMetadata.history()).hasSize(1);
    assertThat(indexMetadata.currentVersionId()).isEqualTo(3);
    assertThat(indexMetadata.currentVersion()).isEqualTo(indexVersionThree);
    assertThat(indexMetadata.formatVersion()).isEqualTo(IndexMetadata.DEFAULT_INDEX_FORMAT_VERSION);
    assertThat(indexMetadata.location()).isEqualTo("custom-location");
    assertThat(indexMetadata.type()).isEqualTo(IndexType.BTREE);
    assertThat(indexMetadata.indexColumnIds()).isEqualTo(INDEX_COLUMN_IDS);
    assertThat(indexMetadata.optimizedColumnIds()).isEqualTo(OPTIMIZED_COLUMN_IDS);

    List<IndexUpdate> changes = indexMetadata.changes();
    assertThat(changes).hasSize(6);
    assertThat(changes)
        .element(0)
        .isInstanceOf(IndexUpdate.AssignUUID.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AssignUUID.class))
        .extracting(IndexUpdate.AssignUUID::uuid)
        .isEqualTo(uuid);

    assertThat(changes)
        .element(1)
        .isInstanceOf(IndexUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetLocation.class))
        .extracting(IndexUpdate.SetLocation::location)
        .isEqualTo("custom-location");

    assertThat(changes)
        .element(2)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(3)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(4)
        .isInstanceOf(IndexUpdate.AddVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddVersion.class))
        .extracting(IndexUpdate.AddVersion::indexVersion)
        .isEqualTo(indexVersionThree);

    assertThat(changes)
        .element(5)
        .isInstanceOf(IndexUpdate.SetCurrentVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.SetCurrentVersion.class))
        .extracting(IndexUpdate.SetCurrentVersion::versionId)
        .isEqualTo(-1);
  }

  @Test
  public void uuidAssignment() {
    String uuid = "fa6506c3-7681-40c8-86dc-e36561f83385";
    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .assignUUID(uuid)
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(
                ImmutableIndexVersion.builder()
                    .versionId(1)
                    .timestampMillis(23L)
                    .properties(ImmutableMap.of())
                    .build())
            .setCurrentVersionId(1)
            .build();

    assertThat(indexMetadata.uuid()).isEqualTo(uuid);

    // uuid should be carried over
    IndexMetadata updated = IndexMetadata.buildFrom(indexMetadata).build();
    assertThat(updated.uuid()).isEqualTo(uuid);
    assertThat(updated.changes()).isEmpty();

    // assigning the same uuid shouldn't fail and shouldn't cause any changes
    updated = IndexMetadata.buildFrom(indexMetadata).assignUUID(uuid).build();
    assertThat(updated.uuid()).isEqualTo(uuid);
    assertThat(updated.changes()).isEmpty();

    // assigning a new uuid shouldn't fail and generate the correct change
    String newUuid = UUID.randomUUID().toString();
    updated = IndexMetadata.buildFrom(indexMetadata).assignUUID(newUuid).build();
    assertThat(updated.uuid()).isEqualTo(newUuid);
    assertThat(updated.changes())
        .hasSize(1)
        .element(0)
        .isInstanceOf(IndexUpdate.AssignUUID.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AssignUUID.class))
        .extracting(IndexUpdate.AssignUUID::uuid)
        .isEqualTo(newUuid);
  }

  @Test
  public void indexMetadataWithMetadataLocation() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("custom-location")
                    .setMetadataLocation("metadata-location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(indexVersion)
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create index metadata with a metadata location and changes");

    // setting metadata location without changes is ok
    IndexMetadata indexMetadata =
        IndexMetadata.buildFrom(
                IndexMetadata.builder()
                    .setLocation("custom-location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(indexVersion)
                    .setCurrentVersionId(1)
                    .build())
            .setMetadataLocation("metadata-location")
            .build();
    assertThat(indexMetadata.metadataFileLocation()).isEqualTo("metadata-location");
  }

  @Test
  public void indexVersionIDReassignment() {
    // all index versions have the same ID
    IndexVersion indexVersionOne = newIndexVersion(1);
    IndexVersion indexVersionTwo =
        ImmutableIndexVersion.builder()
            .from(indexVersionOne)
            .properties(ImmutableMap.of("key", "value1"))
            .build();
    IndexVersion indexVersionThree =
        ImmutableIndexVersion.builder()
            .from(indexVersionOne)
            .properties(ImmutableMap.of("key", "value2"))
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .addVersion(indexVersionThree)
            .setCurrentVersionId(3)
            .build();

    assertThat(indexMetadata.currentVersion())
        .isEqualTo(ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());

    // IDs of the index versions should be re-assigned
    assertThat(indexMetadata.versions())
        .hasSize(3)
        .containsExactly(
            indexVersionOne,
            ImmutableIndexVersion.builder().from(indexVersionTwo).versionId(2).build(),
            ImmutableIndexVersion.builder().from(indexVersionThree).versionId(3).build());
  }

  @Test
  public void indexVersionDeduplication() {
    // all index versions have the same ID
    // additionally, there are duplicate index versions that only differ in their creation timestamp
    IndexVersion indexVersionOne = newIndexVersion(1);
    IndexVersion indexVersionTwo =
        ImmutableIndexVersion.builder()
            .from(indexVersionOne)
            .properties(ImmutableMap.of("key", "value1"))
            .build();
    IndexVersion indexVersionThree =
        ImmutableIndexVersion.builder()
            .from(indexVersionOne)
            .properties(ImmutableMap.of("key", "value2"))
            .build();
    IndexVersion indexVersionOneUpdated =
        ImmutableIndexVersion.builder().from(indexVersionOne).timestampMillis(1000).build();
    IndexVersion indexVersionTwoUpdated =
        ImmutableIndexVersion.builder().from(indexVersionTwo).timestampMillis(100).build();
    IndexVersion indexVersionThreeUpdated =
        ImmutableIndexVersion.builder().from(indexVersionThree).timestampMillis(10).build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersionOne)
            .addVersion(indexVersionTwo)
            .addVersion(indexVersionThree)
            .addVersion(indexVersionOneUpdated)
            .addVersion(indexVersionTwoUpdated)
            .addVersion(indexVersionThreeUpdated)
            .setCurrentVersionId(3)
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
  public void addSnapshot() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(1).containsExactly(snapshot);
    assertThat(indexMetadata.snapshot(1L)).isEqualTo(snapshot);
    assertThat(indexMetadata.snapshotForTableSnapshot(100L)).isEqualTo(snapshot);
  }

  @Test
  public void addSnapshotAndConcurrentAddVersion() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshotOne =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshotOne)
            .build();

    IndexMetadata updateMetadata =
        IndexMetadata.buildFrom(indexMetadata)
            .addVersion(
                ImmutableIndexVersion.builder()
                    .versionId(2)
                    .timestampMillis(45L)
                    .properties(ImmutableMap.of("one", "two"))
                    .build())
            .setCurrentVersionId(2)
            .build();

    IndexSnapshot snapshotTwo =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(2)
            .build();

    List<IndexUpdate> addSnapshotAndAddVersionUpdate =
        IndexMetadata.buildFrom(indexMetadata)
            .addVersion(
                ImmutableIndexVersion.builder()
                    .versionId(2)
                    .timestampMillis(45L)
                    .properties(ImmutableMap.of("three", "four"))
                    .build())
            .setCurrentVersionId(2)
            .addSnapshot(snapshotTwo)
            .build()
            .changes();

    IndexMetadata.Builder metadataBuilder = IndexMetadata.buildFrom(updateMetadata);
    addSnapshotAndAddVersionUpdate.forEach(update -> update.applyTo(metadataBuilder));
    IndexMetadata finalMetadata = metadataBuilder.build();

    IndexSnapshot expectedSnapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(3)
            .build();

    assertThat(finalMetadata.snapshots()).hasSize(2).containsExactly(snapshotOne, expectedSnapshot);
    assertThat(finalMetadata.snapshot(1L)).isEqualTo(snapshotOne);
    assertThat(finalMetadata.snapshotForTableSnapshot(100L)).isEqualTo(snapshotOne);
    assertThat(finalMetadata.snapshot(2L)).isEqualTo(expectedSnapshot);
    assertThat(finalMetadata.snapshotForTableSnapshot(200L)).isEqualTo(expectedSnapshot);
  }

  @Test
  public void addDuplicateSnapshot() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("custom-location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(indexVersion)
                    .setCurrentVersionId(1)
                    .addSnapshot(snapshot)
                    .addSnapshot(snapshot)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add snapshot with duplicate id: 1");
  }

  @Test
  public void missingIndexType() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(
                        ImmutableIndexVersion.builder()
                            .versionId(1)
                            .timestampMillis(23L)
                            .properties(ImmutableMap.of())
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index type: null");
  }

  @Test
  public void missingIndexColumnIds() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
                    .addVersion(
                        ImmutableIndexVersion.builder()
                            .versionId(1)
                            .timestampMillis(23L)
                            .properties(ImmutableMap.of())
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Index column IDs cannot be empty");
  }

  @Test
  public void missingOptimizedColumnIds() {
    assertThatThrownBy(
            () ->
                IndexMetadata.builder()
                    .setLocation("location")
                    .setType(IndexType.BTREE)
                    .setIndexColumnIds(INDEX_COLUMN_IDS)
                    .addVersion(
                        ImmutableIndexVersion.builder()
                            .versionId(1)
                            .timestampMillis(23L)
                            .properties(ImmutableMap.of())
                            .build())
                    .setCurrentVersionId(1)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Optimized column IDs cannot be empty");
  }

  @Test
  public void updateIndexVersionProperties() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of("key1", "value1", "key2", "value2"))
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .build();

    assertThat(indexMetadata.currentVersion().properties())
        .containsEntry("key1", "value1")
        .containsEntry("key2", "value2");

    // Add a new version with updated properties
    IndexVersion newVersion =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(100L)
            .properties(ImmutableMap.of("key1", "updated1", "key3", "value3"))
            .build();

    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata)
            .addVersion(newVersion)
            .setCurrentVersionId(2)
            .build();

    assertThat(updated.currentVersion().properties())
        .containsEntry("key1", "updated1")
        .containsEntry("key3", "value3")
        .doesNotContainKey("key2");

    assertThat(updated.versions()).hasSize(2);
    assertThat(updated.history()).hasSize(2);
  }

  @Test
  public void addMultipleSnapshots() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot3 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(300L)
            .indexSnapshotId(3L)
            .versionId(1)
            .properties(ImmutableMap.of("prop1", "val1"))
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .addSnapshot(snapshot3)
            .build();

    assertThat(indexMetadata.snapshots())
        .hasSize(3)
        .containsExactly(snapshot1, snapshot2, snapshot3);

    // Verify lookup by index snapshot ID
    assertThat(indexMetadata.snapshot(1L)).isEqualTo(snapshot1);
    assertThat(indexMetadata.snapshot(2L)).isEqualTo(snapshot2);
    assertThat(indexMetadata.snapshot(3L)).isEqualTo(snapshot3);
    assertThat(indexMetadata.snapshot(999L)).isNull();

    // Verify lookup by table snapshot ID
    assertThat(indexMetadata.snapshotForTableSnapshot(100L)).isEqualTo(snapshot1);
    assertThat(indexMetadata.snapshotForTableSnapshot(200L)).isEqualTo(snapshot2);
    assertThat(indexMetadata.snapshotForTableSnapshot(300L)).isEqualTo(snapshot3);
    assertThat(indexMetadata.snapshotForTableSnapshot(999L)).isNull();

    // Verify snapshot properties
    assertThat(indexMetadata.snapshot(3L).properties()).containsEntry("prop1", "val1");
  }

  @Test
  public void addSnapshotToExistingMetadata() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(1);

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexMetadata updated = IndexMetadata.buildFrom(indexMetadata).addSnapshot(snapshot2).build();

    assertThat(updated.snapshots()).hasSize(2).containsExactly(snapshot1, snapshot2);

    // Verify the change was recorded
    assertThat(updated.changes())
        .hasSize(1)
        .first()
        .isInstanceOf(IndexUpdate.AddSnapshot.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.AddSnapshot.class))
        .extracting(IndexUpdate.AddSnapshot::indexSnapshot)
        .isEqualTo(snapshot2);
  }

  @Test
  public void snapshotWithDifferentVersionIds() {
    IndexVersion version1 =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of("v", "1"))
            .build();

    IndexVersion version2 =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(100L)
            .properties(ImmutableMap.of("v", "2"))
            .build();

    IndexSnapshot snapshotV1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshotV2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(2)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(version1)
            .addVersion(version2)
            .setCurrentVersionId(2)
            .addSnapshot(snapshotV1)
            .addSnapshot(snapshotV2)
            .build();

    assertThat(indexMetadata.snapshot(1L).versionId()).isEqualTo(1);
    assertThat(indexMetadata.snapshot(2L).versionId()).isEqualTo(2);
  }

  @Test
  public void indexTypes() {
    for (IndexType type : IndexType.values()) {
      IndexMetadata indexMetadata =
          IndexMetadata.builder()
              .setLocation("location")
              .setType(type)
              .setIndexColumnIds(INDEX_COLUMN_IDS)
              .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
              .addVersion(
                  ImmutableIndexVersion.builder()
                      .versionId(1)
                      .timestampMillis(23L)
                      .properties(ImmutableMap.of())
                      .build())
              .setCurrentVersionId(1)
              .build();

      assertThat(indexMetadata.type()).isEqualTo(type);
    }
  }

  @Test
  public void removeSnapshotById() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot3 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(300L)
            .indexSnapshotId(3L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .addSnapshot(snapshot3)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(3);

    // Remove a single snapshot
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(2L)).build();

    assertThat(updated.snapshots()).hasSize(2).containsExactlyInAnyOrder(snapshot1, snapshot3);
    assertThat(updated.snapshot(1L)).isEqualTo(snapshot1);
    assertThat(updated.snapshot(2L)).isNull();
    assertThat(updated.snapshot(3L)).isEqualTo(snapshot3);

    // Verify the change was recorded
    assertThat(updated.changes())
        .hasSize(1)
        .first()
        .isInstanceOf(IndexUpdate.RemoveSnapshots.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.RemoveSnapshots.class))
        .extracting(IndexUpdate.RemoveSnapshots::indexSnapshotIds)
        .isEqualTo(ImmutableSet.of(2L));
  }

  @Test
  public void removeMultipleSnapshots() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot3 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(300L)
            .indexSnapshotId(3L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .addSnapshot(snapshot3)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(3);

    // Remove multiple snapshots at once
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(1L, 3L)).build();

    assertThat(updated.snapshots()).hasSize(1).containsExactly(snapshot2);
    assertThat(updated.snapshot(1L)).isNull();
    assertThat(updated.snapshot(2L)).isEqualTo(snapshot2);
    assertThat(updated.snapshot(3L)).isNull();
  }

  @Test
  public void removeAllSnapshots() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(2);

    // Remove all snapshots
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(1L, 2L)).build();

    assertThat(updated.snapshots()).isEmpty();
    assertThat(updated.snapshot(1L)).isNull();
    assertThat(updated.snapshot(2L)).isNull();
  }

  @Test
  public void removeNonExistentSnapshot() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(1);

    // Removing a non-existent snapshot should not cause an error
    // but also should not add a change since nothing was removed
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(999L)).build();

    assertThat(updated.snapshots()).hasSize(1).containsExactly(snapshot1);
    assertThat(updated.changes()).isEmpty();
  }

  @Test
  public void removeSnapshotsWithNullOrEmptySet() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .build();

    // Null should throw an exception
    assertThatThrownBy(() -> IndexMetadata.buildFrom(indexMetadata).removeSnapshots(null).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot remove snapshots");

    // Empty set should throw an exception
    assertThatThrownBy(
            () -> IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of()).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot remove snapshots");
  }

  @Test
  public void removeSnapshotsMixedExistentAndNonExistent() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .build();

    assertThat(indexMetadata.snapshots()).hasSize(2);

    // Remove a mix of existent and non-existent snapshots
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata).removeSnapshots(ImmutableSet.of(1L, 999L)).build();

    // Only snapshot1 should be removed, snapshot2 remains
    assertThat(updated.snapshots()).hasSize(1).containsExactly(snapshot2);
    assertThat(updated.snapshot(1L)).isNull();
    assertThat(updated.snapshot(2L)).isEqualTo(snapshot2);

    // The change should include both IDs that were requested for removal
    assertThat(updated.changes())
        .hasSize(1)
        .first()
        .isInstanceOf(IndexUpdate.RemoveSnapshots.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IndexUpdate.RemoveSnapshots.class))
        .extracting(IndexUpdate.RemoveSnapshots::indexSnapshotIds)
        .isEqualTo(ImmutableSet.of(1L, 999L));
  }

  @Test
  public void removeSnapshotsAndAddSnapshot() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(1L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(200L)
            .indexSnapshotId(2L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot3 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(300L)
            .indexSnapshotId(3L)
            .versionId(1)
            .build();

    IndexMetadata indexMetadata =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .addSnapshot(snapshot1)
            .addSnapshot(snapshot2)
            .build();

    // Remove a snapshot and add a new one in the same update
    IndexMetadata updated =
        IndexMetadata.buildFrom(indexMetadata)
            .removeSnapshots(ImmutableSet.of(1L))
            .addSnapshot(snapshot3)
            .build();

    assertThat(updated.snapshots()).hasSize(2).containsExactlyInAnyOrder(snapshot2, snapshot3);
    assertThat(updated.snapshot(1L)).isNull();
    assertThat(updated.snapshot(2L)).isEqualTo(snapshot2);
    assertThat(updated.snapshot(3L)).isEqualTo(snapshot3);

    // Verify both changes were recorded
    assertThat(updated.changes()).hasSize(2);
    assertThat(updated.changes().get(0)).isInstanceOf(IndexUpdate.RemoveSnapshots.class);
    assertThat(updated.changes().get(1)).isInstanceOf(IndexUpdate.AddSnapshot.class);
  }

  @Test
  public void versionConcurrency() {
    IndexVersion indexVersion =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(23L)
            .properties(ImmutableMap.of())
            .build();

    IndexMetadata originalIndex =
        IndexMetadata.builder()
            .setLocation("custom-location")
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(indexVersion)
            .setCurrentVersionId(1)
            .build();

    // Add version with new properties based on the originalIndex, call the result a newIndex
    IndexVersion newVersionOne =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(100L)
            .properties(ImmutableMap.of("first", "update"))
            .build();

    IndexMetadata newIndex =
        IndexMetadata.buildFrom(originalIndex).setCurrentVersion(newVersionOne).build();

    List<IndexUpdate> firstUpdateChanges = newIndex.changes();

    // Add version with another set of new properties based on the originalIndex
    IndexVersion newVersionTwo =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(200L)
            .properties(ImmutableMap.of("second", "update"))
            .build();

    IndexMetadata secondUpdate =
        IndexMetadata.buildFrom(originalIndex)
            .setCurrentVersion(newVersionTwo)
            .build();

    // Validate secondUpdate state
    assertThat(secondUpdate.versions()).hasSize(2);
    assertThat(secondUpdate.version(1)).isNotNull();
    assertThat(secondUpdate.version(2)).isNotNull();
    assertThat(secondUpdate.version(2).properties()).containsEntry("second", "update");
    assertThat(secondUpdate.currentVersionId()).isEqualTo(2);

    List<IndexUpdate> secondUpdateChanges = secondUpdate.changes();

    // Apply the changes from the second update to the newIndex
    IndexMetadata.Builder builderAfterSecond = IndexMetadata.buildFrom(newIndex);
    secondUpdateChanges.forEach(update -> update.applyTo(builderAfterSecond));
    IndexMetadata indexAfterSecondApplied = builderAfterSecond.build();

    // Verify that the final index has both new versions added
    assertThat(indexAfterSecondApplied.versions()).hasSize(3);
    assertThat(indexAfterSecondApplied.version(1)).isNotNull();
    assertThat(indexAfterSecondApplied.version(2)).isNotNull();
    assertThat(indexAfterSecondApplied.version(3)).isNotNull();
    assertThat(indexAfterSecondApplied.version(2).properties()).containsEntry("first", "update");
    assertThat(indexAfterSecondApplied.version(3).properties()).containsEntry("second", "update");
    assertThat(indexAfterSecondApplied.currentVersionId()).isEqualTo(3);

    // Apply the changes from the first update to the index resulting from the second update
    IndexMetadata.Builder builderAfterThird = IndexMetadata.buildFrom(indexAfterSecondApplied);
    firstUpdateChanges.forEach(update -> update.applyTo(builderAfterThird));
    IndexMetadata indexAfterFirstAppliedAgain = builderAfterThird.build();

    // Verify that the final index has both new versions added, and the current one is the first
    // version added
    assertThat(indexAfterFirstAppliedAgain.versions()).hasSize(3);
    assertThat(indexAfterFirstAppliedAgain.version(1)).isNotNull();
    assertThat(indexAfterFirstAppliedAgain.version(2)).isNotNull();
    assertThat(indexAfterFirstAppliedAgain.version(3)).isNotNull();
    assertThat(indexAfterFirstAppliedAgain.version(2).properties())
        .containsEntry("first", "update");
    assertThat(indexAfterFirstAppliedAgain.version(3).properties())
        .containsEntry("second", "update");
    // The current version should be the one from the first update
    assertThat(indexAfterFirstAppliedAgain.currentVersionId()).isEqualTo(2);
  }
}
