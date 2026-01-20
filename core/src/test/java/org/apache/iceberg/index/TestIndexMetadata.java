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
import org.apache.iceberg.MetadataUpdate;
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
    List<MetadataUpdate> changes = originalIndexMetadata.changes();
    assertThat(changes).hasSize(5);
    assertThat(changes)
        .element(0)
        .isInstanceOf(MetadataUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetLocation.class))
        .extracting(MetadataUpdate.SetLocation::location)
        .isEqualTo("location");

    assertThat(changes)
        .element(1)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(2)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionThree);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.SetCurrentIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentIndexVersion.class))
        .extracting(MetadataUpdate.SetCurrentIndexVersion::versionId)
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

    List<MetadataUpdate> changes = indexMetadata.changes();
    assertThat(changes).hasSize(6);
    assertThat(changes)
        .element(0)
        .isInstanceOf(MetadataUpdate.AssignUUID.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AssignUUID.class))
        .extracting(MetadataUpdate.AssignUUID::uuid)
        .isEqualTo(uuid);

    assertThat(changes)
        .element(1)
        .isInstanceOf(MetadataUpdate.SetLocation.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetLocation.class))
        .extracting(MetadataUpdate.SetLocation::location)
        .isEqualTo("custom-location");

    assertThat(changes)
        .element(2)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionOne);

    assertThat(changes)
        .element(3)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionTwo);

    assertThat(changes)
        .element(4)
        .isInstanceOf(MetadataUpdate.AddIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.AddIndexVersion.class))
        .extracting(MetadataUpdate.AddIndexVersion::indexVersion)
        .isEqualTo(indexVersionThree);

    assertThat(changes)
        .element(5)
        .isInstanceOf(MetadataUpdate.SetCurrentIndexVersion.class)
        .asInstanceOf(InstanceOfAssertFactories.type(MetadataUpdate.SetCurrentIndexVersion.class))
        .extracting(MetadataUpdate.SetCurrentIndexVersion::versionId)
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

    // can't reassign index uuid
    assertThatThrownBy(
            () ->
                IndexMetadata.buildFrom(indexMetadata)
                    .assignUUID(UUID.randomUUID().toString())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot reassign uuid");
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
}
