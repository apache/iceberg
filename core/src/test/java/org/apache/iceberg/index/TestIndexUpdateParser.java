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

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

public class TestIndexUpdateParser {

  private static final IndexSnapshot INDEX_SNAPSHOT =
      ImmutableIndexSnapshot.builder()
          .tableSnapshotId(100L)
          .indexSnapshotId(200L)
          .versionId(1)
          .properties(ImmutableMap.of("user-key", "user-value"))
          .build();

  private static final IndexVersion INDEX_VERSION =
      ImmutableIndexVersion.builder()
          .versionId(1)
          .timestampMillis(12345L)
          .properties(ImmutableMap.of("version-key", "version-value"))
          .build();

  @Test
  public void testIndexUpdateWithoutActionCannotDeserialize() {
    assertThatThrownBy(() -> IndexUpdateParser.fromJson("{\"action\":null}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index update. Missing field: action");

    assertThatThrownBy(() -> IndexUpdateParser.fromJson("{\"version-id\":1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index update. Missing field: action");
  }

  @Test
  public void testIndexUpdateWithInvalidActionCannotDeserialize() {
    assertThatThrownBy(() -> IndexUpdateParser.fromJson("{\"action\":\"invalid-action\"}"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot convert index update action from json: invalid-action");
  }

  /** AddIndexSnapshot */
  @Test
  public void testAddIndexSnapshotFromJson() {
    String action = IndexUpdateParser.ADD_SNAPSHOT;
    String snapshotJson = IndexSnapshotParser.toJson(INDEX_SNAPSHOT);
    String json = String.format("{\"action\":\"%s\",\"snapshot\":%s}", action, snapshotJson);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.AddSnapshot.class);
    IndexUpdate.AddSnapshot addSnapshot = (IndexUpdate.AddSnapshot) update;
    assertThat(addSnapshot.indexSnapshot().tableSnapshotId())
        .isEqualTo(INDEX_SNAPSHOT.tableSnapshotId());
    assertThat(addSnapshot.indexSnapshot().indexSnapshotId())
        .isEqualTo(INDEX_SNAPSHOT.indexSnapshotId());
    assertThat(addSnapshot.indexSnapshot().versionId()).isEqualTo(INDEX_SNAPSHOT.versionId());
    assertThat(addSnapshot.indexSnapshot().properties()).isEqualTo(INDEX_SNAPSHOT.properties());
  }

  @Test
  public void testAddIndexSnapshotToJson() {
    String snapshotJson = IndexSnapshotParser.toJson(INDEX_SNAPSHOT);
    String expected =
        String.format(
            "{\"action\":\"%s\",\"snapshot\":%s}", IndexUpdateParser.ADD_SNAPSHOT, snapshotJson);

    IndexUpdate update = new IndexUpdate.AddSnapshot(INDEX_SNAPSHOT);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("AddIndexSnapshot should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAddIndexSnapshotRoundTrip() {
    IndexUpdate original = new IndexUpdate.AddSnapshot(INDEX_SNAPSHOT);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.AddSnapshot.class);
    IndexUpdate.AddSnapshot parsedUpdate = (IndexUpdate.AddSnapshot) parsed;
    assertThat(parsedUpdate.indexSnapshot()).isEqualTo(INDEX_SNAPSHOT);
  }

  /** RemoveIndexSnapshots */
  @Test
  public void testRemoveIndexSnapshotsFromJson() {
    String action = IndexUpdateParser.REMOVE_SNAPSHOTS;
    Set<Long> snapshotIds = ImmutableSet.of(1L, 2L, 3L);
    String json = String.format("{\"action\":\"%s\",\"snapshot-ids\":[1,2,3]}", action);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.RemoveSnapshots.class);
    IndexUpdate.RemoveSnapshots removeSnapshots = (IndexUpdate.RemoveSnapshots) update;
    assertThat(removeSnapshots.indexSnapshotIds()).isEqualTo(snapshotIds);
  }

  @Test
  public void testRemoveIndexSnapshotsToJson() {
    Set<Long> snapshotIds = ImmutableSet.of(1L, 2L, 3L);
    IndexUpdate update = new IndexUpdate.RemoveSnapshots(snapshotIds);
    String json = IndexUpdateParser.toJson(update);

    assertThat(json).contains("\"action\":\"remove-snapshots\"");
    assertThat(json).contains("\"snapshot-ids\":");

    // Verify round-trip
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveSnapshots.class);
    assertThat(((IndexUpdate.RemoveSnapshots) parsed).indexSnapshotIds()).isEqualTo(snapshotIds);
  }

  @Test
  public void testRemoveIndexSnapshotsSingleId() {
    IndexUpdate update = new IndexUpdate.RemoveSnapshots(42L);
    String json = IndexUpdateParser.toJson(update);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveSnapshots.class);
    assertThat(((IndexUpdate.RemoveSnapshots) parsed).indexSnapshotIds()).containsExactly(42L);
  }

  /** SetIndexCurrentVersion */
  @Test
  public void testSetIndexCurrentVersionFromJson() {
    String action = IndexUpdateParser.SET_CURRENT_VERSION;
    String versionJson = IndexVersionParser.toJson(INDEX_VERSION);
    String json = String.format("{\"action\":\"%s\",\"version\":%s}", action, versionJson);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.SetCurrentVersion.class);
    IndexUpdate.SetCurrentVersion setCurrentVersion = (IndexUpdate.SetCurrentVersion) update;
    assertThat(setCurrentVersion.indexVersion().versionId()).isEqualTo(INDEX_VERSION.versionId());
  }

  @Test
  public void testSetIndexCurrentVersionToJson() {
    String versionJson = IndexVersionParser.toJson(INDEX_VERSION);
    String expected =
        String.format(
            "{\"action\":\"%s\",\"version\":%s}",
            IndexUpdateParser.SET_CURRENT_VERSION, versionJson);

    IndexUpdate update = new IndexUpdate.SetCurrentVersion(INDEX_VERSION);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("SetIndexCurrentVersion should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetIndexCurrentVersionRoundTrip() {
    IndexUpdate original = new IndexUpdate.SetCurrentVersion(INDEX_VERSION);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetCurrentVersion.class);
    assertThat(((IndexUpdate.SetCurrentVersion) parsed).indexVersion().versionId())
        .isEqualTo(INDEX_VERSION.versionId());
  }

  @Test
  public void testSetIndexCurrentVersionWithoutProperties() {
    IndexVersion versionWithoutProps =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(67890L)
            .properties(ImmutableMap.of())
            .build();

    IndexUpdate original = new IndexUpdate.SetCurrentVersion(versionWithoutProps);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetCurrentVersion.class);
    IndexUpdate.SetCurrentVersion parsedUpdate = (IndexUpdate.SetCurrentVersion) parsed;
    assertThat(parsedUpdate.indexVersion().versionId()).isEqualTo(2);
    assertThat(parsedUpdate.indexVersion().timestampMillis()).isEqualTo(67890L);
    assertThat(parsedUpdate.indexVersion().properties()).isEmpty();
  }

  /** SetIndexLocation */
  @Test
  public void testSetLocationFromJson() {
    String action = IndexUpdateParser.SET_LOCATION;
    String location = "s3://bucket/warehouse/index_location";
    String json = String.format("{\"action\":\"%s\",\"location\":\"%s\"}", action, location);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.SetLocation.class);
    IndexUpdate.SetLocation setLocation = (IndexUpdate.SetLocation) update;
    assertThat(setLocation.location()).isEqualTo(location);
  }

  @Test
  public void testSetLocationToJson() {
    String location = "s3://bucket/warehouse/index_location";
    String expected =
        String.format(
            "{\"action\":\"%s\",\"location\":\"%s\"}", IndexUpdateParser.SET_LOCATION, location);

    IndexUpdate update = new IndexUpdate.SetLocation(location);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("SetIndexLocation should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetLocationRoundTrip() {
    String location = "hdfs://namenode/warehouse/index";
    IndexUpdate original = new IndexUpdate.SetLocation(location);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetLocation.class);
    assertThat(((IndexUpdate.SetLocation) parsed).location()).isEqualTo(location);
  }

  /** Error cases */
  @Test
  public void testFromJsonWithNullNode() {
    assertThatThrownBy(() -> IndexUpdateParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse index update from null string");
  }

  @Test
  public void testToJsonWithUnrecognizedUpdateType() {
    // Create a custom IndexUpdate that's not in the ACTIONS map
    IndexUpdate unknownUpdate =
        new IndexUpdate() {
          @Override
          public void applyTo(IndexMetadata.Builder indexMetadataBuilder) {
            // no-op
          }
        };

    assertThatThrownBy(() -> IndexUpdateParser.toJson(unknownUpdate))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unrecognized index update type");
  }

  @Test
  public void testAddIndexSnapshotMissingSnapshot() {
    String json = "{\"action\":\"add-snapshot\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: snapshot");
  }

  @Test
  public void testRemoveIndexSnapshotsMissingIds() {
    String json = "{\"action\":\"remove-snapshots\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be non-null");
  }

  @Test
  public void testSetIndexCurrentVersionMissingVersion() {
    String json = "{\"action\":\"set-current-version\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing field: version");
  }

  @Test
  public void testSetLocationMissingLocation() {
    String json = "{\"action\":\"set-location\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing string: location");
  }

  /** UpgradeFormatVersion */
  @Test
  public void testUpgradeFormatVersionFromJson() {
    String action = IndexUpdateParser.UPGRADE_FORMAT_VERSION;
    int formatVersion = 2;
    String json = String.format("{\"action\":\"%s\",\"format-version\":%d}", action, formatVersion);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.UpgradeFormatVersion.class);
    IndexUpdate.UpgradeFormatVersion upgradeFormatVersion =
        (IndexUpdate.UpgradeFormatVersion) update;
    assertThat(upgradeFormatVersion.formatVersion()).isEqualTo(formatVersion);
  }

  @Test
  public void testUpgradeFormatVersionToJson() {
    int formatVersion = 2;
    String expected =
        String.format(
            "{\"action\":\"%s\",\"format-version\":%d}",
            IndexUpdateParser.UPGRADE_FORMAT_VERSION, formatVersion);

    IndexUpdate update = new IndexUpdate.UpgradeFormatVersion(formatVersion);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("UpgradeFormatVersion should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testUpgradeFormatVersionRoundTrip() {
    int formatVersion = 3;
    IndexUpdate original = new IndexUpdate.UpgradeFormatVersion(formatVersion);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.UpgradeFormatVersion.class);
    assertThat(((IndexUpdate.UpgradeFormatVersion) parsed).formatVersion())
        .isEqualTo(formatVersion);
  }

  @Test
  public void testUpgradeFormatVersionMissingFormatVersion() {
    String json = "{\"action\":\"upgrade-format-version\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing int: format-version");
  }
}
