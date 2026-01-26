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

import java.util.Map;
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

    assertThat(update).isInstanceOf(IndexUpdate.AddIndexSnapshot.class);
    IndexUpdate.AddIndexSnapshot addIndexSnapshot = (IndexUpdate.AddIndexSnapshot) update;
    assertThat(addIndexSnapshot.indexSnapshot().tableSnapshotId())
        .isEqualTo(INDEX_SNAPSHOT.tableSnapshotId());
    assertThat(addIndexSnapshot.indexSnapshot().indexSnapshotId())
        .isEqualTo(INDEX_SNAPSHOT.indexSnapshotId());
    assertThat(addIndexSnapshot.indexSnapshot().versionId()).isEqualTo(INDEX_SNAPSHOT.versionId());
    assertThat(addIndexSnapshot.indexSnapshot().properties())
        .isEqualTo(INDEX_SNAPSHOT.properties());
  }

  @Test
  public void testAddIndexSnapshotToJson() {
    String snapshotJson = IndexSnapshotParser.toJson(INDEX_SNAPSHOT);
    String expected =
        String.format(
            "{\"action\":\"%s\",\"snapshot\":%s}", IndexUpdateParser.ADD_SNAPSHOT, snapshotJson);

    IndexUpdate update = new IndexUpdate.AddIndexSnapshot(INDEX_SNAPSHOT);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("AddIndexSnapshot should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAddIndexSnapshotRoundTrip() {
    IndexUpdate original = new IndexUpdate.AddIndexSnapshot(INDEX_SNAPSHOT);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.AddIndexSnapshot.class);
    IndexUpdate.AddIndexSnapshot parsedUpdate = (IndexUpdate.AddIndexSnapshot) parsed;
    assertThat(parsedUpdate.indexSnapshot()).isEqualTo(INDEX_SNAPSHOT);
  }

  /** RemoveIndexSnapshots */
  @Test
  public void testRemoveIndexSnapshotsFromJson() {
    String action = IndexUpdateParser.REMOVE_SNAPSHOTS;
    Set<Long> snapshotIds = ImmutableSet.of(1L, 2L, 3L);
    String json = String.format("{\"action\":\"%s\",\"snapshot-ids\":[1,2,3]}", action);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.RemoveIndexSnapshots.class);
    IndexUpdate.RemoveIndexSnapshots removeSnapshots = (IndexUpdate.RemoveIndexSnapshots) update;
    assertThat(removeSnapshots.indexSnapshotIds()).isEqualTo(snapshotIds);
  }

  @Test
  public void testRemoveIndexSnapshotsToJson() {
    Set<Long> snapshotIds = ImmutableSet.of(1L, 2L, 3L);
    IndexUpdate update = new IndexUpdate.RemoveIndexSnapshots(snapshotIds);
    String json = IndexUpdateParser.toJson(update);

    assertThat(json).contains("\"action\":\"remove-snapshots\"");
    assertThat(json).contains("\"snapshot-ids\":");

    // Verify round-trip
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveIndexSnapshots.class);
    assertThat(((IndexUpdate.RemoveIndexSnapshots) parsed).indexSnapshotIds())
        .isEqualTo(snapshotIds);
  }

  @Test
  public void testRemoveIndexSnapshotsSingleId() {
    IndexUpdate update = new IndexUpdate.RemoveIndexSnapshots(42L);
    String json = IndexUpdateParser.toJson(update);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveIndexSnapshots.class);
    assertThat(((IndexUpdate.RemoveIndexSnapshots) parsed).indexSnapshotIds()).containsExactly(42L);
  }

  /** SetIndexCurrentVersion */
  @Test
  public void testSetIndexCurrentVersionFromJson() {
    String action = IndexUpdateParser.SET_CURRENT_VERSION;
    int versionId = 5;
    String json = String.format("{\"action\":\"%s\",\"version-id\":%d}", action, versionId);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.SetIndexCurrentVersion.class);
    IndexUpdate.SetIndexCurrentVersion setCurrentVersion =
        (IndexUpdate.SetIndexCurrentVersion) update;
    assertThat(setCurrentVersion.versionId()).isEqualTo(versionId);
  }

  @Test
  public void testSetIndexCurrentVersionToJson() {
    int versionId = 5;
    String expected =
        String.format(
            "{\"action\":\"%s\",\"version-id\":%d}",
            IndexUpdateParser.SET_CURRENT_VERSION, versionId);

    IndexUpdate update = new IndexUpdate.SetIndexCurrentVersion(versionId);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("SetIndexCurrentVersion should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetIndexCurrentVersionRoundTrip() {
    int versionId = 42;
    IndexUpdate original = new IndexUpdate.SetIndexCurrentVersion(versionId);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetIndexCurrentVersion.class);
    assertThat(((IndexUpdate.SetIndexCurrentVersion) parsed).versionId()).isEqualTo(versionId);
  }

  /** AddIndexVersion */
  @Test
  public void testAddIndexVersionFromJson() {
    String action = IndexUpdateParser.ADD_VERSION;
    String versionJson = IndexVersionParser.toJson(INDEX_VERSION);
    String json = String.format("{\"action\":\"%s\",\"version\":%s}", action, versionJson);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.AddIndexVersion.class);
    IndexUpdate.AddIndexVersion addVersion = (IndexUpdate.AddIndexVersion) update;
    assertThat(addVersion.indexVersion().versionId()).isEqualTo(INDEX_VERSION.versionId());
    assertThat(addVersion.indexVersion().timestampMillis())
        .isEqualTo(INDEX_VERSION.timestampMillis());
    assertThat(addVersion.indexVersion().properties()).isEqualTo(INDEX_VERSION.properties());
  }

  @Test
  public void testAddIndexVersionToJson() {
    String versionJson = IndexVersionParser.toJson(INDEX_VERSION);
    String expected =
        String.format(
            "{\"action\":\"%s\",\"version\":%s}", IndexUpdateParser.ADD_VERSION, versionJson);

    IndexUpdate update = new IndexUpdate.AddIndexVersion(INDEX_VERSION);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("AddIndexVersion should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAddIndexVersionRoundTrip() {
    IndexUpdate original = new IndexUpdate.AddIndexVersion(INDEX_VERSION);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.AddIndexVersion.class);
    IndexUpdate.AddIndexVersion parsedUpdate = (IndexUpdate.AddIndexVersion) parsed;
    assertThat(parsedUpdate.indexVersion()).isEqualTo(INDEX_VERSION);
  }

  @Test
  public void testAddIndexVersionWithoutProperties() {
    IndexVersion versionWithoutProps =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(67890L)
            .properties(ImmutableMap.of())
            .build();

    IndexUpdate original = new IndexUpdate.AddIndexVersion(versionWithoutProps);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.AddIndexVersion.class);
    IndexUpdate.AddIndexVersion parsedUpdate = (IndexUpdate.AddIndexVersion) parsed;
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

    assertThat(update).isInstanceOf(IndexUpdate.SetIndexLocation.class);
    IndexUpdate.SetIndexLocation setLocation = (IndexUpdate.SetIndexLocation) update;
    assertThat(setLocation.location()).isEqualTo(location);
  }

  @Test
  public void testSetLocationToJson() {
    String location = "s3://bucket/warehouse/index_location";
    String expected =
        String.format(
            "{\"action\":\"%s\",\"location\":\"%s\"}", IndexUpdateParser.SET_LOCATION, location);

    IndexUpdate update = new IndexUpdate.SetIndexLocation(location);
    String actual = IndexUpdateParser.toJson(update);

    assertThat(actual)
        .as("SetIndexLocation should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetLocationRoundTrip() {
    String location = "hdfs://namenode/warehouse/index";
    IndexUpdate original = new IndexUpdate.SetIndexLocation(location);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetIndexLocation.class);
    assertThat(((IndexUpdate.SetIndexLocation) parsed).location()).isEqualTo(location);
  }

  /** SetIndexProperties */
  @Test
  public void testSetPropertiesFromJson() {
    String action = IndexUpdateParser.SET_PROPERTIES;
    String json =
        String.format(
            "{\"action\":\"%s\",\"updates\":{\"key1\":\"value1\",\"key2\":\"value2\"}}", action);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.SetIndexProperties.class);
    IndexUpdate.SetIndexProperties setProperties = (IndexUpdate.SetIndexProperties) update;
    assertThat(setProperties.updated())
        .containsEntry("key1", "value1")
        .containsEntry("key2", "value2");
  }

  @Test
  public void testSetPropertiesToJson() {
    Map<String, String> props = ImmutableMap.of("key1", "value1", "key2", "value2");
    IndexUpdate update = new IndexUpdate.SetIndexProperties(props);
    String json = IndexUpdateParser.toJson(update);

    assertThat(json).contains("\"action\":\"set-properties\"");
    assertThat(json).contains("\"updates\":");
    assertThat(json).contains("\"key1\":\"value1\"");
    assertThat(json).contains("\"key2\":\"value2\"");
  }

  @Test
  public void testSetPropertiesRoundTrip() {
    Map<String, String> props = ImmutableMap.of("prop1", "val1", "prop2", "val2");
    IndexUpdate original = new IndexUpdate.SetIndexProperties(props);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.SetIndexProperties.class);
    assertThat(((IndexUpdate.SetIndexProperties) parsed).updated()).isEqualTo(props);
  }

  /** RemoveIndexProperties */
  @Test
  public void testRemovePropertiesFromJson() {
    String action = IndexUpdateParser.REMOVE_PROPERTIES;
    String json = String.format("{\"action\":\"%s\",\"removals\":[\"key1\",\"key2\"]}", action);

    IndexUpdate update = IndexUpdateParser.fromJson(json);

    assertThat(update).isInstanceOf(IndexUpdate.RemoveIndexProperties.class);
    IndexUpdate.RemoveIndexProperties removeProperties = (IndexUpdate.RemoveIndexProperties) update;
    assertThat(removeProperties.removed()).containsExactlyInAnyOrder("key1", "key2");
  }

  @Test
  public void testRemovePropertiesToJson() {
    Set<String> toRemove = ImmutableSet.of("key1", "key2");
    IndexUpdate update = new IndexUpdate.RemoveIndexProperties(toRemove);
    String json = IndexUpdateParser.toJson(update);

    assertThat(json).contains("\"action\":\"remove-properties\"");
    assertThat(json).contains("\"removals\":");

    // Verify round-trip
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);
    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveIndexProperties.class);
    assertThat(((IndexUpdate.RemoveIndexProperties) parsed).removed()).isEqualTo(toRemove);
  }

  @Test
  public void testRemovePropertiesRoundTrip() {
    Set<String> toRemove = ImmutableSet.of("prop1", "prop2", "prop3");
    IndexUpdate original = new IndexUpdate.RemoveIndexProperties(toRemove);
    String json = IndexUpdateParser.toJson(original);
    IndexUpdate parsed = IndexUpdateParser.fromJson(json);

    assertThat(parsed).isInstanceOf(IndexUpdate.RemoveIndexProperties.class);
    assertThat(((IndexUpdate.RemoveIndexProperties) parsed).removed()).isEqualTo(toRemove);
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
  public void testSetIndexCurrentVersionMissingVersionId() {
    String json = "{\"action\":\"set-current-version\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing int: version-id");
  }

  @Test
  public void testAddIndexVersionMissingVersion() {
    String json = "{\"action\":\"add-version\"}";

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

  @Test
  public void testSetPropertiesMissingUpdates() {
    String json = "{\"action\":\"set-properties\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing map: updates");
  }

  @Test
  public void testRemovePropertiesMissingRemovals() {
    String json = "{\"action\":\"remove-properties\"}";

    assertThatThrownBy(() -> IndexUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing set: removals");
  }
}
