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

import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestMetadataUpdateParser {

  @TempDir private Path temp;

  private static final Schema ID_DATA_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  public void testMetadataUpdateWithoutActionCannotDeserialize() {
    List<String> invalidJson =
        ImmutableList.of("{\"action\":null,\"format-version\":2}", "{\"format-version\":2}");

    for (String json : invalidJson) {
      assertThatThrownBy(() -> MetadataUpdateParser.fromJson(json))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cannot parse metadata update. Missing field: action");
    }
  }

  /** AssignUUID * */
  @Test
  public void testAssignUUIDToJson() {
    String action = MetadataUpdateParser.ASSIGN_UUID;
    String uuid = "9510c070-5e6d-4b40-bf40-a8915bb76e5d";
    String json = "{\"action\":\"assign-uuid\",\"uuid\":\"9510c070-5e6d-4b40-bf40-a8915bb76e5d\"}";
    MetadataUpdate expected = new MetadataUpdate.AssignUUID(uuid);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAssignUUIDFromJson() {
    String uuid = "9510c070-5e6d-4b40-bf40-a8915bb76e5d";
    String expected =
        "{\"action\":\"assign-uuid\",\"uuid\":\"9510c070-5e6d-4b40-bf40-a8915bb76e5d\"}";
    MetadataUpdate actual = new MetadataUpdate.AssignUUID(uuid);
    assertThat(MetadataUpdateParser.toJson(actual))
        .as("Assign UUID should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** UpgradeFormatVersion * */
  @Test
  public void testUpgradeFormatVersionToJson() {
    int formatVersion = 2;
    String action = MetadataUpdateParser.UPGRADE_FORMAT_VERSION;
    String json = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion expected =
        new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testUpgradeFormatVersionFromJson() {
    int formatVersion = 2;
    String expected = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion actual =
        new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    assertThat(MetadataUpdateParser.toJson(actual))
        .as("Upgrade format version should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** AddSchema * */
  @Test
  public void testAddSchemaFromJson() {
    String action = MetadataUpdateParser.ADD_SCHEMA;
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String json =
        String.format(
            "{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
            SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate actualUpdate = new MetadataUpdate.AddSchema(schema, lastColumnId);
    assertEquals(action, actualUpdate, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddSchemaFromJsonWithoutLastColumnId() {
    String action = MetadataUpdateParser.ADD_SCHEMA;
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String json =
        String.format("{\"action\":\"add-schema\",\"schema\":%s}", SchemaParser.toJson(schema));
    MetadataUpdate actualUpdate = new MetadataUpdate.AddSchema(schema, lastColumnId);
    assertEquals(action, actualUpdate, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddSchemaToJson() {
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String expected =
        String.format(
            "{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
            SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate update = new MetadataUpdate.AddSchema(schema, lastColumnId);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Add schema should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** SetCurrentSchema * */
  @Test
  public void testSetCurrentSchemaFromJson() {
    String action = MetadataUpdateParser.SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String json = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate.SetCurrentSchema expected = new MetadataUpdate.SetCurrentSchema(schemaId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetCurrentSchemaToJson() {
    String action = MetadataUpdateParser.SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String expected = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate update = new MetadataUpdate.SetCurrentSchema(schemaId);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Set current schema should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** AddPartitionSpec * */
  @Test
  public void testAddPartitionSpecFromJsonWithFieldId() {
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString =
        "{"
            + "\"spec-id\":1,"
            + "\"fields\":[{"
            + "\"name\":\"id_bucket\","
            + "\"transform\":\"bucket[8]\","
            + "\"source-id\":1,"
            + "\"field-id\":1000"
            + "},{"
            + "\"name\":\"data_bucket\","
            + "\"transform\":\"bucket[16]\","
            + "\"source-id\":2,"
            + "\"field-id\":1001"
            + "}]"
            + "}";

    UnboundPartitionSpec actualSpec =
        PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String json =
        String.format(
            "{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    // Partition spec order declaration needs to match declaration in spec string to be assigned the
    // same field ids.
    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(ID_DATA_SCHEMA)
            .bucket("id", 8)
            .bucket("data", 16)
            .withSpecId(1)
            .build();
    MetadataUpdate expected = new MetadataUpdate.AddPartitionSpec(expectedSpec);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddPartitionSpecFromJsonWithoutFieldId() {
    // partition field ids are missing in old PartitionSpec, they always auto-increment from 1000 in
    // declared order
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString =
        "{"
            + "\"spec-id\":1,"
            + "\"fields\":[{"
            + "\"name\":\"id_bucket\","
            + "\"transform\":\"bucket[8]\","
            + "\"source-id\":1"
            + "},{"
            + "\"name\": \"data_bucket\","
            + "\"transform\":\"bucket[16]\","
            + "\"source-id\":2"
            + "}]"
            + "}";

    UnboundPartitionSpec actualSpec =
        PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String json =
        String.format(
            "{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(ID_DATA_SCHEMA)
            .bucket("id", 8)
            .bucket("data", 16)
            .withSpecId(1)
            .build();
    MetadataUpdate expected = new MetadataUpdate.AddPartitionSpec(expectedSpec.toUnbound());
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddPartitionSpecToJson() {
    String action = MetadataUpdateParser.ADD_PARTITION_SPEC;
    String specString =
        "{"
            + "\"spec-id\":1,"
            + "\"fields\":[{"
            + "\"name\":\"id_bucket\","
            + "\"transform\":\"bucket[8]\","
            + "\"source-id\":1,"
            + "\"field-id\":1000"
            + "},{"
            + "\"name\":\"data_bucket\","
            + "\"transform\":\"bucket[16]\","
            + "\"source-id\":2,"
            + "\"field-id\":1001"
            + "}]"
            + "}";

    UnboundPartitionSpec actualSpec =
        PartitionSpecParser.fromJson(ID_DATA_SCHEMA, specString).toUnbound();
    String expected =
        String.format(
            "{\"action\":\"%s\",\"spec\":%s}", action, PartitionSpecParser.toJson(actualSpec));

    // Partition spec order declaration needs to match declaration in spec string to be assigned the
    // same field ids.
    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(ID_DATA_SCHEMA)
            .bucket("id", 8)
            .bucket("data", 16)
            .withSpecId(1)
            .build();
    MetadataUpdate update = new MetadataUpdate.AddPartitionSpec(expectedSpec);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Add partition spec should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** SetDefaultPartitionSpec * */
  @Test
  public void testSetDefaultPartitionSpecToJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String expected = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate update = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Set default partition spec should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetDefaultPartitionSpecFromJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String json = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate.SetDefaultPartitionSpec expected =
        new MetadataUpdate.SetDefaultPartitionSpec(specId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  /** AddSortOrder * */
  @Test
  public void testAddSortOrderToJson() {
    String action = MetadataUpdateParser.ADD_SORT_ORDER;
    UnboundSortOrder sortOrder =
        SortOrder.builderFor(ID_DATA_SCHEMA)
            .withOrderId(3)
            .asc("id", NullOrder.NULLS_FIRST)
            .desc("data")
            .build()
            .toUnbound();

    String expected =
        String.format(
            "{\"action\":\"%s\",\"sort-order\":%s}", action, SortOrderParser.toJson(sortOrder));
    MetadataUpdate update = new MetadataUpdate.AddSortOrder(sortOrder);
    assertThat(MetadataUpdateParser.toJson(update))
        .as("Add sort order should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAddSortOrderFromJson() {
    String action = MetadataUpdateParser.ADD_SORT_ORDER;
    UnboundSortOrder sortOrder =
        SortOrder.builderFor(ID_DATA_SCHEMA)
            .withOrderId(3)
            .asc("id", NullOrder.NULLS_FIRST)
            .desc("data")
            .build()
            .toUnbound();

    String json =
        String.format(
            "{\"action\":\"%s\",\"sort-order\":%s}", action, SortOrderParser.toJson(sortOrder));
    MetadataUpdate.AddSortOrder expected = new MetadataUpdate.AddSortOrder(sortOrder);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  /** SetDefaultSortOrder * */
  @Test
  public void testSetDefaultSortOrderToJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_SORT_ORDER;
    int sortOrderId = 2;
    String expected =
        String.format("{\"action\":\"%s\",\"sort-order-id\":%d}", action, sortOrderId);
    MetadataUpdate update = new MetadataUpdate.SetDefaultSortOrder(sortOrderId);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Set default sort order should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetDefaultSortOrderFromJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_SORT_ORDER;
    int sortOrderId = 2;
    String json = String.format("{\"action\":\"%s\",\"sort-order-id\":%d}", action, sortOrderId);
    MetadataUpdate expected = new MetadataUpdate.SetDefaultSortOrder(sortOrderId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  /** AddSnapshot * */
  @Test
  public void testAddSnapshotToJson() throws IOException {
    String action = MetadataUpdateParser.ADD_SNAPSHOT;
    long parentId = 1;
    long snapshotId = 2;
    int schemaId = 3;

    String manifestList = createManifestListWithManifestFiles(snapshotId, parentId);

    Snapshot snapshot =
        new BaseSnapshot(
            0,
            snapshotId,
            parentId,
            System.currentTimeMillis(),
            DataOperations.REPLACE,
            ImmutableMap.of("files-added", "4", "files-deleted", "100"),
            schemaId,
            manifestList);
    String snapshotJson = SnapshotParser.toJson(snapshot, /* pretty */ false);
    String expected = String.format("{\"action\":\"%s\",\"snapshot\":%s}", action, snapshotJson);
    MetadataUpdate update = new MetadataUpdate.AddSnapshot(snapshot);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Add snapshot should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testAddSnapshotFromJson() throws IOException {
    String action = MetadataUpdateParser.ADD_SNAPSHOT;
    long parentId = 1;
    long snapshotId = 2;
    int schemaId = 3;
    Map<String, String> summary = ImmutableMap.of("files-added", "4", "files-deleted", "100");

    String manifestList = createManifestListWithManifestFiles(snapshotId, parentId);
    Snapshot snapshot =
        new BaseSnapshot(
            0,
            snapshotId,
            parentId,
            System.currentTimeMillis(),
            DataOperations.REPLACE,
            summary,
            schemaId,
            manifestList);
    String snapshotJson = SnapshotParser.toJson(snapshot, /* pretty */ false);
    String json = String.format("{\"action\":\"%s\",\"snapshot\":%s}", action, snapshotJson);
    MetadataUpdate expected = new MetadataUpdate.AddSnapshot(snapshot);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  /** RemoveSnapshots * */
  @Test
  public void testRemoveSnapshotsFromJson() {
    String action = MetadataUpdateParser.REMOVE_SNAPSHOTS;
    long snapshotId = 2L;
    String json = String.format("{\"action\":\"%s\",\"snapshot-ids\":[2]}", action);
    MetadataUpdate expected = new MetadataUpdate.RemoveSnapshot(snapshotId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testRemoveSnapshotsToJson() {
    String action = MetadataUpdateParser.REMOVE_SNAPSHOTS;
    long snapshotId = 2L;
    String expected = String.format("{\"action\":\"%s\",\"snapshot-ids\":[2]}", action);
    MetadataUpdate update = new MetadataUpdate.RemoveSnapshot(snapshotId);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Remove snapshots should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  /** RemoveSnapshotRef * */
  @Test
  public void testRemoveSnapshotRefFromJson() {
    String action = MetadataUpdateParser.REMOVE_SNAPSHOT_REF;
    String snapshotRef = "snapshot-ref";
    String json = "{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"snapshot-ref\"}";
    MetadataUpdate expected = new MetadataUpdate.RemoveSnapshotRef(snapshotRef);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testRemoveSnapshotRefToJson() {
    String snapshotRef = "snapshot-ref";
    String expected = "{\"action\":\"remove-snapshot-ref\",\"ref-name\":\"snapshot-ref\"}";
    MetadataUpdate actual = new MetadataUpdate.RemoveSnapshotRef(snapshotRef);
    assertThat(MetadataUpdateParser.toJson(actual))
        .as("RemoveSnapshotRef should convert to the correct JSON value")
        .isEqualTo(expected);
  }

  /** SetSnapshotRef * */
  @Test
  public void testSetSnapshotRefTagFromJsonDefault_NullValuesMissing() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"tag\"}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefTagFromJsonDefault_ExplicitNullValues() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"tag\","
            + "\"min-snapshots-to-keep\":null,\"max-snapshot-age-ms\":null,\"max-ref-age-ms\":null}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefTagFromJsonAllFields_NullValuesMissing() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = 1L;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\","
            + "\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefTagFromJsonAllFields_ExplicitNullValues() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = 1L;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"tag\","
            + "\"max-ref-age-ms\":1,\"min-snapshots-to-keep\":null,\"max-snapshot-age-ms\":null}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefBranchFromJsonDefault_NullValuesMissing() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.BRANCH;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"bRaNch\"}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefBranchFromJsonDefault_ExplicitNullValues() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.BRANCH;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"bRaNch\","
            + "\"max-ref-age-ms\":null,\"min-snapshots-to-keep\":null,\"max-snapshot-age-ms\":null}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testBranchFromJsonAllFields() {
    String action = MetadataUpdateParser.SET_SNAPSHOT_REF;
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.BRANCH;
    String refName = "hank";
    Integer minSnapshotsToKeep = 2;
    Long maxSnapshotAgeMs = 3L;
    Long maxRefAgeMs = 4L;
    String json =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"branch\","
            + "\"min-snapshots-to-keep\":2,\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    MetadataUpdate expected =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetSnapshotRefTagToJsonDefault() {
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String expected =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"tag\"}";
    MetadataUpdate update =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as(
            "Set snapshot ref should serialize to the correct JSON value for tag with default fields")
        .isEqualTo(expected);
  }

  @Test
  public void testSetSnapshotRefTagToJsonAllFields() {
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.TAG;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = 1L;
    String expected =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\","
            + "\"snapshot-id\":1,\"type\":\"tag\",\"max-ref-age-ms\":1}";
    MetadataUpdate update =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Set snapshot ref should serialize to the correct JSON value for tag with all fields")
        .isEqualTo(expected);
  }

  @Test
  public void testSetSnapshotRefBranchToJsonDefault() {
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.BRANCH;
    String refName = "hank";
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    Long maxRefAgeMs = null;
    String expected =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"branch\"}";
    MetadataUpdate update =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as(
            "Set snapshot ref should serialize to the correct JSON value for branch with default fields")
        .isEqualTo(expected);
  }

  @Test
  public void testSetSnapshotRefBranchToJsonAllFields() {
    long snapshotId = 1L;
    SnapshotRefType type = SnapshotRefType.BRANCH;
    String refName = "hank";
    Integer minSnapshotsToKeep = 2;
    Long maxSnapshotAgeMs = 3L;
    Long maxRefAgeMs = 4L;
    String expected =
        "{\"action\":\"set-snapshot-ref\",\"ref-name\":\"hank\",\"snapshot-id\":1,\"type\":\"branch\","
            + "\"min-snapshots-to-keep\":2,\"max-snapshot-age-ms\":3,\"max-ref-age-ms\":4}";
    MetadataUpdate update =
        new MetadataUpdate.SetSnapshotRef(
            refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as(
            "Set snapshot ref should serialize to the correct JSON value for branch with all fields")
        .isEqualTo(expected);
  }

  /** SetProperties */
  @Test
  public void testSetPropertiesFromJson() {
    String action = MetadataUpdateParser.SET_PROPERTIES;
    Map<String, String> props =
        ImmutableMap.of(
            "prop1", "val1",
            "prop2", "val2");
    String propsMap = "{\"prop1\":\"val1\",\"prop2\":\"val2\"}";

    // make sure reading "updated" & "updates" both work
    String json = String.format("{\"action\":\"%s\",\"updated\":%s}", action, propsMap);
    MetadataUpdate expected = new MetadataUpdate.SetProperties(props);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));

    json = String.format("{\"action\":\"%s\",\"updates\":%s}", action, propsMap);
    expected = new MetadataUpdate.SetProperties(props);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));

    // if "updated" & "updates" are defined, then "updates" takes precedence
    json =
        String.format(
            "{\"action\":\"%s\",\"updates\":%s,\"updated\":{\"propX\":\"valX\"}}",
            action, propsMap);
    expected = new MetadataUpdate.SetProperties(props);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetPropertiesFromJsonFailsWhenDeserializingNullValues() {
    String action = MetadataUpdateParser.SET_PROPERTIES;
    Map<String, String> props = Maps.newHashMap();
    props.put("prop1", "val1");
    props.put("prop2", null);
    String propsMap = "{\"prop1\":\"val1\",\"prop2\":null}";
    String json = String.format("{\"action\":\"%s\",\"updated\":%s}", action, propsMap);
    assertThatThrownBy(() -> MetadataUpdateParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: prop2: null");
  }

  @Test
  public void testSetPropertiesToJson() {
    String action = MetadataUpdateParser.SET_PROPERTIES;
    Map<String, String> props =
        ImmutableMap.of(
            "prop1", "val1",
            "prop2", "val2");
    String propsMap = "{\"prop1\":\"val1\",\"prop2\":\"val2\"}";
    String expected = String.format("{\"action\":\"%s\",\"updates\":%s}", action, propsMap);
    MetadataUpdate update = new MetadataUpdate.SetProperties(props);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Set properties should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  /** RemoveProperties */
  @Test
  public void testRemovePropertiesFromJson() {
    String action = MetadataUpdateParser.REMOVE_PROPERTIES;
    Set<String> toRemove = ImmutableSet.of("prop1", "prop2");
    String toRemoveAsJSON = "[\"prop1\",\"prop2\"]";

    // make sure reading "removed" & "removals" both work
    String json = String.format("{\"action\":\"%s\",\"removed\":%s}", action, toRemoveAsJSON);
    MetadataUpdate expected = new MetadataUpdate.RemoveProperties(toRemove);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));

    json = String.format("{\"action\":\"%s\",\"removals\":%s}", action, toRemoveAsJSON);
    expected = new MetadataUpdate.RemoveProperties(toRemove);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));

    // if "removed" & "removals" are defined, then "removals" takes precedence
    json =
        String.format(
            "{\"action\":\"%s\",\"removals\":%s,\"removed\": [\"propX\"]}", action, toRemoveAsJSON);
    expected = new MetadataUpdate.RemoveProperties(toRemove);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testRemovePropertiesToJson() {
    String action = MetadataUpdateParser.REMOVE_PROPERTIES;
    Set<String> toRemove = ImmutableSet.of("prop1", "prop2");
    String toRemoveAsJSON = "[\"prop1\",\"prop2\"]";
    String expected = String.format("{\"action\":\"%s\",\"removals\":%s}", action, toRemoveAsJSON);
    MetadataUpdate update = new MetadataUpdate.RemoveProperties(toRemove);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Remove properties should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  /** SetLocation */
  @Test
  public void testSetLocationFromJson() {
    String action = MetadataUpdateParser.SET_LOCATION;
    String location = "s3://bucket/warehouse/tbl_location";
    String json = String.format("{\"action\":\"%s\",\"location\":\"%s\"}", action, location);
    MetadataUpdate expected = new MetadataUpdate.SetLocation(location);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetLocationToJson() {
    String action = MetadataUpdateParser.SET_LOCATION;
    String location = "s3://bucket/warehouse/tbl_location";
    String expected = String.format("{\"action\":\"%s\",\"location\":\"%s\"}", action, location);
    MetadataUpdate update = new MetadataUpdate.SetLocation(location);
    String actual = MetadataUpdateParser.toJson(update);
    assertThat(actual)
        .as("Remove properties should serialize to the correct JSON value")
        .isEqualTo(expected);
  }

  @Test
  public void testSetStatistics() {
    String json =
        "{\"action\":\"set-statistics\",\"snapshot-id\":1940541653261589030,\"statistics\":{\"snapshot-id\":1940541653261589030,"
            + "\"statistics-path\":\"s3://bucket/warehouse/stats.puffin\",\"file-size-in-bytes\":124,"
            + "\"file-footer-size-in-bytes\":27,\"blob-metadata\":[{\"type\":\"boring-type\","
            + "\"snapshot-id\":1940541653261589030,\"sequence-number\":2,\"fields\":[1],"
            + "\"properties\":{\"prop-key\":\"prop-value\"}}]}}";

    long snapshotId = 1940541653261589030L;
    MetadataUpdate expected =
        new MetadataUpdate.SetStatistics(
            snapshotId,
            new GenericStatisticsFile(
                snapshotId,
                "s3://bucket/warehouse/stats.puffin",
                124L,
                27L,
                ImmutableList.of(
                    new GenericBlobMetadata(
                        "boring-type",
                        snapshotId,
                        2L,
                        ImmutableList.of(1),
                        ImmutableMap.of("prop-key", "prop-value")))));
    assertEquals(
        MetadataUpdateParser.SET_STATISTICS, expected, MetadataUpdateParser.fromJson(json));
    assertThat(MetadataUpdateParser.toJson(expected))
        .as("Set statistics should convert to the correct JSON value")
        .isEqualTo(json);
  }

  @Test
  public void testRemoveStatistics() {
    String json = "{\"action\":\"remove-statistics\",\"snapshot-id\":1940541653261589030}";
    MetadataUpdate expected = new MetadataUpdate.RemoveStatistics(1940541653261589030L);
    assertEquals(
        MetadataUpdateParser.REMOVE_STATISTICS, expected, MetadataUpdateParser.fromJson(json));
    assertThat(MetadataUpdateParser.toJson(expected))
        .as("Remove statistics should convert to the correct JSON value")
        .isEqualTo(json);
  }

  /** AddViewVersion */
  @Test
  public void testAddViewVersionFromJson() {
    String action = MetadataUpdateParser.ADD_VIEW_VERSION;
    long timestamp = 123456789;
    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(23)
            .timestampMillis(timestamp)
            .schemaId(4)
            .putSummary("user", "some-user")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    String json =
        String.format(
            "{\"action\":\"%s\",\"view-version\":{\"version-id\":23,\"timestamp-ms\":123456789,\"schema-id\":4,\"summary\":{\"user\":\"some-user\"},\"default-namespace\":[\"ns\"],\"representations\":[]}}",
            action);
    MetadataUpdate expected = new MetadataUpdate.AddViewVersion(viewVersion);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddViewVersionToJson() {
    String action = MetadataUpdateParser.ADD_VIEW_VERSION;
    long timestamp = 123456789;
    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(23)
            .timestampMillis(timestamp)
            .schemaId(4)
            .putSummary("user", "some-user")
            .defaultNamespace(Namespace.of("ns"))
            .build();
    String expected =
        String.format(
            "{\"action\":\"%s\",\"view-version\":{\"version-id\":23,\"timestamp-ms\":123456789,\"schema-id\":4,\"summary\":{\"user\":\"some-user\"},\"default-namespace\":[\"ns\"],\"representations\":[]}}",
            action);

    MetadataUpdate update = new MetadataUpdate.AddViewVersion(viewVersion);
    assertThat(MetadataUpdateParser.toJson(update)).isEqualTo(expected);
  }

  /** SetCurrentViewVersion */
  @Test
  public void testSetCurrentViewVersionFromJson() {
    String action = MetadataUpdateParser.SET_CURRENT_VIEW_VERSION;
    String json = String.format("{\"action\":\"%s\",\"view-version-id\":23}", action);
    MetadataUpdate expected = new MetadataUpdate.SetCurrentViewVersion(23);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetCurrentViewVersionToJson() {
    String action = MetadataUpdateParser.SET_CURRENT_VIEW_VERSION;
    String expected = String.format("{\"action\":\"%s\",\"view-version-id\":23}", action);
    MetadataUpdate update = new MetadataUpdate.SetCurrentViewVersion(23);
    assertThat(MetadataUpdateParser.toJson(update)).isEqualTo(expected);
  }

  @Test
  public void testSetPartitionStatistics() {
    String json =
        "{\"action\":\"set-partition-statistics\","
            + "\"partition-statistics\":{\"snapshot-id\":1940541653261589030,"
            + "\"statistics-path\":\"s3://bucket/warehouse/stats1.parquet\","
            + "\"file-size-in-bytes\":43}}";

    long snapshotId = 1940541653261589030L;
    MetadataUpdate expected =
        new MetadataUpdate.SetPartitionStatistics(
            ImmutableGenericPartitionStatisticsFile.builder()
                .snapshotId(snapshotId)
                .path("s3://bucket/warehouse/stats1" + ".parquet")
                .fileSizeInBytes(43L)
                .build());
    assertEquals(
        MetadataUpdateParser.SET_PARTITION_STATISTICS,
        expected,
        MetadataUpdateParser.fromJson(json));
    assertThat(MetadataUpdateParser.toJson(expected))
        .as("Set partition statistics should convert to the correct JSON value")
        .isEqualTo(json);
  }

  @Test
  public void testRemovePartitionStatistics() {
    String json =
        "{\"action\":\"remove-partition-statistics\",\"snapshot-id\":1940541653261589030}";
    MetadataUpdate expected = new MetadataUpdate.RemovePartitionStatistics(1940541653261589030L);
    assertEquals(
        MetadataUpdateParser.REMOVE_PARTITION_STATISTICS,
        expected,
        MetadataUpdateParser.fromJson(json));
    assertThat(MetadataUpdateParser.toJson(expected))
        .as("Remove partition statistics should convert to the correct JSON value")
        .isEqualTo(json);
  }

  public void assertEquals(
      String action, MetadataUpdate expectedUpdate, MetadataUpdate actualUpdate) {
    switch (action) {
      case MetadataUpdateParser.ASSIGN_UUID:
        assertEqualsAssignUUID(
            (MetadataUpdate.AssignUUID) expectedUpdate, (MetadataUpdate.AssignUUID) actualUpdate);
        break;
      case MetadataUpdateParser.UPGRADE_FORMAT_VERSION:
        assertEqualsUpgradeFormatVersion(
            (MetadataUpdate.UpgradeFormatVersion) expectedUpdate,
            (MetadataUpdate.UpgradeFormatVersion) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_SCHEMA:
        assertEqualsAddSchema(
            (MetadataUpdate.AddSchema) expectedUpdate, (MetadataUpdate.AddSchema) actualUpdate);
        break;
      case MetadataUpdateParser.SET_CURRENT_SCHEMA:
        assertEqualsSetCurrentSchema(
            (MetadataUpdate.SetCurrentSchema) expectedUpdate,
            (MetadataUpdate.SetCurrentSchema) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_PARTITION_SPEC:
        assertEqualsAddPartitionSpec(
            (MetadataUpdate.AddPartitionSpec) expectedUpdate,
            (MetadataUpdate.AddPartitionSpec) actualUpdate);
        break;
      case MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC:
        assertEqualsSetDefaultPartitionSpec(
            (MetadataUpdate.SetDefaultPartitionSpec) expectedUpdate,
            (MetadataUpdate.SetDefaultPartitionSpec) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_SORT_ORDER:
        assertEqualsAddSortOrder(
            (MetadataUpdate.AddSortOrder) expectedUpdate,
            (MetadataUpdate.AddSortOrder) actualUpdate);
        break;
      case MetadataUpdateParser.SET_DEFAULT_SORT_ORDER:
        assertEqualsSetDefaultSortOrder(
            (MetadataUpdate.SetDefaultSortOrder) expectedUpdate,
            (MetadataUpdate.SetDefaultSortOrder) actualUpdate);
        break;
      case MetadataUpdateParser.SET_STATISTICS:
        assertEqualsSetStatistics(
            (MetadataUpdate.SetStatistics) expectedUpdate,
            (MetadataUpdate.SetStatistics) actualUpdate);
        break;
      case MetadataUpdateParser.REMOVE_STATISTICS:
        assertEqualsRemoveStatistics(
            (MetadataUpdate.RemoveStatistics) expectedUpdate,
            (MetadataUpdate.RemoveStatistics) actualUpdate);
        break;
      case MetadataUpdateParser.SET_PARTITION_STATISTICS:
        assertEqualsSetPartitionStatistics(
            (MetadataUpdate.SetPartitionStatistics) expectedUpdate,
            (MetadataUpdate.SetPartitionStatistics) actualUpdate);
        break;
      case MetadataUpdateParser.REMOVE_PARTITION_STATISTICS:
        assertEqualsRemovePartitionStatistics(
            (MetadataUpdate.RemovePartitionStatistics) expectedUpdate,
            (MetadataUpdate.RemovePartitionStatistics) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_SNAPSHOT:
        assertEqualsAddSnapshot(
            (MetadataUpdate.AddSnapshot) expectedUpdate, (MetadataUpdate.AddSnapshot) actualUpdate);
        break;
      case MetadataUpdateParser.REMOVE_SNAPSHOTS:
        assertEqualsRemoveSnapshots(
            (MetadataUpdate.RemoveSnapshot) expectedUpdate,
            (MetadataUpdate.RemoveSnapshot) actualUpdate);
        break;
      case MetadataUpdateParser.REMOVE_SNAPSHOT_REF:
        assertEqualsRemoveSnapshotRef(
            (MetadataUpdate.RemoveSnapshotRef) expectedUpdate,
            (MetadataUpdate.RemoveSnapshotRef) actualUpdate);
        break;
      case MetadataUpdateParser.SET_SNAPSHOT_REF:
        assertEqualsSetSnapshotRef(
            (MetadataUpdate.SetSnapshotRef) expectedUpdate,
            (MetadataUpdate.SetSnapshotRef) actualUpdate);
        break;
      case MetadataUpdateParser.SET_PROPERTIES:
        assertEqualsSetProperties(
            (MetadataUpdate.SetProperties) expectedUpdate,
            (MetadataUpdate.SetProperties) actualUpdate);
        break;
      case MetadataUpdateParser.REMOVE_PROPERTIES:
        assertEqualsRemoveProperties(
            (MetadataUpdate.RemoveProperties) expectedUpdate,
            (MetadataUpdate.RemoveProperties) actualUpdate);
        break;
      case MetadataUpdateParser.SET_LOCATION:
        assertEqualsSetLocation(
            (MetadataUpdate.SetLocation) expectedUpdate, (MetadataUpdate.SetLocation) actualUpdate);
        break;
      case MetadataUpdateParser.ADD_VIEW_VERSION:
        assertEqualsAddViewVersion(
            (MetadataUpdate.AddViewVersion) expectedUpdate,
            (MetadataUpdate.AddViewVersion) actualUpdate);
        break;
      case MetadataUpdateParser.SET_CURRENT_VIEW_VERSION:
        assertEqualsSetCurrentViewVersion(
            (MetadataUpdate.SetCurrentViewVersion) expectedUpdate,
            (MetadataUpdate.SetCurrentViewVersion) actualUpdate);
        break;
      default:
        fail("Unrecognized metadata update action: " + action);
    }
  }

  private static void assertEqualsAssignUUID(
      MetadataUpdate.AssignUUID expected, MetadataUpdate.AssignUUID actual) {
    assertThat(actual.uuid()).isEqualTo(expected.uuid());
  }

  private static void assertEqualsUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion expected, MetadataUpdate.UpgradeFormatVersion actual) {
    assertThat(actual.formatVersion()).isEqualTo(expected.formatVersion());
  }

  private static void assertEqualsAddSchema(
      MetadataUpdate.AddSchema expected, MetadataUpdate.AddSchema actual) {
    assertThat(actual.schema().sameSchema(expected.schema())).isTrue();
    assertThat(actual.lastColumnId()).isEqualTo(expected.lastColumnId());
  }

  private static void assertEqualsSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema expected, MetadataUpdate.SetCurrentSchema actual) {
    assertThat(actual.schemaId()).isEqualTo(expected.schemaId());
  }

  private static void assertEqualsSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec expected,
      MetadataUpdate.SetDefaultPartitionSpec actual) {
    assertThat(actual.specId()).isEqualTo(expected.specId());
  }

  private static void assertEqualsAddPartitionSpec(
      MetadataUpdate.AddPartitionSpec expected, MetadataUpdate.AddPartitionSpec actual) {
    assertThat(actual.spec().specId())
        .as("Unbound partition specs should have the same spec id")
        .isEqualTo(expected.spec().specId());
    assertThat(actual.spec().fields())
        .as("Unbound partition specs should have the same number of fields")
        .hasSameSizeAs(expected.spec().fields());

    IntStream.range(0, expected.spec().fields().size())
        .forEachOrdered(
            i -> {
              UnboundPartitionSpec.UnboundPartitionField expectedField =
                  expected.spec().fields().get(i);
              UnboundPartitionSpec.UnboundPartitionField actualField =
                  actual.spec().fields().get(i);
              assertThat(actualField.partitionId()).isEqualTo(expectedField.partitionId());
              assertThat(actualField.name()).isEqualTo(expectedField.name());
              assertThat(actualField.transformAsString())
                  .isEqualTo(expectedField.transformAsString());
              assertThat(actualField.sourceId()).isEqualTo(expectedField.sourceId());
            });
  }

  private static void assertEqualsAddSortOrder(
      MetadataUpdate.AddSortOrder expected, MetadataUpdate.AddSortOrder actual) {
    assertThat(actual.sortOrder().orderId())
        .as("Order id of the sort order should be the same")
        .isEqualTo(expected.sortOrder().orderId());

    assertThat(actual.sortOrder().fields())
        .as("Sort orders should have the same number of fields")
        .hasSameSizeAs(expected.sortOrder().fields());

    IntStream.range(0, expected.sortOrder().fields().size())
        .forEachOrdered(
            i -> {
              UnboundSortOrder.UnboundSortField expectedField =
                  expected.sortOrder().fields().get(i);
              UnboundSortOrder.UnboundSortField actualField = actual.sortOrder().fields().get(i);
              assertThat(actualField.sourceId()).isEqualTo(expectedField.sourceId());
              assertThat(actualField.nullOrder()).isEqualTo(expectedField.nullOrder());
              assertThat(actualField.direction()).isEqualTo(expectedField.direction());
              assertThat(actualField.transformAsString())
                  .isEqualTo(expectedField.transformAsString());
            });
  }

  private static void assertEqualsSetDefaultSortOrder(
      MetadataUpdate.SetDefaultSortOrder expected, MetadataUpdate.SetDefaultSortOrder actual) {
    assertThat(actual.sortOrderId()).isEqualTo(expected.sortOrderId());
  }

  private static void assertEqualsSetStatistics(
      MetadataUpdate.SetStatistics expected, MetadataUpdate.SetStatistics actual) {
    assertThat(actual.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(actual.statisticsFile().snapshotId())
        .isEqualTo(expected.statisticsFile().snapshotId());
    assertThat(actual.statisticsFile().path()).isEqualTo(expected.statisticsFile().path());
    assertThat(actual.statisticsFile().fileSizeInBytes())
        .isEqualTo(expected.statisticsFile().fileSizeInBytes());
    assertThat(actual.statisticsFile().fileFooterSizeInBytes())
        .isEqualTo(expected.statisticsFile().fileFooterSizeInBytes());
    assertThat(actual.statisticsFile().blobMetadata())
        .hasSameSizeAs(expected.statisticsFile().blobMetadata());

    Streams.zip(
            expected.statisticsFile().blobMetadata().stream(),
            actual.statisticsFile().blobMetadata().stream(),
            Pair::of)
        .forEachOrdered(
            pair -> {
              BlobMetadata expectedBlob = pair.first();
              BlobMetadata actualBlob = pair.second();

              assertThat(actualBlob.type()).isEqualTo(expectedBlob.type());
              assertThat(actualBlob.fields()).isEqualTo(expectedBlob.fields());
              assertThat(actualBlob.sourceSnapshotId()).isEqualTo(expectedBlob.sourceSnapshotId());
              assertThat(actualBlob.sourceSnapshotSequenceNumber())
                  .isEqualTo(expectedBlob.sourceSnapshotSequenceNumber());
              assertThat(actualBlob.properties()).isEqualTo(expectedBlob.properties());
            });
  }

  private static void assertEqualsRemoveStatistics(
      MetadataUpdate.RemoveStatistics expected, MetadataUpdate.RemoveStatistics actual) {
    assertThat(actual.snapshotId())
        .as("Snapshots to remove should be the same")
        .isEqualTo(expected.snapshotId());
  }

  private static void assertEqualsSetPartitionStatistics(
      MetadataUpdate.SetPartitionStatistics expected,
      MetadataUpdate.SetPartitionStatistics actual) {
    assertThat(actual.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(actual.partitionStatisticsFile().snapshotId())
        .isEqualTo(expected.partitionStatisticsFile().snapshotId());
    assertThat(actual.partitionStatisticsFile().path())
        .isEqualTo(expected.partitionStatisticsFile().path());
    assertThat(actual.partitionStatisticsFile().fileSizeInBytes())
        .isEqualTo(expected.partitionStatisticsFile().fileSizeInBytes());
  }

  private static void assertEqualsRemovePartitionStatistics(
      MetadataUpdate.RemovePartitionStatistics expected,
      MetadataUpdate.RemovePartitionStatistics actual) {
    assertThat(actual.snapshotId()).isEqualTo(expected.snapshotId());
  }

  private static void assertEqualsAddSnapshot(
      MetadataUpdate.AddSnapshot expected, MetadataUpdate.AddSnapshot actual) {
    assertThat(actual.snapshot().snapshotId()).isEqualTo(expected.snapshot().snapshotId());
    assertThat(actual.snapshot().manifestListLocation())
        .isEqualTo(expected.snapshot().manifestListLocation());
    assertThat(actual.snapshot().summary())
        .as("Snapshot summary should be equivalent")
        .containsExactlyEntriesOf(expected.snapshot().summary());
    assertThat(actual.snapshot().parentId()).isEqualTo(expected.snapshot().parentId());
    assertThat(actual.snapshot().timestampMillis())
        .isEqualTo(expected.snapshot().timestampMillis());
    assertThat(actual.snapshot().schemaId()).isEqualTo(expected.snapshot().schemaId());
  }

  private static void assertEqualsRemoveSnapshots(
      MetadataUpdate.RemoveSnapshot expected, MetadataUpdate.RemoveSnapshot actual) {
    assertThat(actual.snapshotId())
        .as("Snapshots to remove should be the same")
        .isEqualTo(expected.snapshotId());
  }

  private static void assertEqualsSetSnapshotRef(
      MetadataUpdate.SetSnapshotRef expected, MetadataUpdate.SetSnapshotRef actual) {
    // Non-null fields
    assertThat(actual.name()).isNotNull();
    assertThat(actual.name()).isEqualTo(expected.name());
    assertThat(actual.snapshotId()).isEqualTo(expected.snapshotId());
    assertThat(actual.type()).isNotNull();
    assertThat(actual.type()).isEqualTo(expected.type());

    // Nullable fields
    assertThat(actual.minSnapshotsToKeep())
        .as(
            "Min snapshots to keep should be equal when present and null when missing or explicitly null")
        .isEqualTo(expected.minSnapshotsToKeep());
    assertThat(actual.maxSnapshotAgeMs())
        .as(
            "Max snapshot age ms should be equal when present and null when missing or explicitly null")
        .isEqualTo(expected.maxSnapshotAgeMs());
    assertThat(actual.maxRefAgeMs())
        .as("Max ref age ms should be equal when present and null when missing or explicitly null")
        .isEqualTo(expected.maxRefAgeMs());
  }

  private static void assertEqualsRemoveSnapshotRef(
      MetadataUpdate.RemoveSnapshotRef expected, MetadataUpdate.RemoveSnapshotRef actual) {
    assertThat(actual.name()).isEqualTo(expected.name());
  }

  private static void assertEqualsSetProperties(
      MetadataUpdate.SetProperties expected, MetadataUpdate.SetProperties actual) {
    assertThat(actual.updated())
        .as("Properties to set / update should not be null")
        .isNotNull()
        .as("Properties to set / update should be the same")
        .containsExactlyInAnyOrderEntriesOf(expected.updated());
  }

  private static void assertEqualsRemoveProperties(
      MetadataUpdate.RemoveProperties expected, MetadataUpdate.RemoveProperties actual) {
    assertThat(actual.removed())
        .as("Properties to remove should not be null")
        .isNotNull()
        .as("Properties to remove should be equal")
        .containsExactlyInAnyOrderElementsOf(expected.removed());
  }

  private static void assertEqualsSetLocation(
      MetadataUpdate.SetLocation expected, MetadataUpdate.SetLocation actual) {
    assertThat(actual.location()).isEqualTo(expected.location());
  }

  private static void assertEqualsAddViewVersion(
      MetadataUpdate.AddViewVersion expected, MetadataUpdate.AddViewVersion actual) {
    assertThat(actual.viewVersion()).isEqualTo(expected.viewVersion());
  }

  private static void assertEqualsSetCurrentViewVersion(
      MetadataUpdate.SetCurrentViewVersion expected, MetadataUpdate.SetCurrentViewVersion actual) {
    assertThat(actual.versionId()).isEqualTo(expected.versionId());
  }

  private String createManifestListWithManifestFiles(long snapshotId, Long parentSnapshotId)
      throws IOException {
    File manifestList = File.createTempFile("manifests", null, temp.toFile());
    manifestList.deleteOnExit();

    List<ManifestFile> manifests =
        ImmutableList.of(
            new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
            new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    try (ManifestListWriter writer =
        ManifestLists.write(1, Files.localOutput(manifestList), snapshotId, parentSnapshotId, 0)) {
      writer.addAll(manifests);
    }

    return localInput(manifestList).location();
  }
}
