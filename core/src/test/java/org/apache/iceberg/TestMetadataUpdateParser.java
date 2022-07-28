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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataUpdateParser {

  private static final Schema ID_DATA_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  public void testMetadataUpdateWithoutActionCannotDeserialize() {
    List<String> invalidJson =
        ImmutableList.of("{\"action\":null,\"format-version\":2}", "{\"format-version\":2}");

    for (String json : invalidJson) {
      AssertHelpers.assertThrows(
          "MetadataUpdate without a recognized action should fail to deserialize",
          IllegalArgumentException.class,
          "Cannot parse metadata update. Missing field: action",
          () -> MetadataUpdateParser.fromJson(json));
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
    Assert.assertEquals(
        "Assign UUID should convert to the correct JSON value",
        expected,
        MetadataUpdateParser.toJson(actual));
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
    Assert.assertEquals(
        "Upgrade format version should convert to the correct JSON value",
        expected,
        MetadataUpdateParser.toJson(actual));
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
  public void testAddSchemaToJson() {
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String expected =
        String.format(
            "{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
            SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate update = new MetadataUpdate.AddSchema(schema, lastColumnId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Add schema should convert to the correct JSON value", expected, actual);
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
    Assert.assertEquals(
        "Set current schema should convert to the correct JSON value", expected, actual);
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
    Assert.assertEquals(
        "Add partition spec should convert to the correct JSON value", expected, actual);
  }

  /** SetDefaultPartitionSpec * */
  @Test
  public void testSetDefaultPartitionSpecToJson() {
    String action = MetadataUpdateParser.SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String expected = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate update = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals(
        "Set default partition spec should serialize to the correct JSON value", expected, actual);
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
    Assert.assertEquals(
        "Add sort order should serialize to the correct JSON value",
        expected,
        MetadataUpdateParser.toJson(update));
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
    Assert.assertEquals(
        "Set default sort order should serialize to the correct JSON value", expected, actual);
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
  public void testAddSnapshotToJson() {
    String action = MetadataUpdateParser.ADD_SNAPSHOT;
    long parentId = 1;
    long snapshotId = 2;
    int schemaId = 3;
    List<ManifestFile> manifests =
        ImmutableList.of(
            new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
            new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));

    Snapshot snapshot =
        new BaseSnapshot(
            null,
            snapshotId,
            parentId,
            System.currentTimeMillis(),
            DataOperations.REPLACE,
            ImmutableMap.of("files-added", "4", "files-deleted", "100"),
            schemaId,
            manifests);
    String snapshotJson = SnapshotParser.toJson(snapshot, /* pretty */ false);
    String expected = String.format("{\"action\":\"%s\",\"snapshot\":%s}", action, snapshotJson);
    MetadataUpdate update = new MetadataUpdate.AddSnapshot(snapshot);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals(
        "Add snapshot should serialize to the correct JSON value", expected, actual);
  }

  @Test
  public void testAddSnapshotFromJson() {
    String action = MetadataUpdateParser.ADD_SNAPSHOT;
    long parentId = 1;
    long snapshotId = 2;
    int schemaId = 3;
    List<ManifestFile> manifests =
        ImmutableList.of(
            new GenericManifestFile(localInput("file:/tmp/manifest1.avro"), 0),
            new GenericManifestFile(localInput("file:/tmp/manifest2.avro"), 0));
    Map<String, String> summary = ImmutableMap.of("files-added", "4", "files-deleted", "100");
    Snapshot snapshot =
        new BaseSnapshot(
            null,
            snapshotId,
            parentId,
            System.currentTimeMillis(),
            DataOperations.REPLACE,
            summary,
            schemaId,
            manifests);
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
    Assert.assertEquals(
        "Remove snapshots should serialize to the correct JSON value", expected, actual);
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
    Assert.assertEquals(
        "RemoveSnapshotRef should convert to the correct JSON value",
        expected,
        MetadataUpdateParser.toJson(actual));
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
    Assert.assertEquals(
        "Set snapshot ref should serialize to the correct JSON value for tag with default fields",
        expected,
        actual);
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
    Assert.assertEquals(
        "Set snapshot ref should serialize to the correct JSON value for tag with all fields",
        expected,
        actual);
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
    Assert.assertEquals(
        "Set snapshot ref should serialize to the correct JSON value for branch with default fields",
        expected,
        actual);
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
    Assert.assertEquals(
        "Set snapshot ref should serialize to the correct JSON value for branch with all fields",
        expected,
        actual);
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
    String json = String.format("{\"action\":\"%s\",\"updated\":%s}", action, propsMap);
    MetadataUpdate expected = new MetadataUpdate.SetProperties(props);
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
    AssertHelpers.assertThrows(
        "Parsing updates from SetProperties with a property set to null should throw",
        IllegalArgumentException.class,
        "Cannot parse prop2 to a string value: null",
        () -> MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetPropertiesToJson() {
    String action = MetadataUpdateParser.SET_PROPERTIES;
    Map<String, String> props =
        ImmutableMap.of(
            "prop1", "val1",
            "prop2", "val2");
    String propsMap = "{\"prop1\":\"val1\",\"prop2\":\"val2\"}";
    String expected = String.format("{\"action\":\"%s\",\"updated\":%s}", action, propsMap);
    MetadataUpdate update = new MetadataUpdate.SetProperties(props);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals(
        "Set properties should serialize to the correct JSON value", expected, actual);
  }

  /** RemoveProperties */
  @Test
  public void testRemovePropertiesFromJson() {
    String action = MetadataUpdateParser.REMOVE_PROPERTIES;
    Set<String> toRemove = ImmutableSet.of("prop1", "prop2");
    String toRemoveAsJSON = "[\"prop1\",\"prop2\"]";
    String json = String.format("{\"action\":\"%s\",\"removed\":%s}", action, toRemoveAsJSON);
    MetadataUpdate expected = new MetadataUpdate.RemoveProperties(toRemove);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testRemovePropertiesToJson() {
    String action = MetadataUpdateParser.REMOVE_PROPERTIES;
    Set<String> toRemove = ImmutableSet.of("prop1", "prop2");
    String toRemoveAsJSON = "[\"prop1\",\"prop2\"]";
    String expected = String.format("{\"action\":\"%s\",\"removed\":%s}", action, toRemoveAsJSON);
    MetadataUpdate update = new MetadataUpdate.RemoveProperties(toRemove);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals(
        "Remove properties should serialize to the correct JSON value", expected, actual);
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
    Assert.assertEquals(
        "Remove properties should serialize to the correct JSON value", expected, actual);
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
      default:
        Assert.fail("Unrecognized metadata update action: " + action);
    }
  }

  private static void assertEqualsAssignUUID(
      MetadataUpdate.AssignUUID expected, MetadataUpdate.AssignUUID actual) {
    Assert.assertEquals("UUIDs should be equal", expected.uuid(), actual.uuid());
  }

  private static void assertEqualsUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion expected, MetadataUpdate.UpgradeFormatVersion actual) {
    Assert.assertEquals(
        "Format version should be equal", expected.formatVersion(), actual.formatVersion());
  }

  private static void assertEqualsAddSchema(
      MetadataUpdate.AddSchema expected, MetadataUpdate.AddSchema actual) {
    Assert.assertTrue("Schemas should be the same", expected.schema().sameSchema(actual.schema()));
    Assert.assertEquals(
        "Last column id should be equal", expected.lastColumnId(), actual.lastColumnId());
  }

  private static void assertEqualsSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema expected, MetadataUpdate.SetCurrentSchema actual) {
    Assert.assertEquals("Schema id should be equal", expected.schemaId(), actual.schemaId());
  }

  private static void assertEqualsSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec expected,
      MetadataUpdate.SetDefaultPartitionSpec actual) {
    Assertions.assertThat(actual.specId()).isEqualTo(expected.specId());
  }

  private static void assertEqualsAddPartitionSpec(
      MetadataUpdate.AddPartitionSpec expected, MetadataUpdate.AddPartitionSpec actual) {
    Assert.assertEquals(
        "Unbound partition specs should have the same spec id",
        expected.spec().specId(),
        actual.spec().specId());
    Assert.assertEquals(
        "Unbound partition specs should have the same number of fields",
        expected.spec().fields().size(),
        actual.spec().fields().size());

    IntStream.range(0, expected.spec().fields().size())
        .forEachOrdered(
            i -> {
              UnboundPartitionSpec.UnboundPartitionField expectedField =
                  expected.spec().fields().get(i);
              UnboundPartitionSpec.UnboundPartitionField actualField =
                  actual.spec().fields().get(i);
              Assert.assertTrue(
                  "Fields of the unbound partition spec should be the same",
                  Objects.equals(expectedField.partitionId(), actualField.partitionId())
                      && expectedField.name().equals(actualField.name())
                      && Objects.equals(
                          expectedField.transformAsString(), actualField.transformAsString())
                      && expectedField.sourceId() == actualField.sourceId());
            });
  }

  private static void assertEqualsAddSortOrder(
      MetadataUpdate.AddSortOrder expected, MetadataUpdate.AddSortOrder actual) {
    Assert.assertEquals(
        "Order id of the sort order should be the same",
        expected.sortOrder().orderId(),
        actual.sortOrder().orderId());

    Assert.assertEquals(
        "Sort orders should have the same number of fields",
        expected.sortOrder().fields().size(),
        actual.sortOrder().fields().size());

    IntStream.range(0, expected.sortOrder().fields().size())
        .forEachOrdered(
            i -> {
              UnboundSortOrder.UnboundSortField expectedField =
                  expected.sortOrder().fields().get(i);
              UnboundSortOrder.UnboundSortField actualField = actual.sortOrder().fields().get(i);
              Assert.assertTrue(
                  "Fields of the sort order should be the same",
                  expectedField.sourceId() == actualField.sourceId()
                      && expectedField.nullOrder().equals(actualField.nullOrder())
                      && expectedField.direction().equals(actualField.direction())
                      && Objects.equals(
                          expectedField.transformAsString(), actualField.transformAsString()));
            });
  }

  private static void assertEqualsSetDefaultSortOrder(
      MetadataUpdate.SetDefaultSortOrder expected, MetadataUpdate.SetDefaultSortOrder actual) {
    Assert.assertEquals(
        "Sort order id should be the same", expected.sortOrderId(), actual.sortOrderId());
  }

  private static void assertEqualsAddSnapshot(
      MetadataUpdate.AddSnapshot expected, MetadataUpdate.AddSnapshot actual) {
    Assert.assertEquals(
        "Snapshot ID should be equal",
        expected.snapshot().snapshotId(),
        actual.snapshot().snapshotId());
    Assert.assertEquals(
        "Manifest list location should be equal",
        expected.snapshot().manifestListLocation(),
        actual.snapshot().manifestListLocation());
    Assertions.assertThat(actual.snapshot().summary())
        .as("Snapshot summary should be equivalent")
        .containsExactlyEntriesOf(expected.snapshot().summary());
    Assert.assertEquals(
        "Snapshot Parent ID should be equal",
        expected.snapshot().parentId(),
        actual.snapshot().parentId());
    Assert.assertEquals(
        "Snapshot timestamp should be equal",
        expected.snapshot().timestampMillis(),
        actual.snapshot().timestampMillis());
    Assert.assertEquals(
        "Snapshot schema id should be equal",
        expected.snapshot().schemaId(),
        actual.snapshot().schemaId());
  }

  private static void assertEqualsRemoveSnapshots(
      MetadataUpdate.RemoveSnapshot expected, MetadataUpdate.RemoveSnapshot actual) {
    Assert.assertEquals(
        "Snapshots to remove should be the same", expected.snapshotId(), actual.snapshotId());
  }

  private static void assertEqualsSetSnapshotRef(
      MetadataUpdate.SetSnapshotRef expected, MetadataUpdate.SetSnapshotRef actual) {
    // Non-null fields
    Assert.assertNotNull("Snapshot ref name should not be null", actual.name());
    Assert.assertEquals("Snapshot ref name should be equal", expected.name(), actual.name());
    Assert.assertEquals("Snapshot ID should be equal", expected.snapshotId(), actual.snapshotId());
    Assert.assertNotNull("Snapshot reference type should not be null", actual.type());
    Assert.assertEquals("Snapshot reference type should be equal", expected.type(), actual.type());

    // Nullable fields
    Assert.assertEquals(
        "Min snapshots to keep should be equal when present and null when missing or explicitly null",
        expected.minSnapshotsToKeep(),
        actual.minSnapshotsToKeep());
    Assert.assertEquals(
        "Max snapshot age ms should be equal when present and null when missing or explicitly null",
        expected.maxSnapshotAgeMs(),
        actual.maxSnapshotAgeMs());
    Assert.assertEquals(
        "Max ref age ms should be equal when present and null when missing or explicitly null",
        expected.maxRefAgeMs(),
        actual.maxRefAgeMs());
  }

  private static void assertEqualsRemoveSnapshotRef(
      MetadataUpdate.RemoveSnapshotRef expected, MetadataUpdate.RemoveSnapshotRef actual) {
    Assertions.assertThat(actual.name()).isEqualTo(expected.name());
  }

  private static void assertEqualsSetProperties(
      MetadataUpdate.SetProperties expected, MetadataUpdate.SetProperties actual) {
    Assertions.assertThat(actual.updated())
        .as("Properties to set / update should not be null")
        .isNotNull()
        .as("Properties to set / update should be the same")
        .containsExactlyInAnyOrderEntriesOf(expected.updated());
  }

  private static void assertEqualsRemoveProperties(
      MetadataUpdate.RemoveProperties expected, MetadataUpdate.RemoveProperties actual) {
    Assertions.assertThat(actual.removed())
        .as("Properties to remove should not be null")
        .isNotNull()
        .as("Properties to remove should be equal")
        .containsExactlyInAnyOrderElementsOf(expected.removed());
  }

  private static void assertEqualsSetLocation(
      MetadataUpdate.SetLocation expected, MetadataUpdate.SetLocation actual) {
    Assert.assertEquals("Location should be the same", expected.location(), actual.location());
  }
}
