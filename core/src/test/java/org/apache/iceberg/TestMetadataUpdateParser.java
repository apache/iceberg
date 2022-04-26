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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataUpdateParser {

  // MetadataUpdate actions
  private static final String ASSIGN_UUID = "assign-uuid";
  private static final String UPGRADE_FORMAT_VERSION = "upgrade-format-version";
  private static final String ADD_SCHEMA = "add-schema";
  private static final String SET_CURRENT_SCHEMA = "set-current-schema";
  private static final String ADD_PARTITION_SPEC = "add-spec";
  private static final String SET_DEFAULT_PARTITION_SPEC = "set-default-spec";
  private static final String ADD_SORT_ORDER = "add-sort-order";
  private static final String SET_DEFAULT_SORT_ORDER = "set-default-sort-order";
  private static final String ADD_SNAPSHOT = "add-snapshot";
  private static final String REMOVE_SNAPSHOTS = "remove-snapshots";
  private static final String SET_SNAPSHOT_REF = "set-snapshot-ref";
  private static final String SET_PROPERTIES = "set-properties";
  private static final String REMOVE_PROPERTIES = "remove-properties";
  private static final String SET_LOCATION = "set-location";

  private static final Schema ID_DATA_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  public void testMetadataUpdateWithoutActionCannotDeserialize() {
    List<String> invalidJson = ImmutableList.of(
        "{\"action\":null,\"format-version\":2}",
        "{\"format-version\":2}"
    );

    for (String json : invalidJson) {
      AssertHelpers.assertThrows(
          "MetadataUpdate without a recognized action should fail to deserialize",
          IllegalArgumentException.class,
          "Cannot parse metadata update. Missing field: action",
          () -> MetadataUpdateParser.fromJson(json));
    }
  }

  @Test
  public void testUpgradeFormatVersionToJson() {
    int formatVersion = 2;
    String action = UPGRADE_FORMAT_VERSION;
    String json = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion expected = new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testUpgradeFormatVersionFromJson() {
    int formatVersion = 2;
    String expected = "{\"action\":\"upgrade-format-version\",\"format-version\":2}";
    MetadataUpdate.UpgradeFormatVersion actual = new MetadataUpdate.UpgradeFormatVersion(formatVersion);
    Assert.assertEquals("Upgrade format version should convert to the correct JSON value",
        expected, MetadataUpdateParser.toJson(actual));
  }

  @Test
  public void testAddSchemaFromJson() {
    String action = "add-schema";
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String json = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
        SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate actualUpdate = new MetadataUpdate.AddSchema(schema, lastColumnId);
    assertEquals(action, actualUpdate, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testAddSchemaToJson() {
    Schema schema = ID_DATA_SCHEMA;
    int lastColumnId = schema.highestFieldId();
    String expected = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
        SchemaParser.toJson(schema), lastColumnId);
    MetadataUpdate update = new MetadataUpdate.AddSchema(schema, lastColumnId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Add schema should convert to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetCurrentSchemaFromJson() {
    String action = SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String json = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate.SetCurrentSchema expected = new MetadataUpdate.SetCurrentSchema(schemaId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  @Test
  public void testSetCurrentSchemaToJson() {
    String action = SET_CURRENT_SCHEMA;
    int schemaId = 6;
    String expected = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
    MetadataUpdate update = new MetadataUpdate.SetCurrentSchema(schemaId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Set current schema should convert to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetDefaultPartitionSpecToJson() {
    String action = SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String expected = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate update = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    String actual = MetadataUpdateParser.toJson(update);
    Assert.assertEquals("Set default partition spec should serialize to the correct JSON value", expected, actual);
  }

  @Test
  public void testSetDefaultPartitionSpecFromJson() {
    String action = SET_DEFAULT_PARTITION_SPEC;
    int specId = 4;
    String json = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
    MetadataUpdate.SetDefaultPartitionSpec expected = new MetadataUpdate.SetDefaultPartitionSpec(specId);
    assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  }

  public void assertEquals(String action, MetadataUpdate expectedUpdate, MetadataUpdate actualUpdate) {
    switch (action) {
      case ASSIGN_UUID:
        Assert.fail(String.format("MetadataUpdateParser for %s is not implemented", action));
      case UPGRADE_FORMAT_VERSION:
        assertEqualsUpgradeFormatVersion((MetadataUpdate.UpgradeFormatVersion) expectedUpdate,
            (MetadataUpdate.UpgradeFormatVersion) actualUpdate);
        break;
      case ADD_SCHEMA:
        assertEqualsAddSchema((MetadataUpdate.AddSchema) expectedUpdate, (MetadataUpdate.AddSchema) actualUpdate);
        break;
      case SET_CURRENT_SCHEMA:
        assertEqualsSetCurrentSchema((MetadataUpdate.SetCurrentSchema) expectedUpdate,
            (MetadataUpdate.SetCurrentSchema) actualUpdate);
        break;
      case SET_DEFAULT_PARTITION_SPEC:
        assertEqualsSetDefaultPartitionSpec((MetadataUpdate.SetDefaultPartitionSpec) expectedUpdate,
            (MetadataUpdate.SetDefaultPartitionSpec) actualUpdate);
        break;
      case ADD_PARTITION_SPEC:
      case ADD_SORT_ORDER:
      case SET_DEFAULT_SORT_ORDER:
      case ADD_SNAPSHOT:
      case REMOVE_SNAPSHOTS:
      case SET_SNAPSHOT_REF:
      case SET_PROPERTIES:
      case REMOVE_PROPERTIES:
      case SET_LOCATION:
        Assert.fail(String.format("MetadataUpdateParser for %s is not implemented yet", action));
      default:
        Assert.fail("Unrecognized metadata update action: " + action);
    }
  }

  private static void assertEqualsUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion expected, MetadataUpdate.UpgradeFormatVersion actual) {
    Assert.assertEquals("Format version should be equal", expected.formatVersion(), actual.formatVersion());
  }

  private static void assertEqualsAddSchema(MetadataUpdate.AddSchema expected, MetadataUpdate.AddSchema actual) {
    Assert.assertTrue("Schemas should be the same", expected.schema().sameSchema(actual.schema()));
    Assert.assertEquals("Last column id should be equal", expected.lastColumnId(), actual.lastColumnId());
  }

  private static void assertEqualsSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema expected, MetadataUpdate.SetCurrentSchema actual) {
    Assert.assertEquals("Schema id should be equal", expected.schemaId(), actual.schemaId());
  }

  private static void assertEqualsSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec expected, MetadataUpdate.SetDefaultPartitionSpec actual) {
    Assertions.assertThat(actual.specId()).isEqualTo(expected.specId());
  }
}
