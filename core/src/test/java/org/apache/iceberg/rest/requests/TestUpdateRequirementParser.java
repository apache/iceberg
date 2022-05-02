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

package org.apache.iceberg.rest.requests;

import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest.UpdateRequirement;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestUpdateRequirementParser {

  private static final Schema ID_DATA_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Test
  public void testUpdateRequirementWithoutRequirementTypeCannotParse() {
    List<String> invalidJson = ImmutableList.of(
        "{\"type\":null,\"uuid\":\"2cc52516-5e73-41f2-b139-545d41a4e151\"}",
        "{\"uuid\":\"2cc52516-5e73-41f2-b139-545d41a4e151\"}"
    );

    for (String json : invalidJson) {
      AssertHelpers.assertThrows(
          "UpdateRequirement without a recognized requirement type should fail to deserialize",
          IllegalArgumentException.class,
          "Cannot parse update requirement. Missing field: type",
          () -> UpdateRequirementParser.fromJson(json));
    }
  }

  @Test
  public void testAssertUUIDToJson() {
    String action = UpdateRequirementParser.ASSERT_TABLE_UUID;
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String json = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement.AssertTableUUID expected = new UpdateRequirement.AssertTableUUID(uuid);
    assertEquals(action, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertUUIDFromJson() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String expected = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement.AssertTableUUID actual = new UpdateRequirement.AssertTableUUID(uuid);
    Assert.assertEquals("AssertTableUUID should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  // @Test
  // public void testAddSchemaFromJson() {
  //   String action = "add-schema";
  //   Schema schema = ID_DATA_SCHEMA;
  //   int lastColumnId = schema.highestFieldId();
  //   String json = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
  //       SchemaParser.toJson(schema), lastColumnId);
  //   MetadataUpdate actualUpdate = new MetadataUpdate.AddSchema(schema, lastColumnId);
  //   assertEquals(action, actualUpdate, MetadataUpdateParser.fromJson(json));
  // }
  //
  // @Test
  // public void testAddSchemaToJson() {
  //   Schema schema = ID_DATA_SCHEMA;
  //   int lastColumnId = schema.highestFieldId();
  //   String expected = String.format("{\"action\":\"add-schema\",\"schema\":%s,\"last-column-id\":%d}",
  //       SchemaParser.toJson(schema), lastColumnId);
  //   MetadataUpdate update = new MetadataUpdate.AddSchema(schema, lastColumnId);
  //   String actual = MetadataUpdateParser.toJson(update);
  //   Assert.assertEquals("Add schema should convert to the correct JSON value", expected, actual);
  // }
  //
  // @Test
  // public void testSetCurrentSchemaFromJson() {
  //   String action = SET_CURRENT_SCHEMA;
  //   int schemaId = 6;
  //   String json = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
  //   MetadataUpdate.SetCurrentSchema expected = new MetadataUpdate.SetCurrentSchema(schemaId);
  //   assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  // }
  //
  // @Test
  // public void testSetCurrentSchemaToJson() {
  //   String action = SET_CURRENT_SCHEMA;
  //   int schemaId = 6;
  //   String expected = String.format("{\"action\":\"%s\",\"schema-id\":%d}", action, schemaId);
  //   MetadataUpdate update = new MetadataUpdate.SetCurrentSchema(schemaId);
  //   String actual = MetadataUpdateParser.toJson(update);
  //   Assert.assertEquals("Set current schema should convert to the correct JSON value", expected, actual);
  // }
  //
  // @Test
  // public void testSetDefaultPartitionSpecToJson() {
  //   String action = SET_DEFAULT_PARTITION_SPEC;
  //   int specId = 4;
  //   String expected = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
  //   MetadataUpdate update = new MetadataUpdate.SetDefaultPartitionSpec(specId);
  //   String actual = MetadataUpdateParser.toJson(update);
  //   Assert.assertEquals("Set default partition spec should serialize to the correct JSON value", expected, actual);
  // }
  //
  // @Test
  // public void testSetDefaultPartitionSpecFromJson() {
  //   String action = SET_DEFAULT_PARTITION_SPEC;
  //   int specId = 4;
  //   String json = String.format("{\"action\":\"%s\",\"spec-id\":%d}", action, specId);
  //   MetadataUpdate.SetDefaultPartitionSpec expected = new MetadataUpdate.SetDefaultPartitionSpec(specId);
  //   assertEquals(action, expected, MetadataUpdateParser.fromJson(json));
  // }

  public void assertEquals(String requirementType, UpdateRequirement expected, UpdateRequirement actual) {
    switch (requirementType) {
      case UpdateRequirementParser.ASSERT_TABLE_UUID:
        assertEqualsAssertTableUUID((UpdateRequirement.AssertTableUUID) expected,
            (UpdateRequirement.AssertTableUUID) actual);
        break;
      case UpdateRequirementParser.ASSERT_TABLE_DOES_NOT_EXIST:
      case UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID:
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID:
      case UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID:
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID:
      case UpdateRequirementParser.ASSERT_DEFAULT_SPEC_ID:
      case UpdateRequirementParser.ASSERT_DEFAULT_SORT_ORDER_ID:
        Assert.fail(String.format("UpdateRequirementParser equals for %s is not implemented yet", requirementType));
        break;
      default:
        Assert.fail("Unrecognized update requirement type: " + requirementType);
    }
  }

  private static void assertEqualsAssertTableUUID(
      UpdateRequirement.AssertTableUUID expected, UpdateRequirement.AssertTableUUID actual) {
    Assertions.assertThat(actual.uuid()).isNotNull()
        .as("UUID from JSON should be equal").isEqualTo(expected.uuid());
  }

  // private static void assertEqualsUpgradeFormatVersion(
  //     MetadataUpdate.UpgradeFormatVersion expected, MetadataUpdate.UpgradeFormatVersion actual) {
  //   Assert.assertEquals("Format version should be equal", expected.formatVersion(), actual.formatVersion());
  // }
  //
  // private static void assertEqualsAddSchema(MetadataUpdate.AddSchema expected, MetadataUpdate.AddSchema actual) {
  //   Assert.assertTrue("Schemas should be the same", expected.schema().sameSchema(actual.schema()));
  //   Assert.assertEquals("Last column id should be equal", expected.lastColumnId(), actual.lastColumnId());
  // }
  //
  // private static void assertEqualsSetCurrentSchema(
  //     MetadataUpdate.SetCurrentSchema expected, MetadataUpdate.SetCurrentSchema actual) {
  //   Assert.assertEquals("Schema id should be equal", expected.schemaId(), actual.schemaId());
  // }
  //
  // private static void assertEqualsSetDefaultPartitionSpec(
  //     MetadataUpdate.SetDefaultPartitionSpec expected, MetadataUpdate.SetDefaultPartitionSpec actual) {
  //   Assertions.assertThat(actual.specId()).isEqualTo(expected.specId());
  // }
}
