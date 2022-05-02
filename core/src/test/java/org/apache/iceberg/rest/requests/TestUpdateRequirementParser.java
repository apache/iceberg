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
import org.apache.iceberg.rest.requests.UpdateTableRequest.UpdateRequirement;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestUpdateRequirementParser {

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
  public void testAssertUUIDFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_TABLE_UUID;
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String json = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement expected = new UpdateRequirement.AssertTableUUID(uuid);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertUUIDToJson() {
    String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
    String expected = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
    UpdateRequirement actual = new UpdateRequirement.AssertTableUUID(uuid);
    Assert.assertEquals("AssertTableUUID should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  @Test
  public void testAssertTableDoesNotExistFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_TABLE_DOES_NOT_EXIST;
    String json = "{\"type\":\"assert-create\"}";
    UpdateRequirement expected = new UpdateRequirement.AssertTableDoesNotExist();
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertTableDoesNotExistToJson() {
    String expected  = "{\"type\":\"assert-create\"}";
    UpdateRequirement actual = new UpdateRequirement.AssertTableDoesNotExist();
    Assert.assertEquals("AssertTableDoesNotExist should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  // TODO - Come back to these after verifying the rename of field `name` to `ref`.
  // TODO - Also verify what combinations of null / missing are we allowed to have.
  // @Test
  // public void testAssertRefSnapshotIdToJson() {
  //   String action = UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID;
  //   String ref = "";
  //   String json = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
  //   UpdateRequirement.AssertTableUUID expected = new UpdateRequirement.AssertTableUUID(uuid);
  //   assertEquals(action, expected, UpdateRequirementParser.fromJson(json));
  // }
  //
  // @Test
  // public void testAssertRefSnapshotIdFromJson() {
  //   String uuid = "2cc52516-5e73-41f2-b139-545d41a4e151";
  //   String expected = String.format("{\"type\":\"assert-table-uuid\",\"uuid\":\"%s\"}", uuid);
  //   UpdateRequirement.AssertTableUUID actual = new UpdateRequirement.AssertTableUUID(uuid);
  //   Assert.assertEquals("AssertTableUUID should convert to the correct JSON value",
  //       expected, UpdateRequirementParser.toJson(actual));
  // }

  @Test
  public void testAssertLastAssignedFieldIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID;
    int lastAssignedFieldId = 12;
    String json = String.format("{\"type\":\"%s\",\"last-assigned-field-id\":%d}", requirementType, lastAssignedFieldId);
    UpdateRequirement expected = new UpdateRequirement.AssertLastAssignedFieldId(lastAssignedFieldId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertLastAssignedFieldIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID;
    int lastAssignedFieldId = 12;
    String expected = String.format("{\"type\":\"%s\",\"last-assigned-field-id\":%d}", requirementType, lastAssignedFieldId);
    UpdateRequirement actual = new UpdateRequirement.AssertLastAssignedFieldId(lastAssignedFieldId);
    Assert.assertEquals("AssertLastAssignedFieldId should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  @Test
  public void testAssertCurrentSchemaIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID;
    int schemaId = 4;
    String json = String.format("{\"type\":\"%s\",\"current-schema-id\":%d}", requirementType, schemaId);
    UpdateRequirement expected = new UpdateRequirement.AssertCurrentSchemaID(schemaId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertCurrentSchemaIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID;
    int schemaId = 4;
    String expected = String.format("{\"type\":\"%s\",\"current-schema-id\":%d}", requirementType, schemaId);
    UpdateRequirement actual = new UpdateRequirement.AssertCurrentSchemaID(schemaId);
    Assert.assertEquals("AssertCurrentSchemaId should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  @Test
  public void testAssertLastAssignedPartitionIdFromJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID;
    int lastAssignedPartitionId = 1004;
    String json = String.format("{\"type\":\"%s\",\"last-assigned-partition-id\":%d}",
        requirementType, lastAssignedPartitionId);
    UpdateRequirement expected = new UpdateRequirement.AssertLastAssignedPartitionId(lastAssignedPartitionId);
    assertEquals(requirementType, expected, UpdateRequirementParser.fromJson(json));
  }

  @Test
  public void testAssertLastAssignedPartitionIdToJson() {
    String requirementType = UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID;
    int lastAssignedPartitionId = 1004;
    String expected = String.format("{\"type\":\"%s\",\"last-assigned-partition-id\":%d}",
        requirementType, lastAssignedPartitionId);
    UpdateRequirement actual = new UpdateRequirement.AssertLastAssignedPartitionId(lastAssignedPartitionId);
    Assert.assertEquals("AssertLastAssignedPartitionId should convert to the correct JSON value",
        expected, UpdateRequirementParser.toJson(actual));
  }

  public void assertEquals(String requirementType, UpdateRequirement expected, UpdateRequirement actual) {
    switch (requirementType) {
      case UpdateRequirementParser.ASSERT_TABLE_UUID:
        assertEqualsAssertTableUUID((UpdateRequirement.AssertTableUUID) expected,
            (UpdateRequirement.AssertTableUUID) actual);
        break;
      case UpdateRequirementParser.ASSERT_TABLE_DOES_NOT_EXIST:
        // Don't cast here as the function explicitly tests that the types are correct, given that the generated JSON
        // for ASSERT_TABLE_DOES_NOT_EXIST does not have any fields other than the requirement type.
        assertEqualsAssertTableDoesNotExist(expected, actual);
        break;
      case UpdateRequirementParser.ASSERT_REF_SNAPSHOT_ID:
        Assert.fail(String.format("UpdateRequirementParser equality comparison for %s is not implemented yet",
            requirementType));
        break;
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_FIELD_ID:
        assertEqualsAssertLastAssignedFieldId((UpdateRequirement.AssertLastAssignedFieldId) expected,
            (UpdateRequirement.AssertLastAssignedFieldId) actual);
        break;
      case UpdateRequirementParser.ASSERT_CURRENT_SCHEMA_ID:
        assertEqualsAssertCurrentSchemaId((UpdateRequirement.AssertCurrentSchemaID) expected,
            (UpdateRequirement.AssertCurrentSchemaID) actual);
        break;
      case UpdateRequirementParser.ASSERT_LAST_ASSIGNED_PARTITION_ID:
        assertEqualsAssertLastAssignedPartitionId((UpdateRequirement.AssertLastAssignedPartitionId) expected,
            (UpdateRequirement.AssertLastAssignedPartitionId) actual);
        break;
      case UpdateRequirementParser.ASSERT_DEFAULT_SPEC_ID:
      case UpdateRequirementParser.ASSERT_DEFAULT_SORT_ORDER_ID:
        Assert.fail(String.format("UpdateRequirementParser equality comparison for %s is not implemented yet",
            requirementType));
        break;
      default:
        Assert.fail("Unrecognized update requirement type: " + requirementType);
    }
  }

  private static void assertEqualsAssertTableUUID(
      UpdateRequirement.AssertTableUUID expected, UpdateRequirement.AssertTableUUID actual) {
    Assertions.assertThat(actual.uuid())
        .as("UUID from JSON should not be null").isNotNull()
        .as("UUID from JSON should parse correctly").isEqualTo(expected.uuid());
  }

  // AssertTableDoesNotExist does not have any fields beyond the requirement type, so just check that the classes
  // are the same and as expected.
  private static void assertEqualsAssertTableDoesNotExist(UpdateRequirement expected, UpdateRequirement actual) {
    Assertions.assertThat(actual)
        .isOfAnyClassIn(UpdateRequirement.AssertTableDoesNotExist.class)
        .hasSameClassAs(expected);
  }

  private static void assertEqualsAssertLastAssignedFieldId(UpdateRequirement.AssertLastAssignedFieldId expected,
      UpdateRequirement.AssertLastAssignedFieldId actual) {
    Assertions.assertThat(actual.lastAssignedFieldId())
        .as("Last assigned field id from JSON should parse correctly")
        .isEqualTo(expected.lastAssignedFieldId());
  }

  private static void assertEqualsAssertCurrentSchemaId(UpdateRequirement.AssertCurrentSchemaID expected,
      UpdateRequirement.AssertCurrentSchemaID actual) {
    Assertions.assertThat(actual.schemaId())
        .as("Current schema id from JSON should parse correctly")
        .isEqualTo(expected.schemaId());
  }

  private static void assertEqualsAssertLastAssignedPartitionId(
      UpdateRequirement.AssertLastAssignedPartitionId expected,
      UpdateRequirement.AssertLastAssignedPartitionId actual) {
    Assertions.assertThat(actual.lastAssignedPartitionId())
        .as("Last assigned partition id should parse correctly from JSON")
        .isEqualTo(expected.lastAssignedPartitionId());
  }
}
