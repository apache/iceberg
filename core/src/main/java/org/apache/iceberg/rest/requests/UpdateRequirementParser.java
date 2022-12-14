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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.UpdateTableRequest.UpdateRequirement;
import org.apache.iceberg.util.JsonUtil;

public class UpdateRequirementParser {

  private UpdateRequirementParser() {}

  private static final String TYPE = "type";

  // assertion types
  static final String ASSERT_TABLE_UUID = "assert-table-uuid";
  static final String ASSERT_TABLE_DOES_NOT_EXIST = "assert-create";
  static final String ASSERT_REF_SNAPSHOT_ID = "assert-ref-snapshot-id";
  static final String ASSERT_LAST_ASSIGNED_FIELD_ID = "assert-last-assigned-field-id";
  static final String ASSERT_CURRENT_SCHEMA_ID = "assert-current-schema-id";
  static final String ASSERT_LAST_ASSIGNED_PARTITION_ID = "assert-last-assigned-partition-id";
  static final String ASSERT_DEFAULT_SPEC_ID = "assert-default-spec-id";
  static final String ASSERT_DEFAULT_SORT_ORDER_ID = "assert-default-sort-order-id";

  // AssertTableUUID
  private static final String UUID = "uuid";

  // AssertRefSnapshotID
  private static final String NAME = "ref";
  private static final String SNAPSHOT_ID = "snapshot-id";

  // AssertLastAssignedFieldId
  private static final String LAST_ASSIGNED_FIELD_ID = "last-assigned-field-id";

  // AssertCurrentSchemaID
  private static final String SCHEMA_ID = "current-schema-id";

  // AssertLastAssignedPartitionId
  private static final String LAST_ASSIGNED_PARTITION_ID = "last-assigned-partition-id";

  // AssertDefaultSpecID
  private static final String SPEC_ID = "default-spec-id";

  // AssertDefaultSortOrderID
  private static final String SORT_ORDER_ID = "default-sort-order-id";

  private static final Map<Class<? extends UpdateTableRequest.UpdateRequirement>, String> TYPES =
      ImmutableMap.<Class<? extends UpdateTableRequest.UpdateRequirement>, String>builder()
          .put(UpdateRequirement.AssertTableUUID.class, ASSERT_TABLE_UUID)
          .put(UpdateRequirement.AssertTableDoesNotExist.class, ASSERT_TABLE_DOES_NOT_EXIST)
          .put(UpdateRequirement.AssertRefSnapshotID.class, ASSERT_REF_SNAPSHOT_ID)
          .put(UpdateRequirement.AssertLastAssignedFieldId.class, ASSERT_LAST_ASSIGNED_FIELD_ID)
          .put(UpdateRequirement.AssertCurrentSchemaID.class, ASSERT_CURRENT_SCHEMA_ID)
          .put(
              UpdateRequirement.AssertLastAssignedPartitionId.class,
              ASSERT_LAST_ASSIGNED_PARTITION_ID)
          .put(UpdateRequirement.AssertDefaultSpecID.class, ASSERT_DEFAULT_SPEC_ID)
          .put(UpdateRequirement.AssertDefaultSortOrderID.class, ASSERT_DEFAULT_SORT_ORDER_ID)
          .buildOrThrow();

  public static String toJson(UpdateRequirement updateRequirement) {
    return toJson(updateRequirement, false);
  }

  public static String toJson(UpdateRequirement updateRequirement, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(updateRequirement, gen), pretty);
  }

  public static void toJson(UpdateRequirement updateRequirement, JsonGenerator generator)
      throws IOException {
    String requirementType = TYPES.get(updateRequirement.getClass());

    generator.writeStartObject();
    generator.writeStringField(TYPE, requirementType);

    switch (requirementType) {
      case ASSERT_TABLE_DOES_NOT_EXIST:
        // No fields beyond the requirement itself
        break;
      case ASSERT_TABLE_UUID:
        writeAssertTableUUID((UpdateRequirement.AssertTableUUID) updateRequirement, generator);
        break;
      case ASSERT_REF_SNAPSHOT_ID:
        writeAssertRefSnapshotId(
            (UpdateRequirement.AssertRefSnapshotID) updateRequirement, generator);
        break;
      case ASSERT_LAST_ASSIGNED_FIELD_ID:
        writeAssertLastAssignedFieldId(
            (UpdateRequirement.AssertLastAssignedFieldId) updateRequirement, generator);
        break;
      case ASSERT_LAST_ASSIGNED_PARTITION_ID:
        writeAssertLastAssignedPartitionId(
            (UpdateRequirement.AssertLastAssignedPartitionId) updateRequirement, generator);
        break;
      case ASSERT_CURRENT_SCHEMA_ID:
        writeAssertCurrentSchemaId(
            (UpdateRequirement.AssertCurrentSchemaID) updateRequirement, generator);
        break;
      case ASSERT_DEFAULT_SPEC_ID:
        writeAssertDefaultSpecId(
            (UpdateRequirement.AssertDefaultSpecID) updateRequirement, generator);
        break;
      case ASSERT_DEFAULT_SORT_ORDER_ID:
        writeAssertDefaultSortOrderId(
            (UpdateRequirement.AssertDefaultSortOrderID) updateRequirement, generator);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert update requirement to json. Unrecognized type: %s",
                requirementType));
    }

    generator.writeEndObject();
  }

  /**
   * Read MetadataUpdate from a JSON string.
   *
   * @param json a JSON string of a MetadataUpdate
   * @return a MetadataUpdate object
   */
  public static UpdateRequirement fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JSON string: " + json, e);
    }
  }

  public static UpdateRequirement fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse update requirement from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(TYPE), "Cannot parse update requirement. Missing field: type");
    String type = JsonUtil.getString(TYPE, jsonNode).toLowerCase(Locale.ROOT);

    switch (type) {
      case ASSERT_TABLE_DOES_NOT_EXIST:
        return readAssertTableDoesNotExist(jsonNode);
      case ASSERT_TABLE_UUID:
        return readAssertTableUUID(jsonNode);
      case ASSERT_REF_SNAPSHOT_ID:
        return readAssertRefSnapshotId(jsonNode);
      case ASSERT_LAST_ASSIGNED_FIELD_ID:
        return readAssertLastAssignedFieldId(jsonNode);
      case ASSERT_LAST_ASSIGNED_PARTITION_ID:
        return readAssertLastAssignedPartitionId(jsonNode);
      case ASSERT_CURRENT_SCHEMA_ID:
        return readAssertCurrentSchemaId(jsonNode);
      case ASSERT_DEFAULT_SPEC_ID:
        return readAssertDefaultSpecId(jsonNode);
      case ASSERT_DEFAULT_SORT_ORDER_ID:
        return readAssertDefaultSortOrderId(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Unrecognized update requirement. Cannot convert to json: %s", type));
    }
  }

  private static void writeAssertTableUUID(
      UpdateRequirement.AssertTableUUID requirement, JsonGenerator gen) throws IOException {
    gen.writeStringField(UUID, requirement.uuid());
  }

  private static void writeAssertRefSnapshotId(
      UpdateRequirement.AssertRefSnapshotID requirement, JsonGenerator gen) throws IOException {
    gen.writeStringField(NAME, requirement.refName());
    if (requirement.snapshotId() != null) {
      gen.writeNumberField(SNAPSHOT_ID, requirement.snapshotId());
    } else {
      gen.writeNullField(SNAPSHOT_ID);
    }
  }

  private static void writeAssertLastAssignedFieldId(
      UpdateRequirement.AssertLastAssignedFieldId requirement, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(LAST_ASSIGNED_FIELD_ID, requirement.lastAssignedFieldId());
  }

  private static void writeAssertLastAssignedPartitionId(
      UpdateRequirement.AssertLastAssignedPartitionId requirement, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(LAST_ASSIGNED_PARTITION_ID, requirement.lastAssignedPartitionId());
  }

  private static void writeAssertCurrentSchemaId(
      UpdateRequirement.AssertCurrentSchemaID requirement, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SCHEMA_ID, requirement.schemaId());
  }

  private static void writeAssertDefaultSpecId(
      UpdateRequirement.AssertDefaultSpecID requirement, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SPEC_ID, requirement.specId());
  }

  private static void writeAssertDefaultSortOrderId(
      UpdateRequirement.AssertDefaultSortOrderID requirement, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(SORT_ORDER_ID, requirement.sortOrderId());
  }

  @SuppressWarnings(
      "unused") // Keep same signature in case this requirement class evolves and gets fields
  private static UpdateRequirement readAssertTableDoesNotExist(JsonNode node) {
    return new UpdateRequirement.AssertTableDoesNotExist();
  }

  private static UpdateRequirement readAssertTableUUID(JsonNode node) {
    String uuid = JsonUtil.getString(UUID, node);
    return new UpdateRequirement.AssertTableUUID(uuid);
  }

  private static UpdateRequirement readAssertRefSnapshotId(JsonNode node) {
    String name = JsonUtil.getString(NAME, node);
    Long snapshotId = JsonUtil.getLongOrNull(SNAPSHOT_ID, node);
    return new UpdateRequirement.AssertRefSnapshotID(name, snapshotId);
  }

  private static UpdateRequirement readAssertLastAssignedFieldId(JsonNode node) {
    int lastAssignedFieldId = JsonUtil.getInt(LAST_ASSIGNED_FIELD_ID, node);
    return new UpdateRequirement.AssertLastAssignedFieldId(lastAssignedFieldId);
  }

  private static UpdateRequirement readAssertCurrentSchemaId(JsonNode node) {
    int schemaId = JsonUtil.getInt(SCHEMA_ID, node);
    return new UpdateRequirement.AssertCurrentSchemaID(schemaId);
  }

  private static UpdateRequirement readAssertLastAssignedPartitionId(JsonNode node) {
    int lastAssignedPartitionId = JsonUtil.getInt(LAST_ASSIGNED_PARTITION_ID, node);
    return new UpdateRequirement.AssertLastAssignedPartitionId(lastAssignedPartitionId);
  }

  private static UpdateRequirement readAssertDefaultSpecId(JsonNode node) {
    int specId = JsonUtil.getInt(SPEC_ID, node);
    return new UpdateRequirement.AssertDefaultSpecID(specId);
  }

  private static UpdateRequirement readAssertDefaultSortOrderId(JsonNode node) {
    int sortOrderId = JsonUtil.getInt(SORT_ORDER_ID, node);
    return new UpdateRequirement.AssertDefaultSortOrderID(sortOrderId);
  }
}
