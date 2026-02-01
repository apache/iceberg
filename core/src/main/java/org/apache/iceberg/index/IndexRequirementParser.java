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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public class IndexRequirementParser {

  private IndexRequirementParser() {}

  private static final String TYPE = "type";

  // assertion types
  static final String ASSERT_INDEX_UUID = "assert-index-uuid";
  static final String ASSERT_INDEX_DOES_NOT_EXIST = "assert-create";

  // AssertIndexUUID
  private static final String UUID = "uuid";

  private static final Map<Class<? extends IndexRequirement>, String> TYPES =
      ImmutableMap.<Class<? extends IndexRequirement>, String>builder()
          .put(IndexRequirement.AssertIndexUUID.class, ASSERT_INDEX_UUID)
          .put(IndexRequirement.AssertIndexDoesNotExist.class, ASSERT_INDEX_DOES_NOT_EXIST)
          .buildOrThrow();

  public static String toJson(IndexRequirement updateRequirement) {
    return toJson(updateRequirement, false);
  }

  public static String toJson(IndexRequirement indexRequirement, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(indexRequirement, gen), pretty);
  }

  public static void toJson(IndexRequirement indexRequirement, JsonGenerator generator)
      throws IOException {
    String requirementType = TYPES.get(indexRequirement.getClass());

    generator.writeStartObject();
    generator.writeStringField(TYPE, requirementType);

    switch (requirementType) {
      case ASSERT_INDEX_DOES_NOT_EXIST:
        // No fields beyond the requirement itself
        break;
      case ASSERT_INDEX_UUID:
        writeAssertIndexUUID((IndexRequirement.AssertIndexUUID) indexRequirement, generator);
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
  public static IndexRequirement fromJson(String json) {
    return JsonUtil.parse(json, IndexRequirementParser::fromJson);
  }

  public static IndexRequirement fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse update requirement from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(TYPE), "Cannot parse update requirement. Missing field: type");
    String type = JsonUtil.getString(TYPE, jsonNode).toLowerCase(Locale.ROOT);

    switch (type) {
      case ASSERT_INDEX_DOES_NOT_EXIST:
        return readAssertIndexDoesNotExist(jsonNode);
      case ASSERT_INDEX_UUID:
        return readAssertIndexUUID(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Unrecognized update requirement. Cannot convert to json: %s", type));
    }
  }

  private static void writeAssertIndexUUID(
      IndexRequirement.AssertIndexUUID requirement, JsonGenerator gen) throws IOException {
    gen.writeStringField(UUID, requirement.uuid());
  }

  @SuppressWarnings(
      "unused") // Keep same signature in case this requirement class evolves and gets fields
  private static IndexRequirement readAssertIndexDoesNotExist(JsonNode node) {
    return new IndexRequirement.AssertIndexDoesNotExist();
  }

  private static IndexRequirement readAssertIndexUUID(JsonNode node) {
    String uuid = JsonUtil.getString(UUID, node);
    return new IndexRequirement.AssertIndexUUID(uuid);
  }
}
