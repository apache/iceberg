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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;

/**
 * Serializes a snapshot's constraint statuses as the struct of ID lists in the table spec: {@code
 * validated}, {@code valid}, {@code invalid}, and {@code unvalidated}.
 */
public class ConstraintStatusesParser {
  private ConstraintStatusesParser() {}

  private static final Map<ConstraintStatus, String> FIELDS =
      ImmutableMap.of(
          ConstraintStatus.VALIDATED, "validated",
          ConstraintStatus.VALID, "valid",
          ConstraintStatus.INVALID, "invalid",
          ConstraintStatus.UNVALIDATED, "unvalidated");

  public static String toJson(Map<Integer, ConstraintStatus> statuses) {
    return toJson(statuses, true);
  }

  public static String toJson(Map<Integer, ConstraintStatus> statuses, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(statuses, gen), pretty);
  }

  static void toJson(Map<Integer, ConstraintStatus> statuses, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(statuses != null, "Invalid constraint statuses: null");

    generator.writeStartObject();
    for (Map.Entry<ConstraintStatus, String> field : FIELDS.entrySet()) {
      List<Integer> ids =
          statuses.entrySet().stream()
              .filter(entry -> entry.getValue() == field.getKey())
              .map(Map.Entry::getKey)
              .sorted()
              .collect(Collectors.toList());
      if (!ids.isEmpty()) {
        JsonUtil.writeIntegerArray(field.getValue(), ids, generator);
      }
    }
    generator.writeEndObject();
  }

  public static Map<Integer, ConstraintStatus> fromJson(String json) {
    return JsonUtil.parse(json, ConstraintStatusesParser::fromJson);
  }

  static Map<Integer, ConstraintStatus> fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse constraint statuses from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse constraint statuses from non-object: %s", node);

    Map<Integer, ConstraintStatus> statuses = Maps.newHashMap();
    for (Map.Entry<ConstraintStatus, String> field : FIELDS.entrySet()) {
      if (node.hasNonNull(field.getValue())) {
        for (Integer id : JsonUtil.getIntegerList(field.getValue(), node)) {
          Preconditions.checkArgument(
              statuses.put(id, field.getKey()) == null,
              "Invalid constraint statuses: constraint %s is listed more than once",
              id);
        }
      }
    }

    return ImmutableMap.copyOf(statuses);
  }
}
