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
package org.apache.iceberg.spark.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.connector.catalog.Dependency;
import org.apache.spark.sql.connector.catalog.DependencyList;
import org.apache.spark.sql.connector.catalog.FunctionDependency;
import org.apache.spark.sql.connector.catalog.TableDependency;

/**
 * Converts Spark's structured view dependencies to JSON for storage in Iceberg view properties.
 *
 * <p>Iceberg view properties contain strings, while Spark dependencies retain both the dependency
 * type and multipart name.
 */
class SparkViewDependenciesParser {
  private static final String TYPE = "type";
  private static final String NAME_PARTS = "name-parts";
  private static final String TABLE = "table";
  private static final String FUNCTION = "function";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SparkViewDependenciesParser() {}

  static String toJson(DependencyList dependencyList) {
    ArrayNode dependenciesNode = MAPPER.createArrayNode();
    for (Dependency dependency : dependencyList.dependencies()) {
      ObjectNode dependencyNode = dependenciesNode.addObject();
      String[] nameParts;
      if (dependency instanceof TableDependency) {
        dependencyNode.put(TYPE, TABLE);
        nameParts = ((TableDependency) dependency).nameParts();
      } else if (dependency instanceof FunctionDependency) {
        dependencyNode.put(TYPE, FUNCTION);
        nameParts = ((FunctionDependency) dependency).nameParts();
      } else {
        throw new IllegalArgumentException(
            "Unsupported Spark view dependency: " + dependency.getClass().getName());
      }

      ArrayNode namePartsNode = dependencyNode.putArray(NAME_PARTS);
      for (String namePart : nameParts) {
        namePartsNode.add(namePart);
      }
    }

    try {
      return MAPPER.writeValueAsString(dependenciesNode);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write Spark view dependencies", e);
    }
  }

  static DependencyList fromJson(String json) {
    JsonNode node;
    try {
      node = MAPPER.readTree(json);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to parse Spark view dependencies", e);
    }

    Preconditions.checkArgument(
        node.isArray(), "Cannot parse Spark view dependencies from non-array: %s", node);
    ImmutableList.Builder<Dependency> dependenciesBuilder = ImmutableList.builder();
    for (JsonNode dependency : node) {
      String type = string(TYPE, dependency);
      String[] nameParts = stringArray(NAME_PARTS, dependency);
      switch (type) {
        case TABLE:
          dependenciesBuilder.add(Dependency.table(nameParts));
          break;
        case FUNCTION:
          dependenciesBuilder.add(Dependency.function(nameParts));
          break;
        default:
          throw new IllegalArgumentException("Unsupported Spark view dependency type: " + type);
      }
    }

    return DependencyList.of(dependenciesBuilder.build().toArray(new Dependency[0]));
  }

  private static String string(String property, JsonNode node) {
    JsonNode value = node.get(property);
    Preconditions.checkArgument(
        value != null && value.isTextual(),
        "Cannot parse Spark view dependency string: %s: %s",
        property,
        value);
    return value.asText();
  }

  private static String[] stringArray(String property, JsonNode node) {
    JsonNode values = node.get(property);
    Preconditions.checkArgument(
        values != null && values.isArray(),
        "Cannot parse Spark view dependency string array: %s: %s",
        property,
        values);
    String[] result = new String[values.size()];
    for (int index = 0; index < values.size(); index += 1) {
      JsonNode value = values.get(index);
      Preconditions.checkArgument(
          value.isTextual(),
          "Cannot parse Spark view dependency string from non-string: %s",
          value);
      result[index] = value.asText();
    }

    return result;
  }
}
