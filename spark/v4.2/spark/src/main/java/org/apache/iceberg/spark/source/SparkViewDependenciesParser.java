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
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;
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

  private SparkViewDependenciesParser() {}

  static String toJson(DependencyList dependencyList) {
    return JsonUtil.generate(
        generator -> {
          generator.writeStartArray();
          for (Dependency dependency : dependencyList.dependencies()) {
            generator.writeStartObject();
            if (dependency instanceof TableDependency) {
              generator.writeStringField(TYPE, TABLE);
              JsonUtil.writeStringArray(
                  NAME_PARTS, Arrays.asList(((TableDependency) dependency).nameParts()), generator);
            } else if (dependency instanceof FunctionDependency) {
              generator.writeStringField(TYPE, FUNCTION);
              JsonUtil.writeStringArray(
                  NAME_PARTS,
                  Arrays.asList(((FunctionDependency) dependency).nameParts()),
                  generator);
            } else {
              throw new IllegalArgumentException(
                  "Unsupported Spark view dependency: " + dependency.getClass().getName());
            }
            generator.writeEndObject();
          }
          generator.writeEndArray();
        },
        false);
  }

  static DependencyList fromJson(String json) {
    return JsonUtil.parse(
        json,
        node -> {
          Preconditions.checkArgument(
              node.isArray(), "Cannot parse Spark view dependencies from non-array: %s", node);
          ImmutableList.Builder<Dependency> dependenciesBuilder = ImmutableList.builder();
          for (JsonNode dependency : node) {
            String type = JsonUtil.getString(TYPE, dependency);
            String[] nameParts = JsonUtil.getStringArray(NAME_PARTS, dependency);
            switch (type) {
              case TABLE:
                dependenciesBuilder.add(Dependency.table(nameParts));
                break;
              case FUNCTION:
                dependenciesBuilder.add(Dependency.function(nameParts));
                break;
              default:
                throw new IllegalArgumentException(
                    "Unsupported Spark view dependency type: " + type);
            }
          }

          return DependencyList.of(dependenciesBuilder.build().toArray(new Dependency[0]));
        });
  }
}
