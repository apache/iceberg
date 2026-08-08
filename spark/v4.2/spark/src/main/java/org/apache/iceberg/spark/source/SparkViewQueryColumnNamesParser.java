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
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Converts Spark view query column names to and from a property-safe JSON representation. */
class SparkViewQueryColumnNamesParser {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SparkViewQueryColumnNamesParser() {}

  static String toJson(String[] columnNames) {
    try {
      return MAPPER.writeValueAsString(columnNames);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write Spark view query column names", e);
    }
  }

  static String[] fromProperty(String value) {
    if (value.equals("[]") || value.startsWith("[\"")) {
      try {
        JsonNode node = MAPPER.readTree(value);
        Preconditions.checkArgument(
            node.isArray(), "Cannot parse Spark view query column names from non-array: %s", node);
        String[] columnNames = new String[node.size()];
        for (int index = 0; index < node.size(); index += 1) {
          JsonNode columnName = node.get(index);
          Preconditions.checkArgument(
              columnName.isTextual(),
              "Cannot parse Spark view query column name from non-string: %s",
              columnName);
          columnNames[index] = columnName.asText();
        }

        return columnNames;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to parse Spark view query column names", e);
      }
    }

    return value.split(",", -1);
  }
}
