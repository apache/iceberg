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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.JsonUtil;


class NamespaceParser {
  private NamespaceParser() {}

  private static final String NAMESPACE = "namespace";
  private static final String LAST_UPDATED_MILLIS = "last-updated-ms";
  private static final String TABLES = "tables";
  private static final String PROPERTIES = "properties";

  static void toJson(Namespace namespace, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(NAMESPACE, namespace.toString());
    generator.writeNumberField(LAST_UPDATED_MILLIS, namespace.getTimestampMillis());
    generator.writeArrayFieldStart(TABLES);
    for (String table : namespace.getTables()) {
      generator.writeString(table);
    }
    generator.writeEndArray();

    generator.writeObjectFieldStart(PROPERTIES);
    if (namespace.getProperties() != null) {
      namespace.getProperties().forEach((key, value) -> {
        try {
          generator.writeStringField(key, value);
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to write json for: %s", e.getMessage());
        }
      });
    }
    generator.writeEndObject();
    generator.writeEndObject();
  }

  static Namespace fromJson(FileIO io, JsonNode node) {
    Preconditions.checkArgument(node.isObject(),
        "Cannot parse table version from a non-object: %s", node);
    String namespace = JsonUtil.getString(NAMESPACE, node);
    List<String> tableList = JsonUtil.getStringList(TABLES, node);
    long lastUpdatetime = JsonUtil.getLong(LAST_UPDATED_MILLIS, node);
    Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, node);

    Namespace space = Namespace.of(namespace);
    tableList.forEach(space::addTables);
    space.setTimestampMillis(lastUpdatetime);
    properties.forEach(space::setProperties);
    return space;
  }

}
