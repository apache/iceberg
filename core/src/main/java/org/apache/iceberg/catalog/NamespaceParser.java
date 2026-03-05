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
package org.apache.iceberg.catalog;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class NamespaceParser {

  private static final String NAMESPACE = "namespace";
  private static final String NAMESPACE_UUID = "namespace-uuid";

  private NamespaceParser() {}

  public static void toJson(Namespace namespace, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(namespace != null, "Invalid namespace: null");
    if (namespace.uuid() != null) {
      generator.writeStartObject();
      generator.writeArrayFieldStart(NAMESPACE);
      for (String level : namespace.levels()) {
        generator.writeString(level);
      }
      generator.writeEndArray();
      generator.writeStringField(NAMESPACE_UUID, namespace.uuid());
      generator.writeEndObject();
    } else {
      generator.writeStartArray();
      for (String level : namespace.levels()) {
        generator.writeString(level);
      }
      generator.writeEndArray();
    }
  }

  public static Namespace fromJson(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (node.isArray()) {
      return Namespace.of(JsonUtil.getStringArray(node));
    } else if (node.isObject()) {
      Preconditions.checkArgument(
          node.has(NAMESPACE), "Cannot parse namespace from object: missing namespace field");
      String[] levels = JsonUtil.getStringArray(node.get(NAMESPACE));
      String uuid = JsonUtil.getStringOrNull(NAMESPACE_UUID, node);
      return Namespace.of(levels, uuid);
    } else {
      throw new IllegalArgumentException(
          "Cannot parse namespace from non-array or non-object node: " + node);
    }
  }
}
