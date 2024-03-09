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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/**
 * Parses TableIdentifiers from a JSON representation, which is the JSON representation utilized in
 * the REST catalog.
 *
 * <p>For TableIdentifier.of("dogs", "owners.and.handlers", "food"), we'd have the following JSON
 * representation, where the dot character of an individual level is in the namespace is replaced by
 * the unit separator byte character.
 *
 * <pre>
 * {
 *   "namespace": ["dogs", "owners.and.handlers"],
 *   "name": "food"
 * }
 * </pre>
 */
public class TableIdentifierParser {

  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";

  private TableIdentifierParser() {}

  public static String toJson(TableIdentifier identifier) {
    return toJson(identifier, false);
  }

  public static String toJson(TableIdentifier identifier, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(identifier, gen), pretty);
  }

  public static void toJson(TableIdentifier identifier, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(NAMESPACE);
    generator.writeArray(identifier.namespace().levels(), 0, identifier.namespace().length());
    generator.writeStringField(NAME, identifier.name());
    generator.writeEndObject();
  }

  public static TableIdentifier fromJson(String json) {
    Preconditions.checkArgument(
        json != null, "Cannot parse table identifier from invalid JSON: null");
    Preconditions.checkArgument(
        !json.isEmpty(), "Cannot parse table identifier from invalid JSON: ''");
    return JsonUtil.parse(json, TableIdentifierParser::fromJson);
  }

  public static TableIdentifier fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull() && node.isObject(),
        "Cannot parse missing or non-object table identifier: %s",
        node);
    List<String> levels = JsonUtil.getStringListOrNull(NAMESPACE, node);
    String tableName = JsonUtil.getString(NAME, node);
    Namespace namespace =
        levels == null ? Namespace.empty() : Namespace.of(levels.toArray(new String[0]));
    return TableIdentifier.of(namespace, tableName);
  }
}
