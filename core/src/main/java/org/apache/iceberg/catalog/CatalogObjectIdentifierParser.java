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
 * Parses {@link CatalogObjectIdentifier} from a JSON representation.
 *
 * <pre>
 * {
 *   "namespace": ["db", "schema"],
 *   "name": "my_table"
 * }
 * </pre>
 */
public class CatalogObjectIdentifierParser {
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";

  private CatalogObjectIdentifierParser() {}

  public static String toJson(CatalogObjectIdentifier identifier) {
    return toJson(identifier, false);
  }

  public static String toJson(CatalogObjectIdentifier identifier, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(identifier, gen), pretty);
  }

  public static void toJson(CatalogObjectIdentifier identifier, JsonGenerator gen)
      throws IOException {
    Preconditions.checkNotNull(identifier, "Invalid catalog object identifier: null");
    gen.writeStartObject();
    gen.writeFieldName(NAMESPACE);
    gen.writeArray(identifier.namespace().levels(), 0, identifier.namespace().length());
    gen.writeStringField(NAME, identifier.name());
    gen.writeEndObject();
  }

  public static CatalogObjectIdentifier fromJson(String json) {
    Preconditions.checkArgument(
        json != null, "Cannot parse catalog object identifier from invalid JSON: null");
    Preconditions.checkArgument(
        !json.isEmpty(), "Cannot parse catalog object identifier from invalid JSON: ''");
    return JsonUtil.parse(json, CatalogObjectIdentifierParser::fromJson);
  }

  public static CatalogObjectIdentifier fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull() && node.isObject(),
        "Cannot parse missing or non-object catalog object identifier: %s",
        node);
    List<String> levels = JsonUtil.getStringListOrNull(NAMESPACE, node);
    String name = JsonUtil.getString(NAME, node);
    Namespace namespace =
        levels == null ? Namespace.empty() : Namespace.of(levels.toArray(new String[0]));
    return CatalogObjectIdentifier.of(namespace, name);
  }
}
