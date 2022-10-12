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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class NamespaceCreateRequestParser {

  private static final String NAMESPACE = "namespace";
  private static final String PROPERTIES = "properties";

  private NamespaceCreateRequestParser() {}

  public static String toJson(NamespaceCreateRequest request) {
    return toJson(request, false);
  }

  public static String toJson(NamespaceCreateRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(NamespaceCreateRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid namespace creation request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(NAMESPACE);
    for (String level : request.namespace().levels()) {
      gen.writeString(level);
    }
    gen.writeEndArray();

    if (!request.properties().isEmpty()) {
      gen.writeObjectFieldStart(PROPERTIES);
      for (Map.Entry<String, String> pair : request.properties().entrySet()) {
        gen.writeStringField(pair.getKey(), pair.getValue());
      }
      gen.writeEndObject();
    }

    gen.writeEndObject();
  }

  public static NamespaceCreateRequest fromJson(String json) {
    return JsonUtil.parse(json, NamespaceCreateRequestParser::fromJson);
  }

  public static NamespaceCreateRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse namespace creation request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse namespace creation request from non-object: %s", json);

    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    ImmutableNamespaceCreateRequest.Builder builder =
        ImmutableNamespaceCreateRequest.builder().namespace(namespace);
    if (json.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, json));
    }

    return builder.build();
  }
}
