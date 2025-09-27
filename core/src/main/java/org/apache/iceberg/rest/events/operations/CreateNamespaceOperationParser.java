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
package org.apache.iceberg.rest.events.operations;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CreateNamespaceOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String NAMESPACE = "namespace";
  private static final String PROPERTIES = "properties";

  private CreateNamespaceOperationParser() {}

  public static String toJson(CreateNamespaceOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(CreateNamespaceOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(CreateNamespaceOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(CreateNamespaceOperation operation, JsonGenerator gen)
      throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid create namespace operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);

    if (!operation.properties().isEmpty()) {
      gen.writeObjectField(PROPERTIES, operation.properties());
    }

    gen.writeEndObject();
  }

  public static CreateNamespaceOperation fromJson(String json) {
    return JsonUtil.parse(json, CreateNamespaceOperationParser::fromJson);
  }

  public static CreateNamespaceOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(
        null != json, "Cannot parse create namespace operation from null object");

    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));

    ImmutableCreateNamespaceOperation.Builder builder =
        ImmutableCreateNamespaceOperation.builder().namespace(namespace);

    if (json.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, json));
    }

    return builder.build();
  }
}
