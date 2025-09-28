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
package org.apache.iceberg.rest.events.parsers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateNamespacePropertiesOperation;
import org.apache.iceberg.rest.events.operations.UpdateNamespacePropertiesOperation;
import org.apache.iceberg.util.JsonUtil;

public class UpdateNamespacePropertiesOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String NAMESPACE = "namespace";
  private static final String UPDATED = "updated";
  private static final String REMOVED = "removed";
  private static final String MISSING = "missing";

  private UpdateNamespacePropertiesOperationParser() {}

  public static String toJson(UpdateNamespacePropertiesOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(UpdateNamespacePropertiesOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(UpdateNamespacePropertiesOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(UpdateNamespacePropertiesOperation operation, JsonGenerator gen)
      throws IOException {
    Preconditions.checkNotNull(operation, "Invalid update namespace properties operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);
    JsonUtil.writeStringArray(UPDATED, operation.updated(), gen);
    JsonUtil.writeStringArray(REMOVED, operation.removed(), gen);

    if (!operation.missing().isEmpty()) {
      JsonUtil.writeStringArray(MISSING, operation.missing(), gen);
    }

    gen.writeEndObject();
  }

  public static UpdateNamespacePropertiesOperation fromJson(String json) {
    return JsonUtil.parse(json, UpdateNamespacePropertiesOperationParser::fromJson);
  }

  public static UpdateNamespacePropertiesOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(json, "Cannot parse update namespace properties operation from null object");

    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    List<String> updated = JsonUtil.getStringList(UPDATED, json);
    List<String> removed = JsonUtil.getStringList(REMOVED, json);

    ImmutableUpdateNamespacePropertiesOperation.Builder builder =
        ImmutableUpdateNamespacePropertiesOperation.builder()
            .namespace(namespace)
            .updated(updated)
            .removed(removed);

    if (json.has(MISSING)) {
      builder.missing(JsonUtil.getStringList(MISSING, json));
    }

    return builder.build();
  }
}
