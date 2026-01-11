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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.operations.DropNamespaceOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropNamespaceOperation;
import org.apache.iceberg.util.JsonUtil;

public class DropNamespaceOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String NAMESPACE = "namespace";

  private DropNamespaceOperationParser() {}

  public static String toJson(DropNamespaceOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(DropNamespaceOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(DropNamespaceOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(DropNamespaceOperation operation, JsonGenerator gen)
      throws IOException {
    Preconditions.checkNotNull(operation, "Invalid drop namespace operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);

    gen.writeEndObject();
  }

  public static DropNamespaceOperation fromJson(String json) {
    return JsonUtil.parse(json, DropNamespaceOperationParser::fromJson);
  }

  public static DropNamespaceOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(json, "Cannot parse drop namespace operation from null object");

    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));

    return ImmutableDropNamespaceOperation.builder().namespace(namespace).build();
  }
}
