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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CustomOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String CUSTOM_OPERATION_TYPE = "custom-type";
  private static final String IDENTIFIER = "identifier";
  private static final String NAMESPACE = "namespace";
  private static final String TABLE_UUID = "table-uuid";
  private static final String VIEW_UUID = "view-uuid";

  private CustomOperationParser() {}

  public static String toJson(CustomOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(CustomOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(CustomOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(CustomOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid custom operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    gen.writeStringField(CUSTOM_OPERATION_TYPE, operation.customOperationType().type());

    if (operation.identifier() != null) {
      gen.writeFieldName(IDENTIFIER);
      TableIdentifierParser.toJson(operation.identifier(), gen);
    }

    if (operation.namespace() != null) {
      JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);
    }

    if (operation.tableUuid() != null) {
      gen.writeStringField(TABLE_UUID, operation.tableUuid());
    }

    if (operation.viewUuid() != null) {
      gen.writeStringField(VIEW_UUID, operation.viewUuid());
    }

    gen.writeEndObject();
  }

  public static CustomOperation fromJson(String json) {
    return JsonUtil.parse(json, CustomOperationParser::fromJson);
  }

  public static CustomOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(null != json, "Cannot parse custom operation from null object");

    OperationType.CustomOperationType customOperationType =
        new OperationType.CustomOperationType(JsonUtil.getString(CUSTOM_OPERATION_TYPE, json));
    ImmutableCustomOperation.Builder builder =
        ImmutableCustomOperation.builder().customOperationType(customOperationType);

    if (json.has(IDENTIFIER)) {
      TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
      builder.identifier(identifier);
    }

    if (json.has(NAMESPACE)) {
      Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
      builder.namespace(namespace);
    }

    if (json.has(TABLE_UUID)) {
      String tableUuid = JsonUtil.getString(TABLE_UUID, json);
      builder.tableUuid(tableUuid);
    }

    if (json.has(VIEW_UUID)) {
      String viewUuid = JsonUtil.getString(VIEW_UUID, json);
      builder.viewUuid(viewUuid);
    }

    return builder.build();
  }
}
