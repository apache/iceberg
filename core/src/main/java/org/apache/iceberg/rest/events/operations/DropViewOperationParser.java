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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class DropViewOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String IDENTIFIER = "identifier";
  private static final String VIEW_UUID = "view-uuid";

  private DropViewOperationParser() {}

  public static String toJson(DropViewOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(DropViewOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(DropViewOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(DropViewOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid drop view operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());

    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);

    gen.writeStringField(VIEW_UUID, operation.viewUuid());

    gen.writeEndObject();
  }

  public static DropViewOperation fromJson(String json) {
    return JsonUtil.parse(json, DropViewOperationParser::fromJson);
  }

  public static DropViewOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(null != json, "Cannot parse drop view operation from null object");

    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String viewUuid = JsonUtil.getString(VIEW_UUID, json);

    return ImmutableDropViewOperation.builder().identifier(identifier).viewUuid(viewUuid).build();
  }
}
