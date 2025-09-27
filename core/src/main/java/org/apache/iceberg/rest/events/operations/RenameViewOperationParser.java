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

public class RenameViewOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String VIEW_UUID = "view-uuid";
  private static final String SOURCE_IDENTIFIER = "source";
  private static final String DEST_IDENTIFIER = "destination";

  private RenameViewOperationParser() {}

  public static String toJson(RenameViewOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(RenameViewOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(RenameViewOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(RenameViewOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid rename view operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    gen.writeStringField(VIEW_UUID, operation.viewUuid());

    gen.writeFieldName(SOURCE_IDENTIFIER);
    TableIdentifierParser.toJson(operation.sourceIdentifier(), gen);

    gen.writeFieldName(DEST_IDENTIFIER);
    TableIdentifierParser.toJson(operation.destIdentifier(), gen);

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static RenameViewOperation fromJson(String json) {
    return JsonUtil.parse(json, RenameViewOperationParser::fromJson);
  }

  public static RenameViewOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(null != json, "Cannot parse rename view operation from null object");

    String viewUuid = JsonUtil.getString(VIEW_UUID, json);
    TableIdentifier sourceIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(SOURCE_IDENTIFIER, json));
    TableIdentifier destIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(DEST_IDENTIFIER, json));

    return ImmutableRenameViewOperation.builder()
        .viewUuid(viewUuid)
        .sourceIdentifier(sourceIdentifier)
        .destIdentifier(destIdentifier)
        .build();
  }
}
