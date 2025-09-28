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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.operations.ImmutableRenameTableOperation;
import org.apache.iceberg.rest.events.operations.RenameTableOperation;
import org.apache.iceberg.util.JsonUtil;

public class RenameTableOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String TABLE_UUID = "table-uuid";
  private static final String SOURCE_IDENTIFIER = "source";
  private static final String DEST_IDENTIFIER = "destination";

  private RenameTableOperationParser() {}

  public static String toJson(RenameTableOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(RenameTableOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(RenameTableOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(RenameTableOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid rename table operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());
    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    gen.writeFieldName(SOURCE_IDENTIFIER);
    TableIdentifierParser.toJson(operation.sourceIdentifier(), gen);

    gen.writeFieldName(DEST_IDENTIFIER);
    TableIdentifierParser.toJson(operation.destIdentifier(), gen);

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static RenameTableOperation fromJson(String json) {
    return JsonUtil.parse(json, RenameTableOperationParser::fromJson);
  }

  public static RenameTableOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(
        null != json, "Cannot parse rename table operation from null object");

    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    TableIdentifier sourceIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(SOURCE_IDENTIFIER, json));
    TableIdentifier destIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(DEST_IDENTIFIER, json));

    return ImmutableRenameTableOperation.builder()
        .tableUuid(tableUuid)
        .sourceIdentifier(sourceIdentifier)
        .destIdentifier(destIdentifier)
        .build();
  }
}
