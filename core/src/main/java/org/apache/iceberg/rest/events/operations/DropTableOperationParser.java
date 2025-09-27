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

public class DropTableOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String IDENTIFIER = "identifier";
  private static final String TABLE_UUID = "table-uuid";
  private static final String PURGE = "purge";

  private DropTableOperationParser() {}

  public static String toJson(DropTableOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(DropTableOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(DropTableOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(DropTableOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid drop table operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());

    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);

    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    if (operation.purge() != null) {
      gen.writeBooleanField(PURGE, operation.purge());
    }

    gen.writeEndObject();
  }

  public static DropTableOperation fromJson(String json) {
    return JsonUtil.parse(json, DropTableOperationParser::fromJson);
  }

  public static DropTableOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(null != json, "Cannot parse drop table operation from null object");

    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);

    ImmutableDropTableOperation.Builder builder =
        ImmutableDropTableOperation.builder().identifier(identifier).tableUuid(tableUuid);

    if (json.has(PURGE)) {
      builder.purge(JsonUtil.getBool(PURGE, json));
    }

    return builder.build();
  }
}
