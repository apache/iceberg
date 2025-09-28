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
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirementParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateTableOperation;
import org.apache.iceberg.rest.events.operations.UpdateTableOperation;
import org.apache.iceberg.util.JsonUtil;

public class UpdateTableOperationParser {
  private static final String OPERATION_TYPE = "operation-type";
  private static final String IDENTIFIER = "identifier";
  private static final String TABLE_UUID = "table-uuid";
  private static final String UPDATES = "updates";
  private static final String REQUIREMENTS = "requirements";

  private UpdateTableOperationParser() {}

  public static String toJson(UpdateTableOperation operation) {
    return toJson(operation, false);
  }

  public static String toJsonPretty(UpdateTableOperation operation) {
    return toJson(operation, true);
  }

  private static String toJson(UpdateTableOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(UpdateTableOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(null != operation, "Invalid update table operation: null");

    gen.writeStartObject();

    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());

    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);

    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    gen.writeArrayFieldStart(UPDATES);
    for (MetadataUpdate update : operation.updates()) {
      MetadataUpdateParser.toJson(update, gen);
    }
    gen.writeEndArray();

    if (!operation.requirements().isEmpty()) {
      gen.writeArrayFieldStart(REQUIREMENTS);
      for (UpdateRequirement req : operation.requirements()) {
        UpdateRequirementParser.toJson(req, gen);
      }
      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static UpdateTableOperation fromJson(String json) {
    return JsonUtil.parse(json, UpdateTableOperationParser::fromJson);
  }

  public static UpdateTableOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(
        null != json, "Cannot parse update table operation from null object");

    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    List<MetadataUpdate> updates =
        JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);

    ImmutableUpdateTableOperation.Builder builder =
        ImmutableUpdateTableOperation.builder()
            .identifier(identifier)
            .tableUuid(tableUuid)
            .updates(updates);

    if (json.has(REQUIREMENTS)) {
      builder.requirements(
          JsonUtil.getObjectList(REQUIREMENTS, json, UpdateRequirementParser::fromJson));
    }

    return builder.build();
  }
}
