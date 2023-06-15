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
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class CommitTransactionRequestParser {
  private static final String TABLE_CHANGES = "table-changes";
  private static final String IDENTIFIER = "identifier";
  private static final String REQUIREMENTS = "requirements";
  private static final String UPDATES = "updates";

  private CommitTransactionRequestParser() {}

  public static String toJson(CommitTransactionRequest request) {
    return toJson(request, false);
  }

  public static String toJson(CommitTransactionRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(CommitTransactionRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid commit tx request: null");

    gen.writeStartObject();
    gen.writeFieldName(TABLE_CHANGES);
    gen.writeStartArray();

    for (UpdateTableRequest tableChange : request.tableChanges()) {
      gen.writeStartObject();

      gen.writeFieldName(IDENTIFIER);
      TableIdentifierParser.toJson(tableChange.identifier(), gen);

      gen.writeArrayFieldStart(REQUIREMENTS);
      for (UpdateTableRequest.UpdateRequirement updateRequirement : tableChange.requirements()) {
        UpdateRequirementParser.toJson(updateRequirement, gen);
      }
      gen.writeEndArray();

      gen.writeArrayFieldStart(UPDATES);
      for (MetadataUpdate metadataUpdate : tableChange.updates()) {
        MetadataUpdateParser.toJson(metadataUpdate, gen);
      }
      gen.writeEndArray();

      gen.writeEndObject();
    }

    gen.writeEndArray();
    gen.writeEndObject();
  }

  public static CommitTransactionRequest fromJson(String json) {
    return JsonUtil.parse(json, CommitTransactionRequestParser::fromJson);
  }

  public static CommitTransactionRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse commit tx request from null object");

    List<UpdateTableRequest> tableChanges = Lists.newArrayList();
    JsonNode changes = JsonUtil.get(TABLE_CHANGES, json);

    Preconditions.checkArgument(
        changes.isArray(), "Cannot parse commit tx request from non-array: %s", changes);

    for (JsonNode node : changes) {
      TableIdentifier identifier = null;
      List<UpdateTableRequest.UpdateRequirement> requirements = Lists.newArrayList();
      List<MetadataUpdate> updates = Lists.newArrayList();

      if (node.hasNonNull(IDENTIFIER)) {
        identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, node));
      }

      if (node.hasNonNull(REQUIREMENTS)) {
        JsonNode requirementsNode = JsonUtil.get(REQUIREMENTS, node);
        Preconditions.checkArgument(
            requirementsNode.isArray(),
            "Cannot parse requirements from non-array: %s",
            requirementsNode);
        requirementsNode.forEach(req -> requirements.add(UpdateRequirementParser.fromJson(req)));
      }

      if (node.hasNonNull(UPDATES)) {
        JsonNode updatesNode = JsonUtil.get(UPDATES, node);
        Preconditions.checkArgument(
            updatesNode.isArray(), "Cannot parse metadata updates from non-array: %s", updatesNode);

        updatesNode.forEach(update -> updates.add(MetadataUpdateParser.fromJson(update)));
      }

      tableChanges.add(new UpdateTableRequest(identifier, requirements, updates));
    }

    return new CommitTransactionRequest(tableChanges);
  }
}
