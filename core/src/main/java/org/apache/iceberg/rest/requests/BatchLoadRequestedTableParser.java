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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadRequestedTableParser {

  private static final String IDENTIFIER = "identifier";
  private static final String IF_NON_MATCH = "if-non-match";

  private BatchLoadRequestedTableParser() {}

  public static String toJson(BatchLoadRequestedTable table) {
    return toJson(table, false);
  }

  public static String toJson(BatchLoadRequestedTable table, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(table, gen), pretty);
  }

  public static void toJson(BatchLoadRequestedTable table, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != table, "Invalid batch load requested table: null");

    gen.writeStartObject();

    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(table.identifier(), gen);

    if (null != table.ifNonMatch()) {
      gen.writeStringField(IF_NON_MATCH, table.ifNonMatch());
    }

    gen.writeEndObject();
  }

  public static BatchLoadRequestedTable fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadRequestedTableParser::fromJson);
  }

  public static BatchLoadRequestedTable fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load requested table from null object");

    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));

    ImmutableBatchLoadRequestedTable.Builder builder =
        ImmutableBatchLoadRequestedTable.builder().identifier(identifier);

    if (json.hasNonNull(IF_NON_MATCH)) {
      builder.ifNonMatch(JsonUtil.getString(IF_NON_MATCH, json));
    }

    return builder.build();
  }
}
