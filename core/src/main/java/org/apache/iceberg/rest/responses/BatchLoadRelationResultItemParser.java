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
package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadRelationResultItemParser {

  private static final String IDENTIFIER = "identifier";
  private static final String STATUS = "status";
  private static final String RESULT = "result";
  private static final String ETAG = "etag";

  private BatchLoadRelationResultItemParser() {}

  public static String toJson(BatchLoadRelationResultItem item) {
    return toJson(item, false);
  }

  public static String toJson(BatchLoadRelationResultItem item, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(item, gen), pretty);
  }

  public static void toJson(BatchLoadRelationResultItem item, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != item, "Invalid batch load relation result item: null");

    gen.writeStartObject();

    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(item.identifier(), gen);

    gen.writeNumberField(STATUS, item.status());

    if (item.result() != null) {
      gen.writeFieldName(RESULT);
      LoadRelationResponseParser.toJson(item.result(), gen);
    }

    if (item.etag() != null) {
      gen.writeStringField(ETAG, item.etag());
    }

    gen.writeEndObject();
  }

  public static BatchLoadRelationResultItem fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadRelationResultItemParser::fromJson);
  }

  public static BatchLoadRelationResultItem fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load relation result item from null object");

    BatchLoadRelationResultItem.Builder builder =
        BatchLoadRelationResultItem.builder()
            .withIdentifier(TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json)))
            .withStatus(JsonUtil.getInt(STATUS, json));

    if (json.hasNonNull(RESULT)) {
      builder.withResult(LoadRelationResponseParser.fromJson(JsonUtil.get(RESULT, json)));
    }

    if (json.hasNonNull(ETAG)) {
      builder.withEtag(JsonUtil.getString(ETAG, json));
    }

    return builder.build();
  }
}
