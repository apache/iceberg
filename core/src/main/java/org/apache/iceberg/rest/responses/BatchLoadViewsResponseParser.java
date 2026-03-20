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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadViewsResponseParser {

  private static final String VIEWS = "views";

  private BatchLoadViewsResponseParser() {}

  public static String toJson(BatchLoadViewsResponse response) {
    return toJson(response, false);
  }

  public static String toJson(BatchLoadViewsResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(BatchLoadViewsResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid batch load views response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(VIEWS);
    for (BatchLoadViewsResultItem item : response.views()) {
      BatchLoadViewsResultItemParser.toJson(item, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchLoadViewsResponse fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadViewsResponseParser::fromJson);
  }

  public static BatchLoadViewsResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load views response from null object");
    Preconditions.checkArgument(json.hasNonNull(VIEWS), "Cannot parse missing field: %s", VIEWS);

    ImmutableList.Builder<BatchLoadViewsResultItem> views = ImmutableList.builder();
    JsonNode viewsNode = JsonUtil.get(VIEWS, json);
    Preconditions.checkArgument(
        viewsNode.isArray(), "Cannot parse %s from non-array: %s", VIEWS, viewsNode);

    for (JsonNode itemNode : viewsNode) {
      views.add(BatchLoadViewsResultItemParser.fromJson(itemNode));
    }

    return ImmutableBatchLoadViewsResponse.builder().views(views.build()).build();
  }
}
