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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadViewsRequestParser {

  private static final String VIEWS = "views";

  private BatchLoadViewsRequestParser() {}

  public static String toJson(BatchLoadViewsRequest request) {
    return toJson(request, false);
  }

  public static String toJson(BatchLoadViewsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(BatchLoadViewsRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid batch load views request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(VIEWS);
    for (TableIdentifier view : request.views()) {
      TableIdentifierParser.toJson(view, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchLoadViewsRequest fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadViewsRequestParser::fromJson);
  }

  public static BatchLoadViewsRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load views request from null object");
    Preconditions.checkArgument(json.hasNonNull(VIEWS), "Cannot parse missing field: %s", VIEWS);

    ImmutableList.Builder<TableIdentifier> views = ImmutableList.builder();
    JsonNode viewsNode = JsonUtil.get(VIEWS, json);
    Preconditions.checkArgument(
        viewsNode.isArray(), "Cannot parse %s from non-array: %s", VIEWS, viewsNode);

    for (JsonNode viewNode : viewsNode) {
      views.add(TableIdentifierParser.fromJson(viewNode));
    }

    return ImmutableBatchLoadViewsRequest.builder().views(views.build()).build();
  }
}
