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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

public class RemoteSignBatchRequestParser {

  private static final String REQUESTS = "requests";
  private static final String PROPERTIES = "properties";

  private RemoteSignBatchRequestParser() {}

  public static String toJson(RemoteSignBatchRequest request) {
    return toJson(request, false);
  }

  public static String toJson(RemoteSignBatchRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(RemoteSignBatchRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid remote sign batch request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(REQUESTS);
    for (RemoteSignRequest element : request.requests()) {
      RemoteSignRequestParser.toJson(element, gen);
    }

    gen.writeEndArray();
    if (!request.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, request.properties(), gen);
    }

    gen.writeEndObject();
  }

  public static RemoteSignBatchRequest fromJson(String json) {
    return JsonUtil.parse(json, RemoteSignBatchRequestParser::fromJson);
  }

  public static RemoteSignBatchRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse remote sign batch request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse remote sign batch request from non-object: %s", json);

    JsonNode requests = JsonUtil.get(REQUESTS, json);
    Preconditions.checkArgument(
        requests.isArray(), "Cannot parse requests from non-array: %s", requests);

    ImmutableRemoteSignBatchRequest.Builder builder =
        ImmutableRemoteSignBatchRequest.builder()
            .requests(
                Lists.newArrayList(requests).stream().map(RemoteSignRequestParser::fromJson)
                    ::iterator);
    if (json.hasNonNull(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, json));
    }

    return builder.build();
  }
}
