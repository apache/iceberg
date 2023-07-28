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
package org.apache.iceberg.aws.lambda.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class LambdaRESTRequestParser {

  private static final String METHOD = "method";
  private static final String URI = "uri";
  private static final String HEADERS = "headers";
  private static final String ENTITY = "entity";

  private LambdaRESTRequestParser() {}

  public static String toJson(LambdaRESTRequest request) {
    return toJson(request, false);
  }

  public static String toJson(LambdaRESTRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static InputStream toJsonStream(LambdaRESTRequest request) {
    return JsonUtil.generateStream(gen -> toJson(request, gen), false);
  }

  public static InputStream toJsonStream(LambdaRESTRequest request, boolean pretty) {
    return JsonUtil.generateStream(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(LambdaRESTRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid Lambda REST request: null");

    gen.writeStartObject();
    gen.writeStringField(METHOD, request.method());
    gen.writeStringField(URI, request.uri().toString());
    JsonUtil.writeStringMap(HEADERS, request.headers(), gen);

    if (request.entity() != null) {
      gen.writeStringField(ENTITY, request.entity());
    }

    gen.writeEndObject();
  }

  public static LambdaRESTRequest fromJson(String json) {
    return JsonUtil.parse(json, LambdaRESTRequestParser::fromJson);
  }

  public static LambdaRESTRequest fromJsonStream(InputStream json) {
    return JsonUtil.parseStream(json, LambdaRESTRequestParser::fromJson);
  }

  public static LambdaRESTRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse Lambda REST request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse Lambda REST request from non-object: %s", json);

    String method = JsonUtil.getString(METHOD, json);
    java.net.URI uri = java.net.URI.create(JsonUtil.getString(URI, json));
    Map<String, String> headers = JsonUtil.getStringMap(HEADERS, json);
    String entity = JsonUtil.getStringOrNull(ENTITY, json);
    return ImmutableLambdaRESTRequest.builder()
        .method(method)
        .uri(uri)
        .headers(headers)
        .entity(entity)
        .build();
  }
}
