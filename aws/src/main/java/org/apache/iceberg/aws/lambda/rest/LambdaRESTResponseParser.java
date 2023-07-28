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
import java.io.OutputStream;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class LambdaRESTResponseParser {

  private static final String CODE = "code";
  private static final String HEADERS = "headers";
  private static final String ENTITY = "entity";
  private static final String REASON = "reason";

  private LambdaRESTResponseParser() {}

  public static String toJson(LambdaRESTResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LambdaRESTResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static InputStream toJsonStream(LambdaRESTResponse response) {
    return JsonUtil.generateStream(gen -> toJson(response, gen), false);
  }

  public static InputStream toJsonStream(LambdaRESTResponse response, boolean pretty) {
    return JsonUtil.generateStream(gen -> toJson(response, gen), pretty);
  }

  public static void writeToStream(OutputStream stream, LambdaRESTResponse response) {
    JsonUtil.writeToStream(stream, gen -> toJson(response, gen), false);
  }

  public static void writeToStream(
      OutputStream stream, LambdaRESTResponse response, boolean pretty) {
    JsonUtil.writeToStream(stream, gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LambdaRESTResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid Lambda REST response: null");

    gen.writeStartObject();
    gen.writeNumberField(CODE, response.code());
    JsonUtil.writeStringMap(HEADERS, response.headers(), gen);

    if (response.entity() != null) {
      gen.writeStringField(ENTITY, response.entity());
    }

    if (response.reason() != null) {
      gen.writeStringField(REASON, response.reason());
    }

    gen.writeEndObject();
  }

  public static LambdaRESTResponse fromJson(String json) {
    return JsonUtil.parse(json, LambdaRESTResponseParser::fromJson);
  }

  public static LambdaRESTResponse fromJsonStream(InputStream json) {
    return JsonUtil.parseStream(json, LambdaRESTResponseParser::fromJson);
  }

  public static LambdaRESTResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse Lambda REST response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse Lambda REST response from non-object: %s", json);

    int code = JsonUtil.getInt(CODE, json);
    Map<String, String> headers = JsonUtil.getStringMap(HEADERS, json);
    String entity = JsonUtil.getStringOrNull(ENTITY, json);
    String reason = JsonUtil.getStringOrNull(REASON, json);
    return ImmutableLambdaRESTResponse.builder()
        .code(code)
        .headers(headers)
        .entity(entity)
        .reason(reason)
        .build();
  }
}
