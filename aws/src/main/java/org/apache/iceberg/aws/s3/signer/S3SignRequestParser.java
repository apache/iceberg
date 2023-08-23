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
package org.apache.iceberg.aws.s3.signer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;

public class S3SignRequestParser {

  private static final String REGION = "region";
  private static final String METHOD = "method";
  private static final String URI = "uri";
  private static final String HEADERS = "headers";
  private static final String PROPERTIES = "properties";
  private static final String BODY = "body";

  private S3SignRequestParser() {}

  public static String toJson(S3SignRequest request) {
    return toJson(request, false);
  }

  public static String toJson(S3SignRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(S3SignRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid s3 sign request: null");

    gen.writeStartObject();

    gen.writeStringField(REGION, request.region());
    gen.writeStringField(METHOD, request.method());
    gen.writeStringField(URI, request.uri().toString());
    headersToJson(HEADERS, request.headers(), gen);

    if (!request.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, request.properties(), gen);
    }

    if (request.body() != null && !request.body().isEmpty()) {
      gen.writeStringField(BODY, request.body());
    }

    gen.writeEndObject();
  }

  public static S3SignRequest fromJson(String json) {
    return JsonUtil.parse(json, S3SignRequestParser::fromJson);
  }

  public static S3SignRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse s3 sign request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse s3 sign request from non-object: %s", json);

    String region = JsonUtil.getString(REGION, json);
    String method = JsonUtil.getString(METHOD, json);
    java.net.URI uri = java.net.URI.create(JsonUtil.getString(URI, json));
    Map<String, List<String>> headers = headersFromJson(HEADERS, json);

    ImmutableS3SignRequest.Builder builder =
        ImmutableS3SignRequest.builder().region(region).method(method).uri(uri).headers(headers);

    if (json.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, json));
    }

    if (json.has(BODY)) {
      builder.body(JsonUtil.getString(BODY, json));
    }

    return builder.build();
  }

  static void headersToJson(String property, Map<String, List<String>> headers, JsonGenerator gen)
      throws IOException {
    gen.writeObjectFieldStart(property);
    for (Entry<String, List<String>> entry : headers.entrySet()) {
      gen.writeFieldName(entry.getKey());

      gen.writeStartArray();
      for (String val : entry.getValue()) {
        gen.writeString(val);
      }
      gen.writeEndArray();
    }
    gen.writeEndObject();
  }

  static Map<String, List<String>> headersFromJson(String property, JsonNode json) {
    Map<String, List<String>> headers = Maps.newHashMap();
    JsonNode headersNode = JsonUtil.get(property, json);
    headersNode
        .fields()
        .forEachRemaining(
            entry -> {
              String key = entry.getKey();
              List<String> values = Arrays.asList(JsonUtil.getStringArray(entry.getValue()));
              headers.put(key, values);
            });
    return headers;
  }
}
