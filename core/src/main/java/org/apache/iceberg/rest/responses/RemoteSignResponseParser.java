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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.util.JsonUtil;

public class RemoteSignResponseParser {

  private static final String URI = "uri";
  private static final String HEADERS = "headers";

  private RemoteSignResponseParser() {}

  public static String toJson(RemoteSignResponse response) {
    return toJson(response, false);
  }

  public static String toJson(RemoteSignResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(RemoteSignResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid remote sign response: null");

    gen.writeStartObject();

    gen.writeStringField(URI, response.uri().toString());
    RemoteSignRequestParser.headersToJson(HEADERS, response.headers(), gen);

    gen.writeEndObject();
  }

  public static RemoteSignResponse fromJson(String json) {
    return JsonUtil.parse(json, RemoteSignResponseParser::fromJson);
  }

  public static RemoteSignResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse remote sign response from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse remote sign response from non-object: %s", json);

    java.net.URI uri = java.net.URI.create(JsonUtil.getString(URI, json));
    Map<String, List<String>> headers = RemoteSignRequestParser.headersFromJson(HEADERS, json);

    return ImmutableRemoteSignResponse.builder().uri(uri).headers(headers).build();
  }
}
