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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.RemoteSignRequestParser;
import org.apache.iceberg.util.JsonUtil;

public class RemoteSigningConfigParser {

  private static final String PROPERTIES = "properties";
  private static final String HEADERS = "headers";

  private RemoteSigningConfigParser() {}

  public static String toJson(RemoteSigningConfig remoteSigningConfig) {
    return toJson(remoteSigningConfig, false);
  }

  public static String toJson(RemoteSigningConfig remoteSigningConfig, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(remoteSigningConfig, gen), pretty);
  }

  public static void toJson(RemoteSigningConfig remoteSigningConfig, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != remoteSigningConfig, "Invalid remote signing config: null");

    gen.writeStartObject();

    if (!remoteSigningConfig.headers().isEmpty()) {
      RemoteSignRequestParser.headersToJson(HEADERS, remoteSigningConfig.headers(), gen);
    }

    if (!remoteSigningConfig.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, remoteSigningConfig.properties(), gen);
    }

    gen.writeEndObject();
  }

  public static RemoteSigningConfig fromJson(String json) {
    return JsonUtil.parse(json, RemoteSigningConfigParser::fromJson);
  }

  public static RemoteSigningConfig fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse remote signing config from null object");

    ImmutableRemoteSigningConfig.Builder builder = ImmutableRemoteSigningConfig.builder();

    if (json.hasNonNull(PROPERTIES)) {
      Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, json);
      builder.properties(properties);
    }

    if (json.hasNonNull(HEADERS)) {
      Map<String, List<String>> headers = RemoteSignRequestParser.headersFromJson(HEADERS, json);
      builder.headers(headers);
    }

    return builder.build();
  }
}
