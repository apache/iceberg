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
import org.apache.iceberg.util.JsonUtil;

public class ConfigResponseParser {

  private static final String DEFAULTS = "defaults";
  private static final String OVERRIDES = "overrides";

  private ConfigResponseParser() {}

  public static String toJson(ConfigResponse response) {
    return toJson(response, false);
  }

  public static String toJson(ConfigResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(ConfigResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid config response: null");

    gen.writeStartObject();

    JsonUtil.writeStringMap(DEFAULTS, response.defaults(), gen);
    JsonUtil.writeStringMap(OVERRIDES, response.overrides(), gen);

    gen.writeEndObject();
  }

  public static ConfigResponse fromJson(String json) {
    return JsonUtil.parse(json, ConfigResponseParser::fromJson);
  }

  public static ConfigResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse config response from null object");

    ConfigResponse.Builder builder = ConfigResponse.builder();

    if (json.hasNonNull(DEFAULTS)) {
      builder.withDefaults(JsonUtil.getStringMapNullableValues(DEFAULTS, json));
    }

    if (json.hasNonNull(OVERRIDES)) {
      builder.withOverrides(JsonUtil.getStringMapNullableValues(OVERRIDES, json));
    }

    return builder.build();
  }
}
