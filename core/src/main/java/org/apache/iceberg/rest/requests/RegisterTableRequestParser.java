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
import org.apache.iceberg.util.JsonUtil;

public class RegisterTableRequestParser {

  private static final String NAME = "name";
  private static final String METADATA_LOCATION = "metadata-location";

  private RegisterTableRequestParser() {}

  public static String toJson(RegisterTableRequest request) {
    return toJson(request, false);
  }

  public static String toJson(RegisterTableRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(RegisterTableRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid register table request: null");

    gen.writeStartObject();

    gen.writeStringField(NAME, request.name());
    gen.writeStringField(METADATA_LOCATION, request.metadataLocation());

    gen.writeEndObject();
  }

  public static RegisterTableRequest fromJson(String json) {
    return JsonUtil.parse(json, RegisterTableRequestParser::fromJson);
  }

  public static RegisterTableRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse register table request from null object");

    String name = JsonUtil.getString(NAME, json);
    String metadataLocation = JsonUtil.getString(METADATA_LOCATION, json);

    return ImmutableRegisterTableRequest.builder()
        .name(name)
        .metadataLocation(metadataLocation)
        .build();
  }
}
