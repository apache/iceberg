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
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.CredentialParser;
import org.apache.iceberg.util.JsonUtil;

public class LoadCredentialsResponseParser {
  private static final String STORAGE_CREDENTIALS = "storage-credentials";

  private LoadCredentialsResponseParser() {}

  public static String toJson(LoadCredentialsResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LoadCredentialsResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LoadCredentialsResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid load credentials response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(STORAGE_CREDENTIALS);
    for (Credential credential : response.credentials()) {
      CredentialParser.toJson(credential, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static LoadCredentialsResponse fromJson(String json) {
    return JsonUtil.parse(json, LoadCredentialsResponseParser::fromJson);
  }

  public static LoadCredentialsResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse load credentials response from null object");

    JsonNode credentials = JsonUtil.get(STORAGE_CREDENTIALS, json);
    Preconditions.checkArgument(
        credentials.isArray(), "Cannot parse credentials from non-array: %s", credentials);

    ImmutableLoadCredentialsResponse.Builder builder = ImmutableLoadCredentialsResponse.builder();
    for (JsonNode credential : credentials) {
      builder.addCredentials(CredentialParser.fromJson(credential));
    }

    return builder.build();
  }
}
