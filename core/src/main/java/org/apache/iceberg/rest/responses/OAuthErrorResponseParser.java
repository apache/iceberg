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
import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class OAuthErrorResponseParser {

  private OAuthErrorResponseParser() {}

  private static final String ERROR = "error";
  private static final String ERROR_DESCRIPTION = "error_description";
  private static final String ERROR_URI = "error_uri";

  public static String toJson(OAuthErrorResponse errorResponse) {
    return toJson(errorResponse, false);
  }

  public static String toJson(OAuthErrorResponse errorResponse, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(errorResponse, gen), pretty);
  }

  public static void toJson(OAuthErrorResponse errorResponse, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();

    generator.writeStringField(ERROR, errorResponse.error());
    generator.writeStringField(ERROR_DESCRIPTION, errorResponse.errorDescription());
    generator.writeStringField(ERROR_URI, errorResponse.errorUri());

    generator.writeEndObject();
  }

  /**
   * Read OAuthErrorResponse from a JSON string.
   *
   * @param json a JSON string of an OAuthErrorResponse
   * @return an OAuthErrorResponse object
   */
  public static OAuthErrorResponse fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JSON string: " + json, e);
    }
  }

  public static OAuthErrorResponse fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse error response from non-object value: %s",
        jsonNode);
    String error = JsonUtil.getString(ERROR, jsonNode);
    String errorDescription = JsonUtil.getStringOrNull(ERROR_DESCRIPTION, jsonNode);
    String errorUri = JsonUtil.getStringOrNull(ERROR_URI, jsonNode);
    return OAuthErrorResponse.builder()
        .withError(error)
        .withErrorDescription(errorDescription)
        .withErrorUri(errorUri)
        .build();
  }
}
