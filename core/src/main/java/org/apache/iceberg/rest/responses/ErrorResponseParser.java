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
import java.io.StringWriter;
import java.io.UncheckedIOException;
import org.apache.iceberg.util.JsonUtil;

public class ErrorResponseParser {

  private ErrorResponseParser() {
  }

  private static final String MESSAGE = "message";
  private static final String TYPE = "type";
  private static final String CODE = "code";

  public static String toJson(ErrorResponse errorResponse) {
    return toJson(errorResponse, false);
  }

  public static String toJson(ErrorResponse errorResponse, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(errorResponse, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write error response json for: %s", errorResponse), e);
    }
  }

  public static void toJson(ErrorResponse errorResponse, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(MESSAGE, errorResponse.message());
    generator.writeStringField(TYPE, errorResponse.type());
    generator.writeNumberField(CODE, errorResponse.code());
    generator.writeEndObject();
  }

  /**
   * Read ErrorResponse from a JSON string.
   *
   * @param json a JSON string of an ErrorResponse
   * @return an ErrorResponse object
   */
  public static ErrorResponse fromJson(String json) {
    try {
      JsonNode node = JsonUtil.mapper().readValue(json, JsonNode.class);
      return fromJson(node);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JSON string: " + json, e);
    }
  }

  public static ErrorResponse fromJson(JsonNode jsonNode) {
    String message = JsonUtil.getStringOrNull("message", jsonNode);
    String type = JsonUtil.getStringOrNull("type", jsonNode);
    Integer code = JsonUtil.getIntOrNull("code", jsonNode);
    return ErrorResponse.builder()
        .withMessage(message)
        .withType(type)
        .responseCode(code)
        .build();
  }
}

