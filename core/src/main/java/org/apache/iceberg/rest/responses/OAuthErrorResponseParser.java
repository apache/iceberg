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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class OAuthErrorResponseParser {

  private OAuthErrorResponseParser() {}

  private static final String ERROR = "error";
  private static final String ERROR_DESCRIPTION = "error_description";

  /**
   * Read OAuthErrorResponse from a JSON string.
   *
   * @param json a JSON string of an OAuthErrorResponse
   * @return an OAuthErrorResponse object
   */
  public static ErrorResponse fromJson(int code, String json) {
    return JsonUtil.parse(json, node -> OAuthErrorResponseParser.fromJson(code, node));
  }

  public static ErrorResponse fromJson(int code, JsonNode jsonNode) {
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse error response from non-object value: %s",
        jsonNode);
    String error = JsonUtil.getString(ERROR, jsonNode);
    String errorDescription = JsonUtil.getStringOrNull(ERROR_DESCRIPTION, jsonNode);
    return ErrorResponse.builder()
        .responseCode(code)
        .withType(error)
        .withMessage(errorDescription)
        .build();
  }
}
