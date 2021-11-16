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

package org.apache.iceberg.rest.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IcebergHttpResponse<T> {

  private final T data;
  private final Error error;

  @JsonCreator
  public IcebergHttpResponse(
      @JsonProperty("data") T data,
      @JsonProperty("error") Error error) {
    this.data = data;
    this.error = error;
  }

  public Error error() {
    return error;
  }

  public T data() {
    return data;
  }

  /**
   * An error object embedded in every HTTP response.
   *
   * On error, this contains:
   *   - message: A short, human-readable description of the error.
   *   - type: Type of exception - more specifically a class name, e.g. NamespaceNotFoundException)
   *   - code: An (optional) application specific error code, to distinguish between causes
   *           of the same HTTP response code (eg possibly different types of Unauthorized exceptions).
   *
   *   #################### Optional fields to consider ######################################
   *   - status: HTTP response code (optional).
   *   - traceId: Unique specific identifier for this error and request, for monitoring purposes.
   *              Presumably this would be an OpenTracing Span (optional).
   *              Will almost certainly add tracing headers as an optional follow-up.
   *   - metadata: Further map of optional metadata (such as further directions to users etc) (optional - unsure?).
   *   #######################################################################################
   *
   *  Example:
   *    "error": {
   *         "message": "Authorization denied: Missing Bearer header",
   *         "type": "OAuthException",
   *         "code": 40102
   *    }
   */
  public static class Error {

    private final String message;
    private final String type;
    private final int code;

    @JsonCreator
    public Error(
        @JsonProperty("message") String message,
        @JsonProperty("type") String type,
        @JsonProperty("code") int code) {
      this.message = message;
      this.type = type;
      this.code = code;
    }
  }
}
