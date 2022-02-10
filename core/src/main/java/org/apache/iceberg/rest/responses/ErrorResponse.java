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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Standard response body on error.
 */
public class ErrorResponse {

  private final String message;
  private final String type;
  private final Integer code;

  // These aren't part of the spec, but they are likely needed when we abstract out the current HTTP client.
  @JsonIgnore
  private final String body;
  @JsonIgnore
  private Map<String, String> headers;

  private ErrorResponse(String message, String type, Integer code) {
    this(message, type, code, null, Collections.emptyMap());
  }

  private ErrorResponse(String message, String type, Integer code, String body, Map<String, String> headers) {
    this.message = message;
    this.type = type;
    this.code = code;
    this.body = body;
    this.headers = headers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String message() {
    return message;
  }

  public String type() {
    return type;
  }

  public Integer code() {
    return code;
  }

  public String body() {
    return body;
  }

  public Map<String, String> headers() {
    return ImmutableMap.copyOf(headers);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("message", message)
        .add("type", type)
        .add("code", code)
        .add("body", body)
        .add("headers", headers)
        .omitNullValues()
        .toString();
  }

  public static class Builder {
    private String message;
    private String type;
    private Integer code;
    private String body;
    private Map<String, String> headers;

    private Builder() {
      this.headers = Maps.newHashMap();
    }

    public Builder withMessage(String errorMessage) {
      this.message = errorMessage;
      return this;
    }

    public Builder withType(String errorType) {
      this.type = errorType;
      return this;
    }

    public Builder responseCode(Integer responseCode) {
      this.code = responseCode;
      return this;
    }

    public Builder responseBody(String rawBody) {
      this.body = rawBody;
      return this;
    }

    public Builder addHeader(String key, String value) {
      this.headers.put(key, value);
      return this;
    }

    public Builder addHeaders(Map<String, String> additionalHeaders) {
      this.headers.putAll(additionalHeaders);
      return this;
    }

    public ErrorResponse build() {
      return new ErrorResponse(message, type, code, body, headers);
    }
  }
}

