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

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Standard response body for all API errors
 */
public class ErrorResponse {

  private String message;
  private String type;
  private int code;

  private ErrorResponse(String message, String type, int code) {
    this.message = message;
    this.type = type;
    this.code = code;
    validate();
  }

  ErrorResponse validate() {
    return this;
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


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("message", message)
        .add("type", type)
        .add("code", code)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String message;
    private String type;
    private Integer code;

    private Builder() {
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

    public ErrorResponse build() {
      Preconditions.checkArgument(code != null, "Invalid response, missing field: code");
      return new ErrorResponse(message, type, code);
    }
  }
}
