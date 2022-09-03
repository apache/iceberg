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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTErrorResponse;

/** Standard response body for all API errors */
public class OAuthErrorResponse implements RESTErrorResponse {

  private String error;
  private String errorDescription;
  private String errorUri;

  private OAuthErrorResponse(String error, String errorDescription, String errorUri) {
    this.error = error;
    this.errorDescription = errorDescription;
    this.errorUri = errorUri;
    validate();
  }

  @Override
  public void validate() {
    // Because we use the `ErrorResponseParser`, validation is done there.
  }

  public String error() {
    return error;
  }

  public String errorDescription() {
    return errorDescription;
  }

  public String errorUri() {
    return errorUri;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    return sb.append("OAuthErrorResponse(")
        .append("error=")
        .append(error)
        .append(", errorDescription=")
        .append(errorDescription)
        .append(", errorUri=")
        .append(errorUri)
        .append(")")
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String error;
    private String errorDescription;
    private String errorUri;

    private Builder() {}

    public Builder withError(String type) {
      this.error = type;
      return this;
    }

    public Builder withErrorDescription(String description) {
      this.errorDescription = description;
      return this;
    }

    public Builder withErrorUri(String uri) {
      this.errorUri = uri;
      return this;
    }

    public OAuthErrorResponse build() {
      Preconditions.checkArgument(error != null, "Invalid response, missing field: error");
      return new OAuthErrorResponse(error, errorDescription, errorUri);
    }
  }
}
