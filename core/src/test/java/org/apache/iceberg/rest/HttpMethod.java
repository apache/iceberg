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
package org.apache.iceberg.rest;

public enum HttpMethod {
  POST("POST", true, true),
  GET("GET", false, true),
  DELETE("DELETE", false, true),
  HEAD("HEAD", false, false);

  HttpMethod(String method, boolean usesRequestBody, boolean usesResponseBody) {
    this.usesResponseBody = usesResponseBody;
    this.usesRequestBody = usesRequestBody;
  }

  // Represents whether we presently use a request / response body with this type or not,
  // not necessarily if a body is allowed in the request or response for this HTTP verb.
  //
  // These are used to build valid test cases with `mock-server`.
  private final boolean usesRequestBody;
  private final boolean usesResponseBody;

  public boolean usesResponseBody() {
    return usesResponseBody;
  }

  public boolean usesRequestBody() {
    return usesRequestBody;
  }
}
