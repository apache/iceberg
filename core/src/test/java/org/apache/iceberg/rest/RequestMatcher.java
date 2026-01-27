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

import static org.mockito.ArgumentMatchers.argThat;

import java.util.Map;
import java.util.Objects;

class RequestMatcher {
  private RequestMatcher() {}

  public static HTTPRequest matches(HTTPRequest.HTTPMethod method) {
    return argThat(req -> req.method() == method);
  }

  static HTTPRequest matches(HTTPRequest.HTTPMethod method, String path) {
    return argThat(req -> req.method() == method && req.path().equals(path));
  }

  public static HTTPRequest matches(
      HTTPRequest.HTTPMethod method, String path, Map<String, String> headers) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers)));
  }

  public static HTTPRequest matches(
      HTTPRequest.HTTPMethod method,
      String path,
      Map<String, String> headers,
      Map<String, String> parameters) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers))
                && req.queryParameters().equals(parameters));
  }

  public static HTTPRequest matches(
      HTTPRequest.HTTPMethod method,
      String path,
      Map<String, String> headers,
      Map<String, String> parameters,
      Object body) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().equals(HTTPHeaders.of(headers))
                && req.queryParameters().equals(parameters)
                && Objects.equals(req.body(), body));
  }

  public static HTTPRequest containsHeaders(
      HTTPRequest.HTTPMethod method, String path, Map<String, String> headers) {
    return argThat(
        req ->
            req.method() == method
                && req.path().equals(path)
                && req.headers().entries().containsAll(HTTPHeaders.of(headers).entries()));
  }
}
