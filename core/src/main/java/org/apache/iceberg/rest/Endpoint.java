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

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

/**
 * Holds an endpoint definition that consists of the HTTP method (GET, POST, DELETE, ...) and the
 * resource path as defined in the Iceberg OpenAPI REST specification without parameter
 * substitution, such as <b>/v1/{prefix}/namespaces/{namespace}</b>.
 */
public class Endpoint {
  private static final Splitter ENDPOINT_SPLITTER = Splitter.on(" ");
  private static final Joiner ENDPOINT_JOINER = Joiner.on(" ");
  private final String httpMethod;
  private final String path;

  private Endpoint(String httpMethod, String path) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(httpMethod), "Invalid HTTP method: null or empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "Invalid path: null or empty");
    this.httpMethod = httpMethod.toUpperCase(Locale.ROOT);
    this.path = path;
  }

  public String httpMethod() {
    return httpMethod;
  }

  public String path() {
    return path;
  }

  public static Endpoint create(String httpMethod, String path) {
    return new Endpoint(httpMethod, path);
  }

  @Override
  public String toString() {
    return ENDPOINT_JOINER.join(httpMethod(), path());
  }

  public static Endpoint fromString(String endpoint) {
    List<String> elements = ENDPOINT_SPLITTER.splitToList(endpoint);
    Preconditions.checkArgument(
        elements.size() == 2,
        "Invalid endpoint (must consist of two elements separated by a single space): %s",
        endpoint);
    return create(elements.get(0), elements.get(1));
  }

  /**
   * Checks if the set of endpoints support the given {@link Endpoint}.
   *
   * @param supportedEndpoints The set of supported endpoints to check
   * @param endpoint The endpoint to check against the set of supported endpoints
   * @throws UnsupportedOperationException if the given {@link Endpoint} is not included in the set
   *     of endpoints.
   */
  public static void check(Set<Endpoint> supportedEndpoints, Endpoint endpoint) {
    if (!supportedEndpoints.contains(endpoint)) {
      throw new UnsupportedOperationException(
          String.format(
              "Server does not support endpoint: %s %s", endpoint.httpMethod(), endpoint.path()));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Endpoint)) {
      return false;
    }

    Endpoint endpoint = (Endpoint) o;
    return Objects.equals(httpMethod, endpoint.httpMethod) && Objects.equals(path, endpoint.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(httpMethod, path);
  }
}
